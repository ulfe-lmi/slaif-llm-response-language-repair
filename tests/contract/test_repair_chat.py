"""Contract tests for the plain-terminal repair chat example.

Every test drives the real ``examples/repair_chat.py`` as a subprocess
against an owned loopback HTTP server (``127.0.0.1`` port 0) with purely
synthetic Codex homes, TOML configs, catalogs and credentials: no real
profile, no real key, no network beyond the loopback interfaces.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
CLIENT = REPO_ROOT / "examples" / "repair_chat.py"
KEY_ENV = "SYN_042_KEY"
KEY = "synthetic-042-key"

CATALOG_2 = {
    "models": [
        {
            "slug": "alpha",
            "display_name": "First",
            "default_reasoning_level": "medium",
            "supported_reasoning_levels": [{"effort": "low"}, {"effort": "medium"}],
        },
        {
            "slug": "beta",
            "display_name": "Second",
            "default_reasoning_level": "low",
            "supported_reasoning_levels": [
                {"effort": "low"},
                {"effort": "medium"},
                {"effort": "high"},
            ],
        },
    ]
}


class Request:
    """One request observed by the owned loopback server."""

    def __init__(self, method: str, path: str, headers: dict[str, str], body: bytes) -> None:
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body

    def header(self, name: str) -> str | None:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None

    @property
    def json_body(self) -> Any:
        # The outgoing request body is untrusted-by-design JSON; tests
        # index it directly.
        return json.loads(self.body.decode("utf-8"))


class Server:
    """Owned ThreadingHTTPServer with scripted responses per POST index.

    Response specs: ``status``, ``headers`` and ``chunks`` (bytes, or a
    ``("wait", event)`` tuple that holds the stream until the event is
    set).  HTTP/1.0 close-delimited bodies keep the client's urllib reads
    simple.  Handler threads are non-daemon; unexpected handler errors
    (other than client disconnects) are collected and asserted on in
    :meth:`stop`, so a fixture bug can never masquerade as a pass.
    """

    def __init__(self, responses: list[dict[str, Any]] | None = None) -> None:
        self.requests: list[Request] = []
        self.responses = list(responses or [])
        self.handler_errors: list[str] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            timeout = 30  # finite deadline for accepted sockets

            def log_message(self, *args: object) -> None:
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                outer.requests.append(
                    Request("POST", self.path, dict(self.headers.items()), body)
                )
                spec: dict[str, Any] = outer.responses.pop(0) if outer.responses else {}
                self.send_response(int(spec.get("status", 200)))
                for name, value in spec.get("headers", {}).items():
                    self.send_header(name, value)
                self.end_headers()
                for chunk in spec.get("chunks", []):
                    if isinstance(chunk, tuple):
                        chunk[1].wait(30)
                    else:
                        self.wfile.write(chunk)
                        self.wfile.flush()

            def do_GET(self) -> None:
                outer.requests.append(
                    Request("GET", self.path, dict(self.headers.items()), b"")
                )
                self.send_response(404)
                self.end_headers()

        def _collect_error(request: object, client_address: object) -> None:
            # Client disconnects (timeout/SIGINT aborts) are expected;
            # anything else is collected and asserted on in stop().
            exc = sys.exc_info()[1]
            if exc is None or isinstance(exc, (BrokenPipeError, ConnectionResetError)):
                return
            outer.handler_errors.append(f"{type(exc).__name__}: {exc}")

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = False
        self.httpd.handle_error = _collect_error  # type: ignore[method-assign]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=False)
        self._released = False

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1"

    def start(self) -> None:
        self.thread.start()

    def release(self) -> None:
        """Idempotent resource release: stop serving, close the socket and
        join all owned threads.  Fixture holds must already be released by
        the caller (finally blocks); this never masks handler errors."""
        if self._released:
            return
        self._released = True
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(10)
        if self.thread.is_alive():
            raise RuntimeError("owned server thread did not terminate")

    def stop(self) -> None:
        # Release first so cleanup completes even when the error
        # assertions below fail.
        self.release()
        assert self.handler_errors == [], self.handler_errors


@contextlib.contextmanager
def owned_server(responses: list[dict[str, Any]] | None = None) -> Iterator[Server]:
    server = Server(responses)
    server.start()
    try:
        yield server
    finally:
        server.stop()


def write_home(base: Path, config: str | None, models: str | None = None) -> Path:
    """Create a synthetic codex home under ``base``; returns the home dir."""
    home = base / "codex-home"
    home.mkdir(parents=True, exist_ok=True)
    if config is not None:
        (home / "config.toml").write_text(config, encoding="utf-8")
    if models is not None:
        (home / "models.json").write_text(models, encoding="utf-8")
    return home


def config_for(
    server: Server,
    *,
    model: str | None = "alpha",
    effort: str | None = "low",
    catalog: str | None = None,
    wire_api: str = "responses",
    bearer: bool = False,
    headers: dict[str, str] | None = None,
    extra: str = "",
) -> str:
    lines = [f'model = "{model}"', 'model_provider = "prov"']
    if effort is not None:
        lines.append(f'model_reasoning_effort = "{effort}"')
    if catalog is not None:
        lines.append(f'model_catalog_json = "{catalog}"')
    lines += [
        "",
        "[model_providers.prov]",
        'name = "Synthetic"',
        f'base_url = "{server.base_url()}"',
        f'wire_api = "{wire_api}"',
        "requires_openai_auth = false",
    ]
    if bearer:
        lines.append(f'experimental_bearer_token = "{KEY}"')
    else:
        lines.append(f'env_key = "{KEY_ENV}"')
    if headers:
        lines.append("[model_providers.prov.http_headers]")
        for name, value in headers.items():
            lines.append(f"{name} = {json.dumps(value)}")
    if extra:
        lines.append(extra)
    return "\n".join(lines) + "\n"


def env_for(home: Path, **extra: str) -> dict[str, str]:
    env = {
        "HOME": str(home.parent),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        KEY_ENV: KEY,
    }
    env.update(extra)
    return env


def run_client(
    home: Path,
    stdin: str,
    args: tuple[str, ...] = (),
    *,
    timeout: float = 30,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-B", str(CLIENT), "--codex-home", str(home), *args],
        input=stdin,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(REPO_ROOT),
        env=env or env_for(home),
    )


def sse(obj: object) -> bytes:
    return ("data: " + json.dumps(obj, ensure_ascii=False) + "\r\n\r\n").encode("utf-8")


def response_obj(
    status: str = "completed",
    output: list[dict[str, object]] | None = None,
    extension: dict[str, object] | None = None,
    error: object = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "id": "resp_1",
        "object": "response",
        "status": status,
        "output": output if output is not None else [],
    }
    if extension is not None:
        value["llm_language_repair"] = extension
    if error is not None:
        value["error"] = error
    return value


def message_output(text: str) -> dict[str, object]:
    return {
        "type": "message",
        "id": "m1",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def reasoning_output(
    item_id: str,
    content: list[dict[str, object]] | None = None,
    summary: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {"type": "reasoning", "id": item_id, "status": "completed"}
    if content is not None:
        value["content"] = content
    if summary is not None:
        value["summary"] = summary
    return value


def json_spec(value: object, status: int = 200) -> dict[str, object]:
    return {
        "status": status,
        "headers": {"Content-Type": "application/json"},
        "chunks": [json.dumps(value, ensure_ascii=False).encode("utf-8")],
    }


def sse_spec(chunks: Sequence[object]) -> dict[str, object]:
    return {
        "status": 200,
        "headers": {"Content-Type": "text/event-stream"},
        "chunks": chunks,
    }


def stdout_lines(proc: subprocess.CompletedProcess[str]) -> list[str]:
    return proc.stdout.splitlines()


# ---------------------------------------------------------------------------
# Startup, configuration, profiles, catalog, auth, URL
# ---------------------------------------------------------------------------


def test_help_exits_zero_without_network(tmp_path: Path) -> None:
    proc = run_client(tmp_path / "no-such-home", "", args=("--help",))
    assert proc.returncode == 0
    assert "usage" in proc.stdout
    assert "/model" in proc.stdout and "/thinking" in proc.stdout


def test_missing_config_fails_clearly(tmp_path: Path) -> None:
    home = write_home(tmp_path, None)
    with owned_server() as server:
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "no configuration found" in proc.stderr
    assert server.requests == []


def test_missing_profile_refuses_silent_fallback(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n", args=("--profile", "ghost"))
    assert proc.returncode == 1
    assert "profile 'ghost' not found" in proc.stderr
    assert "refusing" in proc.stderr
    assert server.requests == []


def test_profile_table_inherits_root_settings(tmp_path: Path) -> None:
    with owned_server([json_spec(response_obj(output=[message_output("Odgovor.")]))]) as server:
        config = config_for(server, model="alpha") + '\n[profiles.pick]\nmodel = "beta"\n'
        home = write_home(tmp_path, config)
        proc = run_client(home, "Hej.\n", args=("--profile", "pick"))
    assert proc.returncode == 0
    assert len(server.requests) == 1
    body = server.requests[0].json_body
    assert body["model"] == "beta"
    assert body["reasoning"] == {"effort": "low"}  # inherited from root config


def test_standalone_profile_is_complete_selected_file(tmp_path: Path) -> None:
    with owned_server([json_spec(response_obj(output=[message_output("Odgovor.")]))]) as other:
        config_other = config_for(other, model="gamma", effort="high")
        with owned_server() as root_server:
            home = write_home(tmp_path, config_for(root_server, model="alpha"))
            (home / "beta.config.toml").write_text(config_other, encoding="utf-8")
            proc = run_client(home, "Hej.\n", args=("--profile", "beta"))
    assert proc.returncode == 0
    assert len(other.requests) == 1
    assert other.requests[0].json_body["model"] == "gamma"
    assert other.requests[0].json_body["reasoning"] == {"effort": "high"}
    assert root_server.requests == []


def test_cli_overrides_profile_and_root(tmp_path: Path) -> None:
    with owned_server(
        [
            json_spec(response_obj(output=[message_output("A")])),
            json_spec(response_obj(output=[message_output("B")])),
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        proc = run_client(
            home, "Hej.\n", args=("--model", "beta", "--thinking", "high")
        )
    assert proc.returncode == 0
    body = server.requests[0].json_body
    assert body["model"] == "beta"
    assert body["reasoning"] == {"effort": "high"}


def test_models_flag_overrides_broken_catalog_path(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit-catalog.json"
    explicit.write_text(json.dumps({"models": [{"slug": "gamma"}]}), encoding="utf-8")
    with owned_server([json_spec(response_obj(output=[message_output("Odgovor.")]))]) as server:
        home = write_home(
            tmp_path, config_for(server, model="gamma", catalog="missing.json")
        )
        proc = run_client(home, "Hej.\n", args=("--models", str(explicit)))
    assert proc.returncode == 0
    assert server.requests[0].json_body["model"] == "gamma"


def test_relative_catalog_resolved_against_config_dir(tmp_path: Path) -> None:
    with owned_server([json_spec(response_obj(output=[message_output("Odgovor.")]))]) as server:
        home = write_home(
            tmp_path,
            config_for(server, model="alpha", catalog="models.json"),
            models=json.dumps(CATALOG_2),
        )
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert server.requests[0].json_body["model"] == "alpha"


def test_tilde_catalog_expanded_from_home(tmp_path: Path) -> None:
    (tmp_path / "models.json").write_text(json.dumps(CATALOG_2), encoding="utf-8")
    with owned_server([json_spec(response_obj(output=[message_output("Odgovor.")]))]) as server:
        home = write_home(
            tmp_path, config_for(server, model="alpha", catalog="~/models.json")
        )
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert server.requests[0].json_body["model"] == "alpha"


def test_catalog_not_found_fails(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server, catalog="missing.json"))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "model catalog not found" in proc.stderr
    assert server.requests == []


def test_non_responses_wire_api_rejected(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server, wire_api="chat"))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "only the Responses wire API" in proc.stderr
    assert server.requests == []


def test_env_key_unset_fails(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server))
        env = env_for(home)
        del env[KEY_ENV]
        proc = run_client(home, "Hej.\n", env=env)
    assert proc.returncode == 1
    assert f"'{KEY_ENV}' is not set" in proc.stderr
    assert server.requests == []


def test_oauth_only_provider_gets_fixed_explanation(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server).replace(f'env_key = "{KEY_ENV}"\n', ""))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "auth.json" in proc.stderr and "OAuth" in proc.stderr
    assert "Traceback" not in proc.stderr
    assert server.requests == []


def test_invalid_env_key_credential_rejected_without_echo(tmp_path: Path) -> None:
    sentinel = "bad-env-credential"
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server))
        env = env_for(home, **{KEY_ENV: f"line1\n{sentinel}\u001b[31m"})
        proc = run_client(home, "Hej.\n", env=env)
    assert proc.returncode == 1
    assert "not a valid single-line printable ASCII header value" in proc.stderr
    assert sentinel not in proc.stdout + proc.stderr
    assert "Traceback" not in proc.stderr
    assert server.requests == []


def test_invalid_direct_bearer_credential_rejected_without_echo(tmp_path: Path) -> None:
    sentinel = "bad-bearer-credential"
    # TOML-escaped control characters decode to a real CR/LF + BEL value.
    escaped = f"{sentinel}\\r\\nEvil: 1\\u0007"
    with owned_server() as server:
        config = config_for(server, bearer=True).replace(f'"{KEY}"', f'"{escaped}"')
        home = write_home(tmp_path, config)
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "not a valid single-line printable ASCII header value" in proc.stderr
    assert sentinel not in proc.stdout + proc.stderr
    assert "Traceback" not in proc.stderr
    assert server.requests == []


def test_url_scheme_userinfo_and_trailing_responses_rejected(tmp_path: Path) -> None:
    for base_url, needle in (
        ("ftp://127.0.0.1:1/v1", "unsupported URL scheme"),
        ("http://user:pass@127.0.0.1:1/v1", "userinfo"),
        ("http://127.0.0.1:1/v1/responses", "must not already end"),
    ):
        with owned_server() as server:
            broken = config_for(server).replace(
                f'base_url = "{server.base_url()}"', f'base_url = "{base_url}"'
            )
            home = write_home(tmp_path / needle, broken)
            proc = run_client(home, "Hej.\n")
        assert proc.returncode == 1, base_url
        assert needle in proc.stderr
        assert server.requests == []


def test_codex_home_environment_variable_is_ignored(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Pravi.")]))]
    ) as real, owned_server() as decoy:
        decoy_home = write_home(tmp_path / "decoy", config_for(decoy, model="decoy-model"))
        real_home = write_home(tmp_path, config_for(real))
        proc = run_client(
            real_home,
            "Hej.\n",
            env=env_for(real_home, CODEX_HOME=str(decoy_home)),
        )
    assert proc.returncode == 0
    assert len(real.requests) == 1
    assert decoy.requests == []


# ---------------------------------------------------------------------------
# Request shape, header invariants, history
# ---------------------------------------------------------------------------


def test_request_shape_and_client_owned_headers(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Pozdrav.\n")
    assert proc.returncode == 0
    req = server.requests[0]
    assert req.method == "POST" and req.path == "/v1/responses"
    body = req.json_body
    assert body["model"] == "alpha"
    assert body["store"] is False and body["stream"] is True
    assert body["reasoning"] == {"effort": "low"}
    assert body["input"] == [
        {"role": "user", "content": [{"type": "input_text", "text": "Pozdrav."}]}
    ]
    for forbidden in ("tools", "previous_response_id", "conversation", "metadata"):
        assert forbidden not in body
    assert req.header("X-LLM-Language-Repair-Details") == "1"
    assert req.header("Authorization") == f"Bearer {KEY}"
    assert (req.header("Content-Type") or "").startswith("application/json")


def test_history_is_delivered_only_across_turns(tmp_path: Path) -> None:
    first = response_obj(
        output=[message_output("Prvi odgovor.")],
        extension={
            "version": 1,
            "status": "complete",
            "output_index": 0,
            "content_index": 0,
            "original_text": "IZVIRNIK-tekst",
        },
    )
    with owned_server(
        [
            json_spec(first),
            json_spec(response_obj(output=[message_output("Drugi odgovor.")])),
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Prvi.\nDrugi.\n")
    assert proc.returncode == 0
    second_input = server.requests[1].json_body["input"]
    assert second_input == [
        {"role": "user", "content": [{"type": "input_text", "text": "Prvi."}]},
        {
            "role": "assistant",
            "content": [{"type": "input_text", "text": "Prvi odgovor."}]},
        {"role": "user", "content": [{"type": "input_text", "text": "Drugi."}]},
    ]
    second_raw = json.dumps(second_input, ensure_ascii=False)
    assert "IZVIRNIK-tekst" not in second_raw
    assert "llm_language_repair" not in second_raw


def test_noncompleted_terminal_not_committed(tmp_path: Path) -> None:
    held = sse_spec(
        [sse({"type": "response.completed", "response": response_obj(status="in_progress")})]
    )
    with owned_server(
        [held, json_spec(response_obj(output=[message_output("Veljaven.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Neveljaven.\nVeljaven.\n")
    assert proc.returncode == 0
    assert len(server.requests) == 2
    assert len(server.requests[1].json_body["input"]) == 1
    assert proc.stdout.count("Delivered response") == 1
    assert "response failed on the endpoint" in proc.stdout


def test_json_terminal_statuses_rejected(tmp_path: Path) -> None:
    cases = (
        (
            response_obj(status="queued", output=[message_output("X")]),
            "response failed on the endpoint",
        ),
        (
            response_obj(status="incomplete", output=[message_output("X")]),
            "response incomplete on the endpoint",
        ),
        (
            response_obj(status="completed", output=[message_output("X")], error={"code": "boom"}),
            "response failed on the endpoint",
        ),
    )
    for index, (value, expected) in enumerate(cases):
        with owned_server([json_spec(value)]) as server:
            home = write_home(tmp_path / str(index), config_for(server))
            proc = run_client(home, "Hej.\n")
        assert expected in proc.stdout, (index, proc.stdout)
        assert "Delivered response" not in proc.stdout
        assert server.requests[0] is not None


def test_completed_without_answer_text_not_committed(tmp_path: Path) -> None:
    value = response_obj(
        output=[
            reasoning_output("r1", content=[{"type": "reasoning_text", "text": "misel"}])
        ]
    )
    with owned_server([json_spec(value)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "No assistant answer text was returned" in proc.stdout
    assert "Delivered response" not in proc.stdout


def test_json_non_object_and_invalid_body_rejected(tmp_path: Path) -> None:
    for index, (chunks, expected) in enumerate(
        [
        ([json.dumps([1, 2]).encode()], "non-object JSON"),
        ([b"not json at all"], "non-JSON where JSON was expected"),
        ]
    ):
        with owned_server(
            [{"status": 200, "headers": {"Content-Type": "application/json"}, "chunks": chunks}]
        ) as server:
            home = write_home(tmp_path / str(index), config_for(server))
            proc = run_client(home, "Hej.\n")
        assert expected in proc.stdout, (index, proc.stdout)
        assert "Delivered response" not in proc.stdout
        assert proc.returncode == 0


# ---------------------------------------------------------------------------
# SSE integrity and untrusted-input safety
# ---------------------------------------------------------------------------


def test_named_sse_fields_accepted_before_data(tmp_path: Path) -> None:
    chunks = [
        b"event: response.completed\r\nid: synthetic-1\r\nretry: 1000\r\n",
        sse({"type": "response.completed", "response": response_obj(
            output=[message_output("Odgovor.")])}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "Delivered response" in proc.stdout
    assert "Odgovor." in proc.stdout
    assert "Original response was not provided." in proc.stdout


def test_malformed_sse_data_is_honest_failure(tmp_path: Path) -> None:
    chunks = [
        b"data: {broken}\r\n\r\n",
        sse({"type": "response.completed", "response": response_obj(
            output=[message_output("Odgovor.")])}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\nDrugic.\n")
    assert "malformed stream data" in proc.stdout
    assert "Delivered response" not in proc.stdout
    assert "Traceback" not in proc.stderr
    assert proc.returncode == 0
    assert len(server.requests) == 2
    assert len(server.requests[1].json_body["input"]) == 1


def test_non_object_sse_frame_is_malformed(tmp_path: Path) -> None:
    chunks = [
        b"data: [1, 2]\r\n\r\n",
        sse({"type": "response.completed", "response": response_obj(
            output=[message_output("Odgovor.")])}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert "malformed stream event" in proc.stdout
    assert "Delivered response" not in proc.stdout


def test_line_without_field_is_malformed(tmp_path: Path) -> None:
    with owned_server([sse_spec([b"garbageline\r\n\r\n"])]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert "malformed stream data" in proc.stdout
    assert "Delivered response" not in proc.stdout
    assert "Traceback" not in proc.stderr


def test_unknown_wellformed_event_types_ignored(tmp_path: Path) -> None:
    chunks = [
        sse({"type": "response.created", "response": {"id": "r"}}),
        sse({"type": "response.synthetic_marker", "value": 1}),
        sse({"type": "response.completed", "response": response_obj(
            output=[message_output("Odgovor.")])}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "Delivered response" in proc.stdout
    assert "Odgovor." in proc.stdout


def test_premature_eof_is_not_success(tmp_path: Path) -> None:
    chunks = [
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": "del"}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert "stream ended before completion" in proc.stdout
    assert "Delivered response" not in proc.stdout
    assert proc.returncode == 0


def test_response_failed_and_incomplete_events(tmp_path: Path) -> None:
    for index, (event_type, expected) in enumerate(
        [("response.failed", "response failed on the endpoint"),
         ("response.incomplete", "response incomplete on the endpoint")]
    ):
        with owned_server(
            [sse_spec([sse({"type": event_type, "response": {"id": "r"}})])]
        ) as server:
            home = write_home(tmp_path / str(index), config_for(server))
            proc = run_client(home, "Hej.\n")
        assert expected in proc.stdout
        assert "Delivered response" not in proc.stdout


def test_error_event_payload_never_echoed(tmp_path: Path) -> None:
    secret_event = sse({"type": "error", "error": {"code": KEY, "message": KEY}})
    with owned_server([sse_spec([secret_event])]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert "the endpoint reported an error" in proc.stdout
    assert KEY not in proc.stdout + proc.stderr
    assert "Delivered response" not in proc.stdout


def test_http_error_and_bearer_never_printed(tmp_path: Path) -> None:
    with owned_server(
        [
            {"status": 500, "headers": {"Content-Type": "text/plain"},
             "chunks": [f"secret {KEY}".encode()]},
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server, bearer=True))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "HTTP 500 from the endpoint" in proc.stdout
    assert KEY not in proc.stdout + proc.stderr


def test_redirect_is_refused_without_cross_origin_request(tmp_path: Path) -> None:
    with owned_server() as sink:
        redirect = {"status": 302, "headers": {"Location": f"http://127.0.0.1:{sink.port}/sink"},
                    "chunks": []}
        with owned_server([redirect]) as server:
            home = write_home(tmp_path, config_for(server))
            proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "HTTP 302 from the endpoint" in proc.stdout
    assert "Delivered response" not in proc.stdout
    assert sink.requests == []


def test_timeout_mid_stream_returns_to_prompt(tmp_path: Path) -> None:
    hold = threading.Event()
    chunks = [
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": "del"}),
        ("wait", hold),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        try:
            proc = run_client(home, "Hej.\n/quit\n", args=("--timeout", "1"))
        finally:
            hold.set()
    assert proc.returncode == 0
    assert "request timed out after 1 seconds" in proc.stdout
    assert "Delivered response" not in proc.stdout


# ---------------------------------------------------------------------------
# Reasoning display: contiguous streaming, dedup, JSON fallback
# ---------------------------------------------------------------------------


def test_streamed_thinking_contiguous_and_not_repeated(tmp_path: Path) -> None:
    thinking = "Preverjam slovenski stavek."
    final = response_obj(
        output=[
            reasoning_output(
                "rs_1",
                content=[{"type": "reasoning_text", "text": thinking}],
                summary=[],
            ),
            message_output("Odgovor."),
        ]
    )
    chunks = [
        b": heartbeat\r\n\r\n",
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": thinking[:10]}),
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": thinking[10:]}),
        sse({"type": "response.completed", "response": final}),
        b"data: [DONE]\r\n\r\n",
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert thinking in proc.stdout
    assert proc.stdout.count(thinking) == 1
    assert proc.stdout.count("Thinking") == 1
    assert "Delivered response" in proc.stdout


def test_done_only_reasoning_printed_once(tmp_path: Path) -> None:
    text = "Celotna razmislitev."
    final = response_obj(
        output=[
            reasoning_output("rs_2", content=[{"type": "reasoning_text", "text": text}]),
            message_output("Odgovor."),
        ]
    )
    chunks = [
        sse({"type": "response.reasoning_text.done", "item_id": "rs_2",
             "output_index": 0, "content_index": 0, "text": text}),
        sse({"type": "response.completed", "response": final}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert proc.stdout.count(text) == 1


def test_terminal_item_only_reasoning_not_dropped(tmp_path: Path) -> None:
    text = "Samostojna razmislitev."
    final = response_obj(
        output=[
            reasoning_output("r9", content=[{"type": "reasoning_text", "text": text}]),
            message_output("Odgovor."),
        ]
    )
    with owned_server(
        [sse_spec([sse({"type": "response.completed", "response": final})])]
    ) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert proc.stdout.count(text) == 1
    assert "Thinking" in proc.stdout


def test_output_item_done_event_only_reasoning(tmp_path: Path) -> None:
    raw_text = "Vidni izvirni zapis."
    summary_text = "Vidni povzetek iz item.done."
    done_item = reasoning_output(
        "r1",
        content=[{"type": "reasoning_text", "text": raw_text}],
        summary=[{"type": "summary_text", "text": summary_text}],
    )
    # The terminal snapshot repeats the same reasoning item without the
    # text; a message item done event must never be printed.
    final = response_obj(
        output=[
            reasoning_output("r1", content=[], summary=[]),
            message_output("Odgovor."),
        ]
    )
    chunks = [
        sse({"type": "response.output_item.done", "output_index": 0, "item": done_item}),
        sse({"type": "response.output_item.done", "output_index": 1,
             "item": message_output("Ne sme izpisati.")}),
        sse({"type": "response.completed", "response": final}),
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert proc.stdout.count(raw_text) == 1
    assert proc.stdout.count(summary_text) == 1
    assert "Ne sme izpisati." not in proc.stdout
    assert "Delivered response" in proc.stdout


def test_json_summary_only_is_displayed_as_summary(tmp_path: Path) -> None:
    summary = "Samo povzetek misli."
    value = response_obj(
        output=[
            reasoning_output("r1", summary=[{"type": "summary_text", "text": summary}]),
            message_output("Odgovor."),
        ]
    )
    with owned_server([json_spec(value)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert summary in proc.stdout
    lines = stdout_lines(proc)
    # The heading may share the "you> " prompt line; a raw "Thinking"
    # heading would only ever appear as its own line.
    assert "Thinking summary" in proc.stdout
    assert "Thinking" not in lines
    assert any(line.endswith("Thinking summary") for line in lines)


def test_extension_unavailable_with_list_reason_does_not_crash(tmp_path: Path) -> None:
    value = response_obj(
        output=[message_output("Odgovor.")],
        extension={"version": 1, "status": "unavailable", "reason": []},
    )
    with owned_server([json_spec(value)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "Odgovor." in proc.stdout
    assert "Original response was not provided." in proc.stdout
    assert "(reason:" not in proc.stdout
    assert "Traceback" not in proc.stderr


def test_extension_unavailable_fixed_reason_shown(tmp_path: Path) -> None:
    value = response_obj(
        output=[message_output("Odgovor.")],
        extension={"version": 1, "status": "unavailable",
                   "reason": "EDIT_MAPPING_UNAVAILABLE"},
    )
    with owned_server([json_spec(value)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert "Original response was not provided." in proc.stdout
    assert "(reason: EDIT_MAPPING_UNAVAILABLE)" in proc.stdout


def test_extension_bool_version_and_wrong_indexes_rejected(tmp_path: Path) -> None:
    cases = (
        {"version": True, "status": "complete", "output_index": 0,
         "content_index": 0, "original_text": "Ne sme izbrati."},
        {"version": 2, "status": "complete", "output_index": 0,
         "content_index": 0, "original_text": "Ne sme izbrati."},
        {"version": 1, "status": "complete", "output_index": 5,
         "content_index": 0, "original_text": "Ne sme izbrati."},
        {"version": 1, "status": "complete", "output_index": 0,
         "content_index": 0, "original_text": 123},
    )
    for index, extension in enumerate(cases):
        value = response_obj(output=[message_output("Odgovor.")], extension=extension)
        with owned_server([json_spec(value)]) as server:
            home = write_home(tmp_path / str(index), config_for(server))
            proc = run_client(home, "Hej.\n")
        assert proc.returncode == 0, (index, proc.stderr)
        assert "Odgovor." in proc.stdout, index
        assert "Original response was not provided." in proc.stdout, index
        assert "Ne sme izbrati." not in proc.stdout, index


def test_extension_edits_summary_and_unicode_original(tmp_path: Path) -> None:
    original = "Lep sončn dan 😀."
    delivered = "Lep sončen dan 😀."
    value = response_obj(
        output=[message_output(delivered)],
        extension={
            "version": 1,
            "status": "complete",
            "output_index": 0,
            "content_index": 0,
            "original_text": original,
            "edits": [
                {"original_start": 4, "original_end": 9,
                 "repaired_start": 4, "repaired_end": 10, "replacement": "sončen"}
            ],
        },
    )
    with owned_server([json_spec(value)]) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert f"Original response\n{original}" in proc.stdout
    assert f"Delivered response\n{delivered}" in proc.stdout
    assert "Applied edits: 1 edit(s), code-point ranges: 4-9 -> 4-10" in proc.stdout


# ---------------------------------------------------------------------------
# Configured-header invariants and menu/session behaviour
# ---------------------------------------------------------------------------


def test_client_owned_headers_cannot_be_overridden(tmp_path: Path) -> None:
    for index, (name, value) in enumerate(
        [
            ("x-llm-language-repair-details", "0"),
            ("Authorization", "Bearer other"),
            ("content-type", "text/plain"),
        ]
    ):
        with owned_server() as server:
            home = write_home(tmp_path / str(index), config_for(server, headers={name: value}))
            proc = run_client(home, "Hej.\n")
        assert proc.returncode == 1, (name, proc.stderr)
        assert "header" in (proc.stderr + proc.stdout).lower()
        assert "Traceback" not in proc.stderr
        assert server.requests == []


def test_crlf_header_value_rejected(tmp_path: Path) -> None:
    with owned_server() as server:
        config = config_for(server, headers={"evil": "x\r\nEvil: 1"})
        home = write_home(tmp_path, config)
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 1
    assert "CR or LF" in proc.stderr
    assert "Traceback" not in proc.stderr
    assert server.requests == []


def test_valid_extra_header_is_sent(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server, headers={"x-synthetic": "da"}))
        proc = run_client(home, "Hej.\n")
    assert proc.returncode == 0
    assert server.requests[0].header("x-synthetic") == "da"
    assert server.requests[0].header("X-LLM-Language-Repair-Details") == "1"


def test_unexpected_fixture_errors_are_collected_not_masked(tmp_path: Path) -> None:
    # A malformed fixture chunk makes the handler raise TypeError.  The
    # client must still fail honestly, and the collected error must remain
    # visible so a fixture bug can never masquerade as a passing test.
    server = Server(
        [
            {
                "status": 200,
                "headers": {"Content-Type": "text/event-stream"},
                "chunks": [None],
            }
        ]
    )
    server.start()
    try:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "Hej.\n")
        assert proc.returncode == 0
        assert "Delivered response" not in proc.stdout
    finally:
        server.release()
    assert server.handler_errors, "unexpected handler error was not collected"
    assert any("TypeError" in error for error in server.handler_errors)


def test_model_menu_selects_on_next_line(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        proc = run_client(home, "/model\n2\nHej.\n")
    assert proc.returncode == 0
    lines = stdout_lines(proc)
    assert any(line.strip().startswith("1. alpha") and "[current]" in line for line in lines)
    assert any(line.strip().startswith("2. beta") for line in lines)
    assert server.requests[0].json_body["model"] == "beta"


def test_model_menu_blank_and_zero_cancel(tmp_path: Path) -> None:
    with owned_server(
        [
            json_spec(response_obj(output=[message_output("A")])),
            json_spec(response_obj(output=[message_output("B")])),
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        blank = run_client(home, "/model\n\nHej.\n")
        zero = run_client(home, "/model\n0\n/quit\n")
    assert blank.returncode == 0
    assert "cancelled" in blank.stdout
    assert server.requests[0].json_body["model"] == "alpha"
    assert zero.returncode == 0
    assert "cancelled" in zero.stdout
    assert len(server.requests) == 1


def test_model_menu_invalid_selection_unchanged(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("A")])),
         json_spec(response_obj(output=[message_output("B")])),
         json_spec(response_obj(output=[message_output("C")])),
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        invalid = run_client(home, "/model\n99\nHej.\n")
        by_name = run_client(home, "/model\nbeta\nHej.\n")
        direct = run_client(home, "/model beta\nHej.\n")
    assert invalid.returncode == 0
    assert "Invalid selection '99'" in invalid.stdout
    assert server.requests[0].json_body["model"] == "alpha"
    assert by_name.returncode == 0
    assert server.requests[1].json_body["model"] == "beta"
    assert direct.returncode == 0
    assert server.requests[2].json_body["model"] == "beta"


def test_quit_and_eof_from_menu_exit_cleanly(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        quit_run = run_client(home, "/model\n/quit\n")
        eof_run = run_client(home, "/model\n")
    assert quit_run.returncode == 0
    assert eof_run.returncode == 0
    assert server.requests == []


def test_thinking_menu_selects_on_next_line(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        proc = run_client(home, "/thinking\n2\nHej.\n")
    assert proc.returncode == 0
    assert "Next request will use thinking effort: medium" in proc.stdout
    assert server.requests[0].json_body["reasoning"] == {"effort": "medium"}


def test_thinking_menu_invalid_and_direct_invalid_unchanged(tmp_path: Path) -> None:
    with owned_server(
        [
            json_spec(response_obj(output=[message_output("A")])),
            json_spec(response_obj(output=[message_output("B")])),
        ]
    ) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        menu_invalid = run_client(home, "/thinking\nbogus\nHej.\n")
        direct_invalid = run_client(home, "/thinking xhigh\nHej.\n")
    assert menu_invalid.returncode == 0
    assert "Invalid selection 'bogus'" in menu_invalid.stdout
    assert server.requests[0].json_body["reasoning"] == {"effort": "low"}
    assert direct_invalid.returncode == 0
    assert "Level 'xhigh' is not available for alpha" in direct_invalid.stdout
    assert "allowed: low, medium" in direct_invalid.stdout
    assert server.requests[1].json_body["reasoning"] == {"effort": "low"}


def test_model_switch_preserves_supported_effort(tmp_path: Path) -> None:
    catalog = {
        "models": [
            {"slug": "alpha", "default_reasoning_level": "medium",
             "supported_reasoning_levels": [{"effort": "low"}, {"effort": "medium"}]},
            {"slug": "beta", "default_reasoning_level": "low",
             "supported_reasoning_levels": [{"effort": "low"}, {"effort": "medium"}]},
        ]
    }
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server, effort="medium"),
                          models=json.dumps(catalog))
        proc = run_client(home, "/model\n2\nHej.\n")
    assert proc.returncode == 0
    assert "Effort reset" not in proc.stdout
    assert server.requests[0].json_body["model"] == "beta"
    assert server.requests[0].json_body["reasoning"] == {"effort": "medium"}


def test_model_switch_resets_unsupported_effort(tmp_path: Path) -> None:
    catalog = {
        "models": [
            {"slug": "alpha", "default_reasoning_level": "medium",
             "supported_reasoning_levels": [{"effort": "low"}, {"effort": "medium"}]},
            {"slug": "beta", "default_reasoning_level": "low",
             "supported_reasoning_levels": [{"effort": "low"}, {"effort": "high"}]},
        ]
    }
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server, effort="medium"),
                          models=json.dumps(catalog))
        proc = run_client(home, "/model\n2\nHej.\n")
    assert proc.returncode == 0
    assert "Effort reset to 'low'" in proc.stdout
    assert server.requests[0].json_body["reasoning"] == {"effort": "low"}


def test_slash_only_session_makes_zero_requests(tmp_path: Path) -> None:
    with owned_server() as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        proc = run_client(home, "/help\n/model\n2\n/thinking\n1\n/quit\n")
    assert proc.returncode == 0
    assert "usage" not in proc.stdout and "Commands (one per line)" in proc.stdout
    assert server.requests == []


def test_unknown_slash_shows_help_and_blank_lines_ignored(tmp_path: Path) -> None:
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        home = write_home(tmp_path, config_for(server))
        proc = run_client(home, "/what\n\n\nHej.\n")
    assert proc.returncode == 0
    assert "Commands (one per line)" in proc.stdout
    assert len(server.requests) == 1


# ---------------------------------------------------------------------------
# Interactive behaviour: held-final barrier, Ctrl+C, unsafe control chars
# ---------------------------------------------------------------------------


class Pump:
    """Interactive child with a background stdout reader."""

    def __init__(self, home: Path, args: tuple[str, ...] = ()) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-B", str(CLIENT), "--codex-home", str(home), *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(REPO_ROOT),
            env=env_for(home),
        )
        self.buf = bytearray()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self) -> None:
        assert self.proc.stdout is not None
        fd = self.proc.stdout.fileno()
        while True:
            # os.read returns as soon as data is available; buffered
            # read(n) on a pipe would block until n bytes or EOF.
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            self.buf.extend(chunk)

    def wait_for(self, needle: bytes, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if needle in self.buf:
                return True
            time.sleep(0.02)
        return False

    def write(self, data: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(data.encode("utf-8"))
        self.proc.stdin.flush()

    def finish(self, timeout: float = 10) -> tuple[int, bytes, bytes]:
        try:
            returncode = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            returncode = self.proc.wait()
        self.reader.join(5)
        stderr_stream = self.proc.stderr
        stderr = stderr_stream.read() if stderr_stream is not None else b""
        return returncode, bytes(self.buf), stderr


def test_held_final_barrier_thinking_flushed_then_original(tmp_path: Path) -> None:
    thinking = "Preverjam slovenski stavek."
    original = "Lep sončn dan 😀."
    delivered = "Lep sončen dan 😀."
    final = response_obj(
        output=[
            reasoning_output(
                "rs_1",
                content=[{"type": "reasoning_text", "text": thinking}],
                summary=[],
            ),
            message_output(delivered),
        ],
        extension={
            "version": 1,
            "status": "complete",
            "output_index": 1,
            "content_index": 0,
            "original_text": original,
            "edits": [
                {"original_start": 4, "original_end": 9,
                 "repaired_start": 4, "repaired_end": 10, "replacement": "sončen"}
            ],
        },
    )
    release = threading.Event()

    chunks: list[object] = [
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": thinking[:10]}),
        sse({"type": "response.reasoning_text.delta", "item_id": "rs_1",
             "output_index": 0, "content_index": 0, "delta": thinking[10:]}),
        ("wait", release),
        sse({"type": "response.completed", "response": final}),
        b"data: [DONE]\r\n\r\n",
    ]
    with owned_server([sse_spec(chunks)]) as server:
        home = write_home(tmp_path, config_for(server), models=json.dumps(CATALOG_2))
        pump = Pump(home)
        try:
            pump.write("/model\n2\n/thinking\n1\nPopravi opis vremena.\n")
            assert pump.wait_for(thinking.encode("utf-8"), 8), bytes(pump.buf)
            assert delivered.encode("utf-8") not in pump.buf
            release.set()
            pump.write("/quit\n")
            returncode, out, err = pump.finish()
        finally:
            release.set()
            if pump.proc.poll() is None:
                pump.proc.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    pump.proc.wait(3)
    assert returncode == 0, err
    assert out.count(thinking.encode("utf-8")) == 1
    assert delivered.encode("utf-8") in out
    assert original.encode("utf-8") in out
    assert b"\x1b" not in out
    assert len(server.requests) == 1
    body = server.requests[0].json_body
    assert body["model"] == "beta"
    assert body["reasoning"] == {"effort": "low"}


def test_sigint_during_request_returns_prompt_and_quits(tmp_path: Path) -> None:
    hold = threading.Event()
    with owned_server([sse_spec([("wait", hold)])]) as server:
        home = write_home(tmp_path, config_for(server))
        pump = Pump(home)
        try:
            pump.write("Hej.\n")
            assert pump.wait_for(b"you> ", 5)
            deadline = time.monotonic() + 5
            while not server.requests and time.monotonic() < deadline:
                time.sleep(0.02)
            assert server.requests, "client never reached the owned upstream"
            time.sleep(0.2)
            os.kill(pump.proc.pid, signal.SIGINT)
            assert pump.wait_for(b"Request interrupted; nothing was committed.", 5), \
                bytes(pump.buf)
            pump.write("/quit\n")
            returncode, out, err = pump.finish()
        finally:
            hold.set()
            if pump.proc.poll() is None:
                pump.proc.terminate()
                with contextlib.suppress(subprocess.TimeoutExpired):
                    pump.proc.wait(3)
    assert returncode == 0, err
    assert b"Delivered response" not in out
    assert len(server.requests) == 1


def test_control_chars_in_slug_rendered_visibly(tmp_path: Path) -> None:
    slug = "evil\u001b[2Jx"
    catalog = {"models": [{"slug": slug, "default_reasoning_level": "low"}]}
    with owned_server(
        [json_spec(response_obj(output=[message_output("Odgovor.")]))]
    ) as server:
        # TOML basic strings need the escape for the raw ESC control char.
        config = (
            'model = "evil\\u001b[2Jx"\n'
            'model_provider = "prov"\n'
            'model_reasoning_effort = "low"\n'
            '\n'
            '[model_providers.prov]\n'
            'name = "Synthetic"\n'
            f'base_url = "{server.base_url()}"\n'
            'wire_api = "responses"\n'
            'requires_openai_auth = false\n'
            f'env_key = "{KEY_ENV}"\n'
        )
        home = write_home(tmp_path, config, models=json.dumps(catalog, ensure_ascii=True))
        proc = run_client(home, "/model\n0\nHej.\n")
    assert proc.returncode == 0
    assert "evil\\u001b[2Jx" in proc.stdout
    assert "\u001b" not in proc.stdout
    assert server.requests[0].json_body["model"] == slug

