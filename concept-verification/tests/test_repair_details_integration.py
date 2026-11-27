"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE.

041-a secondary real-HTTP integration regressions (order 041-a,
requirement 6): the caller opt-in X-LLM-Language-Repair-Details: 1
and the exact root llm_language_repair final-response metadata.

Black-box real-HTTP regressions against the actual runnable chain
(strict handler mode first, transparent/SSE added as the file
matures): real TCP client -> real ConceptProxy (strict admission/
intake/contract/setup/capture/delivery guards, real main forward
with its narrow header policy) -> real RepairEngine (pinned Pandoc
3.7.0.2 via CONCEPT_PANDOC, real read-only tiny SQLite corpus, real
CPU detection/acceptance, real composed validation, real
apply_edits patcher) -> real ResponsesReviewer with its real owned
transport child -> fake main upstream and fake reviewer endpoint,
both synthetic loopback servers OUTSIDE those boundaries.  No live
model calls: synthetic text only, fake keys only.

EXPECTED_ACCEPTED_BASE_FAILURES: on the accepted base (no
extension) the OPTED-IN positive cases fail at the actual HTTP
response boundary - the root llm_language_repair object is absent
from an otherwise fully successful repaired completed-200 answer -
which is the ordered old-baseline evidence (root proof:
root-041-old-baseline.json/log in the 041 workorder directory),
distinct from fixture or import failures.  The off/absent cases
prove unchanged prior behavior: they PASS on the accepted base and
must keep passing after the implementation.
"""

from __future__ import annotations

import contextlib
import copy
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import composed_validation  # noqa: E402
import proxy  # noqa: E402
import repair_details  # noqa: E402
from qwen_client import ResponsesReviewer  # noqa: E402
from tests.test_reasoning_streaming import (  # noqa: E402
    _ANSWER,
    _CANARY,
    _EDITED_ANSWER,
    _assert_incomplete_client_view,
    _ClientCapture,
    _contract_body_stream,
    _drain_until_eof,
    _expected_edited,
    _one_shot,
    _prefix_cut,
    _QuietThreadingHTTPServer,
    _render_stream,
    _stream_payloads,
    _StreamUpstream,
)
from tests.test_responses_contract import _json_completed  # noqa: E402
from tests.test_review_batch import (  # noqa: E402
    _build_corpus,
    _contract_body,
    _real_spec,
)
from tests.test_scaled_review_integration import (  # noqa: E402
    _CLEAN,
    _CONFLICT_TEXT,
    _REPLACEMENT,
    _SHORT,
    _TARGET,
    _build_conflict_corpus,
    _build_multibyte_corpus,
    _DynamicReviewer,
    _expected_repaired,
    _Harness,
    _hold_until_close,
    _load_candidate_loader,
    _long_answer,
    _LoopbackServer,
    _main_complete_handler,
    _MainState,
    _read_http_request,
    _read_http_response,
    _send_http_response,
    _usage_observer,
    _write_toml,
)

# The validated-known usage fixture records for the fake reviewer
# endpoint (the same exact numeric records the scaled-review
# fixture uses; defined locally so importing no other module's
# TestCase class keeps this file's collection clean).
_U1 = {"input_tokens": 11, "output_tokens": 17, "total_tokens": 28}
_U2 = {"input_tokens": 13, "output_tokens": 19, "total_tokens": 32}

# The private opt-in control header (order 041-a, requirement 1):
# exactly one trimmed "1" means on; absent or one value "0" means
# off; anything else (or duplicates) is a fixed pre-main rejection.
_DETAILS_HEADER = "X-LLM-Language-Repair-Details"

# The fixed root field name selected by the owner (never
# slaif_repair).
_FIELD = "llm_language_repair"


def _post_details_request(
    client: socket.socket, body: bytes, values: list[str]
) -> None:
    """One raw client POST of the nonstreaming contract body with an
    explicit (possibly empty, possibly repeated) control-header
    list.  The raw line construction is the fixture's own: header
    casing and repetition are exactly what the test says."""
    head = (
        b"POST /v1/responses HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\n"
    )
    for value in values:
        head += (
            _DETAILS_HEADER.encode("ascii")
            + b": "
            + value.encode("ascii")
            + b"\r\n"
        )
    head += (
        b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
        b"\r\n"
    )
    client.sendall(head + body)


def _strict_exchange_details(
    harness: _Harness,
    values: list[str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """One real client POST through the owned strict proxy with the
    exact control-header list; returns the response triple."""
    client = socket.create_connection(
        ("127.0.0.1", harness.proxy_server.server_port), timeout=20
    )
    try:
        _post_details_request(
            client,
            body if body is not None else _contract_body(False),
            values if values is not None else [],
        )
        return _read_http_response(client)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Fixture ground truth: exact expected coordinates derived from the
# fixture DEFINITION (target word, replacement, occurrence spans) -
# never reconstructed from a textual diff of the delivered answer.
# ---------------------------------------------------------------------------


def _find_spans(original: str, word: str) -> list[tuple[int, int]]:
    """All exact nonoverlapping occurrences of ``word`` in original
    order, as zero-based inclusive-start/exclusive-end CODE POINT
    spans (the A-SPAN-01 unit)."""
    spans: list[tuple[int, int]] = []
    start = original.find(word)
    while start != -1:
        spans.append((start, start + len(word)))
        start = original.find(word, start + 1)
    return spans


def _expected_edit(
    original_start: int,
    original_end: int,
    replacement: str,
    preceding_deltas: list[int],
) -> dict[str, object]:
    """The exact expected edit object: original code-point span plus
    delivered coordinates derived arithmetically from the PRECEDING
    actual length changes only (order 041-a, requirement 2)."""
    shift = sum(preceding_deltas)
    return {
        "original_start": original_start,
        "original_end": original_end,
        "repaired_start": original_start + shift,
        "repaired_end": original_start + shift + len(replacement),
        "replacement": replacement,
    }


def _complete_details(
    original_text: str,
    output_index: int,
    content_index: int,
    edits: list[dict[str, object]],
) -> dict[str, object]:
    """The exact ordered complete object shape."""
    return {
        "version": 1,
        "status": "complete",
        "offset_unit": "unicode_codepoints",
        "output_index": output_index,
        "content_index": content_index,
        "original_text": original_text,
        "edits": edits,
    }


def _unavailable(reason: str) -> dict[str, object]:
    """The exact ordered unavailable object shape (fixed reasons
    only)."""
    return {"version": 1, "status": "unavailable", "reason": reason}


class _KeepReviewer(_DynamicReviewer):
    """Fake reviewer answering EVERY target with keep=true /
    replacement=null (structurally valid per the batch schema):
    the engine must still make the actual call, apply NO edit, and
    report the exact original with an empty edit list."""

    def _payload_for(self, request_body: dict[str, object], usage: dict[str, int] | None) -> bytes:
        targets = self.parse_targets(request_body)
        inner = {
            "version": "batch-v1",
            "proposals": [
                {
                    "id": target["id"],
                    "keep": True,
                    "replacement": None,
                    "needs_wider_edit": False,
                }
                for target in targets
            ],
        }
        envelope: dict[str, object] = {
            "object": "response",
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps(inner, ensure_ascii=False),
                        }
                    ],
                }
            ],
        }
        if usage is not None:
            envelope["usage"] = usage
        return json.dumps(envelope, ensure_ascii=False).encode()


def _swap_review_fake(harness: _Harness, fake: _DynamicReviewer) -> None:
    """Point the harness's OWNED fake review endpoint at a different
    fake instance (the real engine/reviewer are untouched; only the
    outside-boundary scripted response changes)."""
    harness.review_server._handler = fake.handler()
    harness.reviewer = fake


def _swap_main_payload(harness: _Harness, payload: bytes) -> None:
    """Point the harness's owned fake main upstream at an explicit
    completed payload (the same per-exchange state swap the harness
    itself performs in set_answer)."""
    harness.main_state = _MainState(payload)
    harness.main_server._handler = _main_complete_handler(
        harness.main_requests, harness.main_state
    )


def _swap_main_sse(harness: _Harness, payload: bytes) -> None:
    """Point the harness's owned fake main upstream at an explicit
    completed SSE stream (Content-Type text/event-stream) - the same
    per-exchange state swap as _swap_main_payload, for the strict
    handler's streaming request mode (declared stream must agree with
    the observed upstream content type)."""

    def handle(client: socket.socket) -> None:
        line, headers, body = _read_http_request(client)
        harness.main_requests.append(
            {"line": line, "headers": headers, "body": body}
        )
        harness.main_state.request_event.set()
        _send_http_response(client, payload, "text/event-stream")
        if _hold_until_close(client, harness.main_state.released):
            harness.main_state.closed.set()

    harness.main_server._handler = handle


def _swap_main_status(
    harness: _Harness,
    status: int,
    payload: bytes,
    content_type: str = "application/json",
) -> None:
    """Point the harness's owned fake main upstream at an explicit
    status + payload (non-200 upstream responses) - the same
    per-exchange state swap as _swap_main_payload."""

    def handle(client: socket.socket) -> None:
        line, headers, body = _read_http_request(client)
        harness.main_requests.append(
            {"line": line, "headers": headers, "body": body}
        )
        harness.main_state.request_event.set()
        head = (
            f"HTTP/1.1 {status} ERR\r\n"
            "Content-Type: " + content_type + "\r\n"
            "Content-Length: " + str(len(payload)) + "\r\n"
            "\r\n"
        ).encode("ascii")
        client.sendall(head + payload)
        if _hold_until_close(client, harness.main_state.released):
            harness.main_state.closed.set()

    harness.main_server._handler = handle


# ---------------------------------------------------------------------------
# Transparent-mode (passthrough) real-chain harness: the same claimed
# boundary as _Harness (real transparent ConceptProxy, real
# RepairEngine with the pinned Pandoc and the 038-a tiny real corpus,
# real ResponsesReviewer with its owned transport child) but with the
# 038-a scripted SSE fake main upstream and the owned dynamic fake
# reviewer endpoint, both synthetic fakes OUTSIDE those boundaries.
# The 038-a corpus accepts the possible-typo repair
# presenetliv -> presenetljiv (the batch-parser default "tako" is NOT
# accepted by it), so the fake reviewer's replacement is fixed to the
# corpus-accepted correction.
# ---------------------------------------------------------------------------

_SSE_REPLACEMENT = "presenetljiv"


def _read_http_response_eof(
    client: socket.socket,
) -> tuple[int, dict[str, str], bytes]:
    """Bounded raw response read: Content-Length framed when declared,
    otherwise read to connection close (proxy relay/transformed
    responses are close framed)."""
    client.settimeout(25.0)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = client.recv(65536)
        if not chunk:
            raise OSError("proxy closed before the response headers")
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1").split("\r\n")
    status = int(lines[0].split(" ", 2)[1])
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    length = headers.get("content-length")
    if length is None:
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            body += chunk
    else:
        length = int(length)
        while len(body) < length:
            chunk = client.recv(65536)
            if not chunk:
                break
            body += chunk
    return status, headers, body


def _post_raw(
    port: int,
    path: str,
    body: bytes,
    header_lines: list[bytes],
) -> tuple[int, dict[str, str], bytes]:
    """One raw client POST to an explicit proxy path with explicit
    extra/control header lines; the response is read to completion
    (Content-Length framed or close framed)."""
    client = socket.create_connection(("127.0.0.1", port), timeout=25)
    try:
        head = (
            f"POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        ).encode("ascii")
        for line in header_lines:
            head += line + b"\r\n"
        head += (
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n\r\n"
        )
        client.sendall(head + body)
        return _read_http_response_eof(client)
    finally:
        client.close()


def _start_raw_client(
    port: int,
    body: bytes,
    header_lines: list[bytes],
    cap: _ClientCapture,
    canary: bytes,
) -> threading.Thread:
    """One raw-socket incremental streaming client to
    /v1/responses with an explicit extra/control header list; the
    capture semantics (chunks, canary snapshot, finished) are the
    038-a _ClientCapture contract."""

    def task() -> None:
        client: socket.socket | None = None
        try:
            client = socket.create_connection(
                ("127.0.0.1", port), timeout=25
            )

            def note(piece: bytes) -> None:
                cap.chunks.append(piece)
                if not cap.canary_seen.is_set() and canary in cap.blob:
                    cap.canary_seen_at = time.monotonic()
                    cap.first_canary_snapshot = cap.blob
                    cap.canary_seen.set()

            head = (
                b"POST /v1/responses HTTP/1.1\r\n"
                b"Host: 127.0.0.1\r\n"
                b"Content-Type: application/json\r\n"
            )
            for line in header_lines:
                head += line + b"\r\n"
            head += (
                b"Content-Length: "
                + str(len(body)).encode("ascii")
                + b"\r\n\r\n"
            )
            client.sendall(head + body)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = client.recv(65536)
                if not chunk:
                    raise OSError("proxy closed before response headers")
                data += chunk
            headpart, _, remainder = data.partition(b"\r\n\r\n")
            cap.status = int(headpart.split(b"\r\n", 1)[0].split(b" ", 2)[1])
            if remainder:
                note(remainder)
            while True:
                piece = client.recv(65536)
                if not piece:
                    break
                note(piece)
        except BaseException as exc:  # noqa: BLE001 - recorded, asserted
            cap.error = exc
        finally:
            if client is not None:
                with contextlib.suppress(OSError):
                    client.close()
            cap.finished.set()

    thread = threading.Thread(target=task, daemon=True, name="sse-client")
    thread.start()
    return thread


def _raw_request_head_details(body: bytes, values: list[str]) -> bytes:
    """The 038-a raw request head plus the exact control-header line
    list (no Connection: close, so the later write-half close is the
    cancellation)."""
    head = (
        b"POST /v1/responses HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\n"
    )
    for value in values:
        head += (
            _DETAILS_HEADER.encode("ascii")
            + b": "
            + value.encode("ascii")
            + b"\r\n"
        )
    head += f"Content-Length: {len(body)}\r\n".encode("ascii") + b"\r\n"
    return head + body


def _terminal_response(blob: bytes) -> dict[str, object]:
    """The response object of the LAST response.completed frame of a
    rendered SSE body."""
    marker = b'"type":"response.completed"'
    index = blob.rindex(marker)
    frame_start = blob.rfind(b"data: ", 0, index)
    frame_end = blob.find(b"\n\n", index)
    if frame_start == -1 or frame_end == -1:
        raise AssertionError(
            "no parseable terminal response.completed frame"
        )
    frame = blob[frame_start + len(b"data: ") : frame_end]
    event = json.loads(frame.decode("utf-8"))
    assert event["type"] == "response.completed"
    return event["response"]


class _SseDetailsHarness:
    """One real transparent ConceptProxy (passthrough_mode=True, the
    normal launcher's forwarding mode) with the real RepairEngine
    (pinned Pandoc via CONCEPT_PANDOC, the 038-a tiny real corpus) and
    the real ResponsesReviewer pointed at the OWNED dynamic fake
    reviewer endpoint; the fake main upstream is the 038-a scripted
    SSE/JSON server.  Both fakes are OUTSIDE the claimed boundary."""

    def __init__(
        self,
        records: list[dict[str, object]],
        observed: threading.Event,
        *,
        review_usage: list[dict[str, int] | None] | None = None,
        capture_timeout: float = 15.0,
        delivery_timeout: float = 15.0,
    ) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = _build_corpus(Path(self.temp.name))
        # Reset the scripted upstream class state (one harness at a
        # time in this file; sequential test order).
        _StreamUpstream.before = b""
        _StreamUpstream.after = b""
        _StreamUpstream.chunk = 65536
        _StreamUpstream.inter_frame_sleep = 0.0
        _StreamUpstream.release = None
        _StreamUpstream.barrier_timeout = 4.0
        _StreamUpstream.pre_after_sleep = 0.0
        _StreamUpstream.truncated_after = None
        _StreamUpstream.send_content_length = True
        _StreamUpstream.status = 200
        _StreamUpstream.content_type = "text/event-stream"
        _StreamUpstream.error_body = b"{}"
        _StreamUpstream.recorded = []
        _StreamUpstream.request_received = threading.Event()
        _StreamUpstream.barrier_entered = threading.Event()
        _StreamUpstream.barrier_entered_at = None
        self.reviewer = _DynamicReviewer()
        self.reviewer.usage = review_usage or []
        self.reviewer.replacement = _SSE_REPLACEMENT
        self.review_server = _LoopbackServer(
            "sse-review-upstream", self.reviewer.handler()
        )
        self.review_server.start()
        reviewer = ResponsesReviewer(
            f"http://127.0.0.1:{self.review_server.port}/v1",
            "test-model",
            api_key="fake-key",
        )
        engine = proxy.RepairEngine(self.db, reviewer, pandoc=_real_spec())
        self.upstream_server = _QuietThreadingHTTPServer(
            ("127.0.0.1", 0), _StreamUpstream
        )
        self.proxy_server = proxy.ConceptProxy(
            ("127.0.0.1", 0),
            f"http://127.0.0.1:{self.upstream_server.server_port}",
            engine,
            usage_observer=_usage_observer(records, observed),
            timeout=capture_timeout,
            delivery_timeout=delivery_timeout,
            passthrough_mode=True,
        )
        self.servers = [self.upstream_server, self.proxy_server]
        self.threads: list[threading.Thread] = []
        for name, server in (
            ("sse-main-serve", self.upstream_server),
            ("sse-proxy-serve", self.proxy_server),
        ):
            thread = threading.Thread(
                target=server.serve_forever, name=name, daemon=True
            )
            thread.start()
            self.threads.append(thread)

    @property
    def proxy_port(self) -> int:
        return self.proxy_server.server_address[1]

    def upstream(
        self,
        *,
        before: bytes,
        after: bytes,
        barrier: bool = False,
        chunk: int = 65536,
        inter_frame_sleep: float = 0.0,
        barrier_timeout: float = 4.0,
        pre_after_sleep: float = 0.0,
        truncated_after: int | None = None,
        content_type: str = "text/event-stream",
    ) -> threading.Event:
        """Script one main-upstream response; returns the release
        Event (meaningful only with barrier=True)."""
        _StreamUpstream.before = before
        _StreamUpstream.after = after
        _StreamUpstream.chunk = chunk
        _StreamUpstream.inter_frame_sleep = inter_frame_sleep
        _StreamUpstream.barrier_timeout = barrier_timeout
        _StreamUpstream.pre_after_sleep = pre_after_sleep
        _StreamUpstream.truncated_after = truncated_after
        _StreamUpstream.send_content_length = True
        _StreamUpstream.status = 200
        _StreamUpstream.content_type = content_type
        _StreamUpstream.barrier_entered = threading.Event()
        _StreamUpstream.barrier_entered_at = None
        if barrier:
            _StreamUpstream.release = threading.Event()
            return _StreamUpstream.release
        _StreamUpstream.release = None
        return threading.Event()

    def main_requests(self) -> list[dict[str, object]]:
        return list(_StreamUpstream.recorded)

    def review_calls(self) -> int:
        return len(self.reviewer.requests)

    def close(self) -> None:
        release = _StreamUpstream.release
        if release is not None:
            release.set()
        for server in self.servers:
            with contextlib.suppress(Exception):
                server.shutdown()
            with contextlib.suppress(Exception):
                server.server_close()
        with contextlib.suppress(Exception):
            self.review_server.close()
        for thread in self.threads:
            thread.join(8.0)
        self.temp.cleanup()


class _DetailsCase(unittest.TestCase):
    """Shared lifecycle for one real chain exchange per test."""

    def setUp(self) -> None:
        self.records: list[dict[str, object]] = []
        self.observed = threading.Event()
        self.harness: _Harness | None = None
        self.sse_harness: _SseDetailsHarness | None = None

    def tearDown(self) -> None:
        if self.harness is not None:
            self.harness.close()
        if self.sse_harness is not None:
            self.sse_harness.close()


class DetailsBaselinePositiveTests(_DetailsCase):
    """The focused positive expected-extension request (order 041-a
    verification prerequisite): the smallest opted-in single-edit
    answer through the actual strict HTTP chain.

    EXPECTED ACCEPTED BASE FAILURE: the repaired completed-200
    response carries NO root llm_language_repair at all - the
    failure is the missing extension at the actual HTTP response
    boundary, with the real chain (one actual main forward, one
    actual review attempt, real CPU acceptance, real patcher)
    otherwise fully successful.  Not a fixture or import failure.
    """

    def test_opted_in_single_edit_json_exact_schema(self) -> None:
        answer = "To je presenetliv zapisano."
        self.assertEqual(answer.count(_TARGET), 1)
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        # The actual repair still happens: one accepted
        # length-changing edit applied by the real patcher.
        delivered = envelope["output"][0]["content"][0]["text"]
        self.assertEqual(delivered, answer.replace(_TARGET, _REPLACEMENT))
        # Details must not add review calls or change accounting:
        # exactly one actual attempt with the validated known usage.
        self.assertEqual(len(harness.reviewer.requests), 1)
        self.assertEqual(
            self.records,
            [
                {
                    "review_calls": 1,
                    "reviewer_usage": dict(_U1),
                }
            ],
        )
        self.assertTrue(self.observed.wait(10.0), "no usage observation")
        # The exact extension: root llm_language_repair with the
        # ordered schema and the exact fixture-derived coordinates.
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 1)
        (start, end), = spans
        edits = [
            _expected_edit(
                start, end, _REPLACEMENT, []
            )
        ]
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, edits),
        )
        # Only the extension root field was added; every other root
        # value is preserved exactly.
        base = json.loads(harness.source.decode("utf-8"))
        self.assertEqual(set(envelope) - set(base), {_FIELD})
        for key in base:
            if key != "output":
                self.assertEqual(envelope[key], base[key])


class DetailsHeaderMatrixTests(_DetailsCase):
    """Opt-in negotiation and unchanged prior behavior (order 041-a,
    requirement 1/6).  Absent and zero values are OFF: exactly the
    prior behavior, no extension, no header side effect on the main
    request.  These cases PASS on the accepted base and must keep
    passing after the implementation."""

    def test_header_absent_exact_prior_behavior(self) -> None:
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, [])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        # The actual repair happens; the old client sees exactly the
        # prior behavior: no extension anywhere.
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _expected_repaired(answer),
        )
        self.assertNotIn(_FIELD, envelope)
        # The main request carries no control header and no new
        # body field: the ordinary contract body, byte-identical.
        self.assertEqual(len(harness.main_requests), 1)
        request = harness.main_requests[0]
        for key in request["headers"]:
            self.assertNotEqual(key.lower(), _DETAILS_HEADER.lower())
        self.assertEqual(request["body"], _contract_body(False))

    def test_header_zero_value_means_off(self) -> None:
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["0"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        # One value "0" is off: identical to the absent-header
        # behavior.
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _expected_repaired(answer),
        )
        self.assertNotIn(_FIELD, envelope)
        self.assertEqual(len(harness.main_requests), 1)
        for key in harness.main_requests[0]["headers"]:
            self.assertNotEqual(key.lower(), _DETAILS_HEADER.lower())


class DetailsJsonContentTests(_DetailsCase):
    """Exact content of the extension for supported opted-in
    completed plain-answer JSON (order 041-a, requirement 2/6)."""

    def test_opted_in_clean_answer_zero_edits_exact_original(self) -> None:
        # No correction means exact original_text plus empty edits -
        # not an extra call: the zero-suspicion answer makes zero
        # review attempts; the delivered ANSWER is exactly the
        # original text (no edit at all), and the envelope is
        # unchanged apart from the presentation extension (which
        # necessarily adds the new root field to the body bytes).
        answer = _CLEAN
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(len(harness.reviewer.requests), 0)
        envelope = json.loads(body.decode("utf-8"))
        # The exact extension: complete, exact original text, EMPTY
        # edits.
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, []),
        )
        # The answer text is exactly the original, and every other
        # parsed envelope field is preserved exactly (ONLY the new
        # root field excluded from the comparison).
        base = json.loads(harness.source.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            base,
        )


class DetailsHeaderMatrixRejectionTests(_DetailsCase):
    """Invalid and duplicate control-header values are fixed
    content-free 400 rejections BEFORE any main/review work
    (order 041-a, requirement 1).  EXPECTED ACCEPTED BASE FAILURE:
    the base performs no such validation - the request proceeds to
    actual main work instead of the pre-main 400."""

    def _run(self, values: list[str]) -> tuple[int, bytes, _Harness]:
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, values)
        return status, body, harness

    def _assert_rejected(self, status: int, body: bytes, harness: _Harness) -> None:
        self.assertEqual(status, 400)
        # Zero main, zero review: the rejection is pre-forward and
        # pre-dispatch.
        self.assertEqual(harness.main_requests, [])
        self.assertEqual(len(harness.reviewer.requests), 0)
        # Fixed content-free reason: the exact contract shape, no
        # caller value reflected, no answer text.
        parsed = json.loads(body.decode("utf-8"))
        self.assertEqual(
            parsed,
            {
                "error": {
                    "type": "invalid_request",
                    "code": "REPAIR_DETAILS_HEADER_INVALID",
                }
            },
        )

    def test_header_invalid_value_rejects_400_before_main(self) -> None:
        status, body, harness = self._run(["2"])
        self._assert_rejected(status, body, harness)

    def test_header_duplicate_rejects_400_before_main(self) -> None:
        # Two lines of the same control header: duplicates reject
        # even when every line is a valid "1".
        status, body, harness = self._run(["1", "1"])
        self._assert_rejected(status, body, harness)

    def test_transparent_handler_invalid_and_duplicate_reject_400(
        self,
    ) -> None:
        # The transparent (passthrough) handler has its OWN ingress
        # parse (order 041-a, R1: decided after the admission/intake
        # precedence and BEFORE the upstream connection and any
        # review work), so the same fixed content-free 400 contract
        # is proven here on that separate code path.  Subcases:
        # invalid value "2" and duplicate "1","1" (duplicates reject
        # even when every line is a valid "1").  Both yield ZERO
        # actual main requests (the owned scripted upstream records
        # none) and ZERO review calls.  EXPECTED ACCEPTED BASE
        # FAILURE: the base relays the header and performs actual
        # main work instead of the pre-connection 400.
        body = _contract_body(False)
        for values in (("2",), ("1", "1")):
            self.sse_harness = _SseDetailsHarness(
                self.records, self.observed
            )
            harness = self.sse_harness
            lines = [
                _DETAILS_HEADER.encode("ascii")
                + b": "
                + value.encode("ascii")
                for value in values
            ]
            status, _headers, resp = _post_raw(
                harness.proxy_port,
                "/v1/responses",
                body,
                lines,
            )
            self.assertEqual(status, 400)
            # Zero main, zero review: the rejection is
            # pre-connection and pre-dispatch.
            self.assertEqual(harness.main_requests(), [])
            self.assertEqual(harness.review_calls(), 0)
            # Fixed content-free reason: the exact contract shape,
            # no caller value reflected, no answer text.
            self.assertEqual(
                json.loads(resp.decode("utf-8")),
                {
                    "error": {
                        "type": "invalid_request",
                        "code": "REPAIR_DETAILS_HEADER_INVALID",
                    }
                },
            )
            self.assertEqual(self.records, [])
            harness.close()
            self.sse_harness = None


class DetailsHeaderOnMatrixTests(_DetailsCase):
    """Opt-in value acceptance and strict-mode header hygiene
    (order 041-a, requirement 1/6)."""

    def test_header_trimmed_one_is_on(self) -> None:
        # One trimmed "1" means on: the extension is present with
        # the exact content (EXPECTED ACCEPTED BASE FAILURE: the
        # base ignores the header entirely - no extension).
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, [" 1 "])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _expected_repaired(answer),
        )
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 2)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, edits),
        )

    def test_strict_main_request_never_carries_control_header(self) -> None:
        # Opt-in ON: the strict narrow forward policy (content-type,
        # accept, authorization only) must keep dropping the private
        # control header, and the header must never be added to the
        # main JSON.  This case PASSes on the accepted base (the
        # narrow policy already drops it) and must keep passing.
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(len(harness.main_requests), 1)
        request = harness.main_requests[0]
        for key in request["headers"]:
            self.assertNotEqual(key.lower(), _DETAILS_HEADER.lower())
        self.assertEqual(request["body"], _contract_body(False))


class DetailsAcceptanceTests(_DetailsCase):
    """Reviewer keep/reject and batch failure through the actual
    CPU acceptance and composed validation (order 041-a,
    requirement 3/6): candidate/rejected proposals never appear as
    applied edits."""

    def test_reviewer_keep_yields_no_edit(self) -> None:
        answer = _SHORT
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_review_fake(harness, _KeepReviewer())
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        # The actual call happened; keep means the answer is
        # delivered EXACTLY original and the extension is complete
        # with an EMPTY edit list (not a fabricated one, not an
        # extra call).
        self.assertEqual(len(harness.reviewer.requests), 1)
        envelope = json.loads(body.decode("utf-8"))
        # The delivered envelope is unchanged apart from the new
        # root field (the answer text stays exactly original).
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            json.loads(harness.source.decode("utf-8")),
        )
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, []),
        )

    def test_reviewer_identity_replacement_not_applied(self) -> None:
        # A structurally valid proposal whose replacement is
        # IDENTICAL to the original slice is rejected by the real
        # CPU acceptance (reject identity): it must never appear as
        # an applied edit, and the answer stays exactly original.
        answer = _SHORT
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        harness.reviewer.replacement = _TARGET  # identity replacement
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(len(harness.reviewer.requests), 1)
        envelope = json.loads(body.decode("utf-8"))
        # The delivered envelope is unchanged apart from the new
        # root field (the answer text stays exactly original).
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            json.loads(harness.source.decode("utf-8")),
        )
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, []),
        )

    def test_batch_failure_returns_original_empty_edits(self) -> None:
        # The second ACTUAL batch fails structurally (invalid JSON):
        # the completed original is delivered with all proposals
        # discarded (order 041-a requirement 6: batch failure
        # original + empty edits), while the actual attempts and the
        # unknown aggregate stay recorded.
        answer = _long_answer()  # two actual batches at default B
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        harness.reviewer.fault_payloads = [None, b"not-json"]
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(len(harness.reviewer.requests), 2)
        envelope = json.loads(body.decode("utf-8"))
        # The delivered envelope is unchanged apart from the new
        # root field (the answer text stays exactly original).
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            json.loads(harness.source.decode("utf-8")),
        )
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, []),
        )


class DetailsRequestLocalTests(_DetailsCase):
    """Request-local outcome reset (order 041-a, requirement 3/6):
    one proxy, two sequential exchanges - the second exchange's
    extension must be its own, never the first request's result."""

    def test_request_local_reset_between_exchanges(self) -> None:
        answer = _long_answer()
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1), dict(_U2)],
        )
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        first = json.loads(body.decode("utf-8"))
        self.assertEqual(
            len(first.get(_FIELD, {}).get("edits", [])), 6
        )
        # Second exchange on the SAME owned proxy: the clean answer
        # observes zero calls and its OWN exact extension (original
        # text is the clean answer, edits empty) - no leak of the
        # first request's original/edits, no carryover usage.
        harness.set_answer(_CLEAN)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        second = json.loads(body.decode("utf-8"))
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(
            second.get(_FIELD),
            _complete_details(_CLEAN, 0, 0, []),
        )
        self.assertEqual(
            self.records,
            [
                {
                    "review_calls": 2,
                    "reviewer_usage": {
                        "input_tokens": _U1["input_tokens"]
                        + _U2["input_tokens"],
                        "output_tokens": _U1["output_tokens"]
                        + _U2["output_tokens"],
                        "total_tokens": _U1["total_tokens"]
                        + _U2["total_tokens"],
                    },
                },
                {"review_calls": 0, "reviewer_usage": "unknown"},
            ],
        )


class DetailsSchemaEdgeTests(_DetailsCase):
    """Actual output-part identity, Unicode/CRLF code-point
    coordinates, and the bounded unavailable paths (order 041-a,
    requirement 2/4/6)."""

    def test_output_part_identity_after_reasoning_item(self) -> None:
        # The actual existing answer part is identified even after
        # a reasoning item: output_index is the REAL list position
        # of the answer message, and the reasoning item is preserved
        # byte-for-byte while never appearing in the extension.
        answer = "To je presenetliv zapisano."
        envelope = json.loads(_json_completed(answer))
        envelope["output"].insert(
            0,
            {
                "type": "reasoning",
                "id": "rs_1",
                "status": "completed",
                "summary": [],
                "content": [
                    {"type": "reasoning_text", "text": "razmislek."}
                ],
            },
        )
        envelope["output"][1]["output_index"] = 1
        payload = json.dumps(
            envelope, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        result = json.loads(body.decode("utf-8"))
        # The reasoning item is preserved exactly (index 0) and the
        # repaired answer part is at its real position (index 1).
        self.assertEqual(
            result["output"][0],
            {
                "type": "reasoning",
                "id": "rs_1",
                "status": "completed",
                "summary": [],
                "content": [
                    {"type": "reasoning_text", "text": "razmislek."}
                ],
            },
        )
        self.assertEqual(
            result["output"][1]["content"][0]["text"],
            answer.replace(_TARGET, _REPLACEMENT),
        )
        spans = _find_spans(answer, _TARGET)
        (start, end), = spans
        self.assertEqual(
            result.get(_FIELD),
            _complete_details(
                answer,
                1,  # the ACTUAL output index after the reasoning item
                0,
                [_expected_edit(start, end, _REPLACEMENT, [])],
            ),
        )

    def test_unicode_crlf_codepoint_coordinates(self) -> None:
        # Multi-byte code points BEFORE the edit and a CRLF in the
        # answer: the coordinates must be UNMODIFIED-original Unicode
        # code points (never UTF-8 bytes or UTF-16 units), so the
        # delivered slice at the repaired offsets is exactly the
        # replacement and the CRLF survives.
        answer = "Ž je presenetliv zapisano.\r\nTo."
        self.assertEqual(len(answer), 31)  # code points
        self.assertEqual(len(answer.encode("utf-8")), 32)  # UTF-8 bytes
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            corpus_factory=_build_multibyte_corpus,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        delivered = envelope["output"][0]["content"][0]["text"]
        self.assertEqual(delivered, "Ž je tako zapisano.\r\nTo.")
        spans = _find_spans(answer, _TARGET)
        (start, end), = spans
        self.assertEqual(start, 5)
        self.assertEqual(end, 16)
        # The SAME offsets are NOT byte offsets (the multi-byte Ž
        # before the target shifts byte space by one): the code-point
        # slices are exact while the byte slice at the same indices is
        # not the target.
        self.assertEqual(answer[start:end], _TARGET)
        raw = answer.encode("utf-8")
        self.assertNotEqual(raw[start:end], _TARGET.encode("utf-8"))
        self.assertEqual(
            delivered[start:start + len(_REPLACEMENT)], _REPLACEMENT
        )
        self.assertIn("\r\n", delivered)
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(
                answer,
                0,
                0,
                [_expected_edit(start, end, _REPLACEMENT, [])],
            ),
        )

    def test_upstream_field_collision_replaced_unavailable(self) -> None:
        # An opted-in supported upstream response that ALREADY
        # carries root llm_language_repair: the reserved field is
        # replaced with the small unavailable/UPSTREAM_FIELD_
        # COLLISION object; the normal answer/review eligibility is
        # unchanged.  With the header OFF the default behavior
        # preserves the unknown upstream field as before.
        answer = _SHORT
        base = json.loads(_json_completed(answer))
        injected = {"version": 999, "injected": "untrusted"}
        base[_FIELD] = dict(injected)
        payload = json.dumps(
            base, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        # The untrusted value is gone, replaced by the fixed small
        # unavailable object.
        self.assertNotEqual(envelope.get(_FIELD), injected)
        self.assertEqual(
            envelope.get(_FIELD),
            _unavailable("UPSTREAM_FIELD_COLLISION"),
        )
        # Normal answer/review eligibility unchanged: the answer is
        # still actually repaired with one actual call, and every
        # other field is preserved.
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _expected_repaired(answer),
        )
        self.assertEqual(len(harness.reviewer.requests), 1)
        # The ONE expected change against the upstream base: the
        # delivered answer text is the approved repair; every other
        # parsed field is preserved.
        expected_base = copy.deepcopy(
            {
                key: value for key, value in base.items() if key != _FIELD
            }
        )
        expected_base["output"][0]["content"][0]["text"] = (
            _expected_repaired(answer)
        )
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            expected_base,
        )
        # Header OFF: the same upstream field is preserved as before
        # (default-off byte/semantic preservation).
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, [])
        self.assertEqual(status, 200)
        off = json.loads(body.decode("utf-8"))
        self.assertEqual(off.get(_FIELD), injected)

    def test_details_too_large_unavailable(self) -> None:
        # The completed plain answer is fully supported, but the
        # serialized extension (original_text alone exceeds the
        # 1,048,576 UTF-8 byte cap) must degrade to the small
        # unavailable/DETAILS_TOO_LARGE object - no partial or
        # truncated original or edit list, no new review work (the
        # analysis-eligibility gate already bypasses review for an
        # answer this long, so zero review calls).
        answer = "a" * 1_048_577
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(len(harness.reviewer.requests), 0)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        self.assertEqual(
            envelope.get(_FIELD), _unavailable("DETAILS_TOO_LARGE")
        )

    def test_logprobs_policy_independent_of_details(self) -> None:
        # Defined logprobs removal (key presence, defined locations)
        # applies independently of the details opt-in: with the
        # extension present and an edit applied, the defined
        # output_text-part logprobs location is still removed.
        answer = _SHORT
        base = json.loads(_json_completed(answer))
        base["output"][0]["content"][0]["logprobs"] = [
            {"token": "synthetic", "logprob": 0.0}
        ]
        payload = json.dumps(
            base, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        part = envelope["output"][0]["content"][0]
        self.assertNotIn("logprobs", part)
        self.assertEqual(
            part["text"], _expected_repaired(answer)
        )
        spans = _find_spans(answer, _TARGET)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, edits),
        )

    def test_other_root_metadata_preserved(self) -> None:
        # Unrelated root metadata and usage are preserved exactly
        # when the extension is added.
        answer = _SHORT
        base = json.loads(_json_completed(answer))
        base["metadata"] = {"k": "v"}
        base["usage"] = {"input_tokens": 1, "output_tokens": 2,
                         "total_tokens": 3}
        payload = json.dumps(
            base, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(envelope["metadata"], {"k": "v"})
        self.assertEqual(
            envelope["usage"],
            {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
        )
        spans = _find_spans(answer, _TARGET)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, edits),
        )


class DetailsMultiEditTests(_DetailsCase):
    """Exact coordinates for repeated length-changing occurrences
    across actual batches (order 041-a, requirement 2/6)."""

    def test_multiple_length_changing_repeated_occurrences(
        self,
    ) -> None:
        answer = _long_answer()  # 6 occurrences, 1025 cp, B = 2
        self.assertEqual(answer.count(_TARGET), 6)
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1), dict(_U2)],
        )
        harness = self.harness
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        delivered = envelope["output"][0]["content"][0]["text"]
        self.assertEqual(delivered, _expected_repaired(answer))
        # Two ACTUAL sequential batches (1025 cp at the default
        # 1024 cp unit); details must not add a third or drop one.
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [
                {
                    "review_calls": 2,
                    "reviewer_usage": {
                        "input_tokens": _U1["input_tokens"]
                        + _U2["input_tokens"],
                        "output_tokens": _U1["output_tokens"]
                        + _U2["output_tokens"],
                        "total_tokens": _U1["total_tokens"]
                        + _U2["total_tokens"],
                    },
                }
            ],
        )
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 6)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        details = envelope.get(_FIELD)
        self.assertEqual(details, _complete_details(answer, 0, 0, edits))
        # The delivered-coordinate arithmetic double-checked against
        # the delivered text itself: every delivered slice at the
        # repaired offsets is exactly the replacement, and the exact
        # fixture-derived composition equals the delivered answer.
        for edit in details["edits"]:
            self.assertEqual(
                delivered[
                    edit["repaired_start"] : edit["repaired_end"]
                ],
                edit["replacement"],
            )
        composed = answer
        for edit in sorted(
            edits,
            key=lambda item: (
                item["original_end"],
                item["original_start"],
            ),
            reverse=True,
        ):
            composed = (
                composed[: edit["original_start"]]
                + edit["replacement"]
                + composed[edit["original_end"] :]
            )
        self.assertEqual(composed, delivered)
        # Sorted, disjoint, replacement never equals the original
        # slice.
        # Intentionally unequal lengths (n vs n-1) for the adjacent
        # pairs; the edit/span pairing is expected equal.
        for first, second in zip(edits, edits[1:], strict=False):
            self.assertLess(first["original_end"], second["original_start"])
        for edit, (s, e) in zip(edits, spans, strict=True):
            self.assertNotEqual(answer[s:e], edit["replacement"])


class DetailsSseTests(_DetailsCase):
    """Opt-in details through the ACTUAL transparent (passthrough)
    chain: real transparent ConceptProxy (early-reasoning prefix
    emission, buffered tail), real RepairEngine (pinned Pandoc,
    038-a tiny real corpus), real ResponsesReviewer, with the 038-a
    scripted main upstream and the owned dynamic fake reviewer
    endpoint OUTSIDE those boundaries (order 041-a, requirement 5/6).

    The extension exists ONLY in the terminal response.completed
    response object, once; no new event kinds; prefix frames stay
    ordinal-identical.  Opted-in positives are EXPECTED ACCEPTED
    BASE FAILURES at the actual HTTP stream/response boundary (no
    llm_language_repair, control header relayed instead of consumed);
    protection/prior-behavior cases pass on the accepted base and
    must keep passing.
    """

    def test_opted_in_transparent_json_exact_schema(self) -> None:
        # Opt-in ON through the ACTUAL transparent handler with a
        # completed JSON upstream response: the real repair chain
        # runs, the delivered envelope carries ONLY the new root
        # field with the ordered complete schema, and the private
        # control header is CONSUMED - absent from the main upstream
        # request while every other header is preserved.
        # EXPECTED ACCEPTED BASE FAILURE: the base relays the control
        # header to main and adds no llm_language_repair.
        answer = _ANSWER
        self.sse_harness = _SseDetailsHarness(
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.sse_harness
        payload = _json_completed(answer)
        harness.upstream(
            before=payload,
            after=b"",
            content_type="application/json",
        )
        status, _headers, body = _post_raw(
            harness.proxy_port,
            "/v1/responses",
            _contract_body(False),
            [
                _DETAILS_HEADER.encode("ascii") + b": 1",
                b"X-Custom-Keep: v1",
            ],
        )
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _EDITED_ANSWER,
        )
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 2)
        delta = len(_SSE_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _SSE_REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(answer, 0, 0, edits),
        )
        # The control header is consumed; every other header and the
        # body are preserved exactly on the main upstream request.
        requests = harness.main_requests()
        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertEqual(request["body"], _contract_body(False))
        # The scripted upstream records on-wire (original-case)
        # header names; compare a normalized lowercase view so the
        # case-insensitive contract is tested, not a casing accident.
        main_headers = {
            key.lower(): value for key, value in request["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)
        self.assertEqual(main_headers.get("x-custom-keep"), "v1")
        # One actual review attempt with the validated known usage;
        # the extension adds no second call.
        self.assertEqual(harness.review_calls(), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )
        # Every other parsed root field is preserved against the
        # upstream base (the one expected change is the repaired
        # answer text).
        base = json.loads(payload.decode("utf-8"))
        expected_base = copy.deepcopy(
            {key: value for key, value in base.items() if key != _FIELD}
        )
        expected_base["output"][0]["content"][0]["text"] = _EDITED_ANSWER
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            expected_base,
        )

    def test_opted_in_sse_terminal_details_exact_schema(self) -> None:
        # The actual streaming barrier: the proven reasoning prefix
        # reaches the client while the main upstream is still HELD
        # before any answer frame exists; the extension may exist
        # ONLY in the actual terminal response.completed response
        # object, once, after final capture/review.
        # EXPECTED ACCEPTED BASE FAILURE: the base terminal response
        # object carries no llm_language_repair at all.
        answer = _ANSWER
        self.assertEqual(answer.count(_TARGET), 2)
        self.sse_harness = _SseDetailsHarness(
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.sse_harness
        payloads = _stream_payloads(answer)
        cut = _prefix_cut(False)
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        release = harness.upstream(
            before=before, after=after, barrier=True
        )
        cap = _ClientCapture()
        thread = _start_raw_client(
            harness.proxy_port,
            _contract_body(True),
            [_DETAILS_HEADER.encode("ascii") + b": 1"],
            cap,
            _CANARY,
        )
        self.assertTrue(cap.canary_seen.wait(10.0), "canary not received")
        # The fake flushed its whole blocked section: the upstream
        # entered the barrier only after the canary frame was sent.
        self.assertTrue(
            _StreamUpstream.barrier_entered.wait(10.0),
            "upstream never entered the barrier",
        )
        # Pre-release client view: the proven reasoning prefix is
        # ordinal-identical and early; nothing final exists yet.
        snapshot = cap.blob
        self.assertTrue(snapshot.startswith(before))
        self.assertNotIn(b"llm_language_repair", snapshot)
        self.assertNotIn(b'"type":"response.completed"', snapshot)
        self.assertNotIn(_ANSWER.encode("utf-8"), snapshot)
        release.set()
        thread.join(30)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        blob = cap.blob
        self.assertIn(b'"type":"response.completed"', blob)
        # The extension name appears exactly once in the whole
        # stream: it exists only in the actual terminal completion.
        self.assertEqual(blob.count(b"llm_language_repair"), 1)
        # Every non-terminal frame is byte-identical to the
        # documented edited stream (ordinal identity, repaired
        # answer copies).
        expected_full = _expected_edited(payloads, _EDITED_ANSWER)
        marker = b'"type":"response.completed"'
        self.assertEqual(
            blob[:blob.rindex(marker)],
            expected_full[:expected_full.rindex(marker)],
        )
        # The terminal response object: the exact prior shape plus
        # ONLY the new root field with the ordered complete schema
        # (actual output index 1 after the reasoning item).
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 2)
        delta = len(_SSE_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _SSE_REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        expected_terminal = copy.deepcopy(payloads[-1])
        self.assertEqual(expected_terminal["type"], "response.completed")
        response = expected_terminal["response"]
        response["output"][1]["content"][0]["text"] = _EDITED_ANSWER
        response[_FIELD] = _complete_details(answer, 1, 0, edits)
        self.assertEqual(_terminal_response(blob), response)
        # One actual review attempt with the validated known usage;
        # the details add no second call.
        self.assertEqual(harness.review_calls(), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )

    def test_sse_off_exact_prior_behavior(self) -> None:
        # Header OFF in transparent streaming: exactly the prior
        # 038-a behavior - the proven reasoning prefix is early, the
        # delivered stream is byte-identical to the documented
        # edited stream, NO extension bytes anywhere, one actual
        # review with the validated known usage.  This case PASSES on
        # the accepted base and must keep passing after the
        # implementation (ordinary old clients unchanged).
        answer = _ANSWER
        self.sse_harness = _SseDetailsHarness(
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.sse_harness
        payloads = _stream_payloads(answer)
        cut = _prefix_cut(False)
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        release = harness.upstream(
            before=before, after=after, barrier=True
        )
        cap = _ClientCapture()
        thread = _start_raw_client(
            harness.proxy_port,
            _contract_body(True),
            [],
            cap,
            _CANARY,
        )
        self.assertTrue(cap.canary_seen.wait(10.0), "canary not received")
        self.assertTrue(
            _StreamUpstream.barrier_entered.wait(10.0),
            "upstream never entered the barrier",
        )
        snapshot = cap.blob
        self.assertTrue(snapshot.startswith(before))
        self.assertNotIn(_ANSWER.encode("utf-8"), snapshot)
        self.assertNotIn(b'"type":"response.completed"', snapshot)
        release.set()
        thread.join(30)
        self.assertFalse(thread.is_alive())
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Byte-identical to the documented edited stream: the off
        # client sees exactly the prior behavior, no extension
        # bytes anywhere.
        self.assertEqual(cap.blob, _expected_edited(payloads, _EDITED_ANSWER))
        self.assertNotIn(b"llm_language_repair", cap.blob)
        self.assertEqual(harness.review_calls(), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )

    def test_sse_incomplete_no_details_no_invented_completion(self) -> None:
        # Opt-in ON but the main upstream declares the FULL body and
        # sends fewer bytes (cut inside the answer delta frame)
        # before closing: the client view stays observably
        # incomplete (the 038-a contract), zero review, and NO
        # details object, no synthetic terminal, no answer release.
        # Protection preservation: this case PASSES on the accepted
        # base.
        answer = _ANSWER
        self.sse_harness = _SseDetailsHarness(
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.sse_harness
        payloads = _stream_payloads(answer)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        delta_marker = after.index(b'"type":"response.output_text.delta"')
        harness.upstream(
            before=before,
            after=after,
            truncated_after=delta_marker + 20,
        )
        status, _headers, body = _post_raw(
            harness.proxy_port,
            "/v1/responses",
            _contract_body(True),
            [_DETAILS_HEADER.encode("ascii") + b": 1"],
        )
        _assert_incomplete_client_view(status, body)
        # No presentation extension is invented for an incomplete
        # main, and no review of the incomplete answer happened.
        self.assertNotIn(b"llm_language_repair", body)
        self.assertNotIn(b'"edits"', body)
        self.assertEqual(harness.review_calls(), 0)
        self.assertEqual(self.records, [])

    def test_sse_cancel_after_prefix_no_synthetic_terminal(self) -> None:
        # Opt-in ON; the client write-half-closes (FIN) after the
        # proven reasoning prefix while the main upstream is still
        # held on its barrier: the abort stays QUIET - no synthetic
        # terminal/[DONE]/answer/details for the dead client, no
        # review dispatch, one content-free abort observation, and
        # the proxy stays healthy for the next request.
        # Protection preservation: this case PASSES on the accepted
        # base.
        answer = _ANSWER
        self.sse_harness = _SseDetailsHarness(
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.sse_harness
        payloads = _stream_payloads(answer)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        release = harness.upstream(
            before=before,
            after=after,
            barrier=True,
            inter_frame_sleep=0.004,
            barrier_timeout=4.0,
        )
        client = socket.create_connection(
            ("127.0.0.1", harness.proxy_port), timeout=20.0
        )
        try:
            client.sendall(_raw_request_head_details(_contract_body(True), ["1"]))
            # The owned setup write completed: the synthetic main saw
            # the whole request BEFORE the client write-half-closes.
            self.assertTrue(
                _StreamUpstream.request_received.wait(15.0),
                "main upstream never received the request",
            )
            time.sleep(0.3)
            client.shutdown(socket.SHUT_WR)  # FIN
            # Give the disconnect watcher and the guard bounded time
            # to act; the answer has not completed yet (upstream
            # still blocked on its barrier), so no review may be
            # dispatched.
            time.sleep(1.5)
            self.assertEqual(
                harness.review_calls(), 0,
                "review dispatched after client disconnect",
            )
            release.set()
            time.sleep(0.5)
            self.assertEqual(harness.review_calls(), 0)
            self.assertEqual(len(harness.main_requests()), 1)
            # Quiet abort: the proxy closes the cancelled connection
            # and the drained bytes carry no synthetic terminal,
            # no answer, no details metadata.
            drained, saw_eof = _drain_until_eof(client)
            self.assertTrue(
                saw_eof,
                "proxy did not close the cancelled connection",
            )
            self.assertNotIn(b"llm_language_repair", drained)
            self.assertNotIn(b'"type":"response.completed"', drained)
            self.assertNotIn(b"[DONE]", drained)
            self.assertNotIn(_ANSWER.encode("utf-8"), drained)
            # The accepted transparent FIN-cancel contract (verified
            # by the private probe against the accepted base,
            # D041/probe-cancel-observation2.py): the abort is quiet
            # and engine begin/abort never runs on this path, so NO
            # abort usage observation is emitted for the cancelled
            # request (the strict-mode abort-observation contract is
            # separately covered by the strict cancellation tests).
            self.assertEqual(self.records, [])
        finally:
            client.close()
        # Proxy still healthy: a following clean zero-review request
        # round-trips byte-identically with zero review.
        clean = _stream_payloads(_CLEAN)
        clean_source = _render_stream(clean)
        harness.upstream(before=clean_source, after=b"")
        status, body = _one_shot(
            harness.proxy_port, _contract_body_stream()
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, clean_source)
        self.assertEqual(harness.review_calls(), 0)
        self.assertEqual(
            self.records[-1],
            {"review_calls": 0, "reviewer_usage": "unknown"},
        )

    def test_transparent_strips_header_arbitrary_route(self) -> None:
        # A non-Responses transparent route: the private control
        # header is CONSUMED (case-insensitively) from ALL upstream
        # forwarding, while the route otherwise preserves its
        # existing relay behavior byte-for-byte (path, body, other
        # headers).  EXPECTED ACCEPTED BASE FAILURE: the base relays
        # the control header to the arbitrary-route upstream.
        self.sse_harness = _SseDetailsHarness(self.records, self.observed)
        harness = self.sse_harness
        relayed = b'{"x":1}'
        harness.upstream(
            before=relayed, after=b"", content_type="application/json"
        )
        status, _headers, body = _post_raw(
            harness.proxy_port,
            "/v1/other",
            relayed,
            [
                _DETAILS_HEADER.encode("ascii") + b": 1",
                b"X-Custom-Keep: v2",
            ],
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, relayed)
        requests = harness.main_requests()
        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertEqual(request["path"], "/v1/other")
        self.assertEqual(request["body"], relayed)
        # Normalized lowercase view: the scripted upstream records
        # on-wire (original-case) header names.
        main_headers = {
            key.lower(): value for key, value in request["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)
        self.assertEqual(main_headers.get("x-custom-keep"), "v2")
        self.assertEqual(harness.review_calls(), 0)


class DetailsStrictSseTests(_DetailsCase):
    """Opt-in details through the ACTUAL strict (non-passthrough)
    handler with a declared streaming request and a completed SSE
    upstream response (order 041-a, requirement 6: both handler
    modes and JSON/SSE).  The strict delivery is Content-Length
    framed, so the full transformed stream is readable in one
    bounded response."""

    def test_opted_in_strict_sse_terminal_details_exact_schema(self) -> None:
        # Declared stream:true, real completed SSE upstream, opted
        # in: the transformed stream is byte-identical to the
        # documented edited stream up to the terminal frame, and the
        # extension exists exactly once - in the terminal
        # response.completed response object (actual output index 1
        # after the reasoning item).  The private control header is
        # absent from the main request.
        answer = _SHORT
        self.assertEqual(answer.count(_TARGET), 2)
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        payloads = _stream_payloads(answer)
        _swap_main_sse(harness, _render_stream(payloads))
        status, _headers, body = _strict_exchange_details(
            harness, ["1"], body=_contract_body(True),
        )
        self.assertEqual(status, 200)
        # The main request carries the declared streaming contract
        # body and no control header.
        self.assertEqual(len(harness.main_requests), 1)
        request = harness.main_requests[0]
        self.assertEqual(request["body"], _contract_body(True))
        main_headers = {
            key.lower(): value for key, value in request["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)
        blob = body
        self.assertIn(b'"type":"response.completed"', blob)
        self.assertEqual(blob.count(b"llm_language_repair"), 1)
        expected_full = _expected_edited(payloads, _expected_repaired(answer))
        marker = b'"type":"response.completed"'
        self.assertEqual(
            blob[:blob.rindex(marker)],
            expected_full[:expected_full.rindex(marker)],
        )
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 2)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        expected_terminal = copy.deepcopy(payloads[-1])
        self.assertEqual(expected_terminal["type"], "response.completed")
        response = expected_terminal["response"]
        response["output"][1]["content"][0]["text"] = _expected_repaired(answer)
        response[_FIELD] = _complete_details(answer, 1, 0, edits)
        self.assertEqual(_terminal_response(blob), response)
        # One actual review attempt with the validated known usage.
        self.assertEqual(len(harness.reviewer.requests), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )

    def test_strict_sse_off_exact_prior_behavior(self) -> None:
        # Declared stream:true, opted OUT: the strict delivery is
        # byte-identical to the documented edited stream, NO
        # extension bytes anywhere, one actual review.  Prior
        # behavior preserved.
        answer = _SHORT
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        payloads = _stream_payloads(answer)
        _swap_main_sse(harness, _render_stream(payloads))
        status, _headers, body = _strict_exchange_details(
            harness, [], body=_contract_body(True),
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, _expected_edited(payloads, _expected_repaired(answer)))
        self.assertNotIn(b"llm_language_repair", body)
        self.assertEqual(len(harness.reviewer.requests), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )


class DetailsComposedRollbackTests(_DetailsCase):
    """Composed conflict rollback through the REAL combined
    validator (order 041-a, requirement 6): the existing real
    CrossBatchInteractionTests conflict fixture is reused AS-IS -
    same tiny conflict corpus, same answer, same real operator
    policy (targets_per_call = 1, characters_per_call = 7) forcing
    the two individually-accepted conflicting sites into SEPARATE
    actual batches.  The real combined validator rejects the whole
    interacting group, so the completed original is delivered and
    the extension must be complete with EMPTY edits (the final
    surviving edit set after composed rollback) - never the
    individually accepted proposals."""

    def setUp(self) -> None:
        super().setUp()
        self.policy_temp = tempfile.TemporaryDirectory()
        self.policy_dir = Path(self.policy_temp.name)

    def tearDown(self) -> None:
        if self.harness is not None:
            self.harness.close()
        self.policy_temp.cleanup()

    def test_conflicting_group_rollback_delivers_empty_edits(self) -> None:
        policy = _load_candidate_loader().load_review_policy(
            _write_toml(
                self.policy_dir,
                "policy.toml",
                "[review]\ncharacters_per_call = 7\ntargets_per_call = 1\n",
            )
        )
        self.assertEqual(policy.characters_per_call, 7)
        self.assertEqual(policy.targets_per_call, 1)
        # Transparent observation of the ACTUAL combined validator
        # (forwarding observer: records the exact argument tuple,
        # calls the real validator with the UNCHANGED arguments and
        # returns its UNCHANGED result - never a fake validator or
        # acceptance result).
        real_validate = composed_validation.validate_composed
        observations: list[tuple[str, tuple, frozenset[int]]] = []

        def observing_validate(original, records, corpus, intervals):
            result = real_validate(original, records, corpus, intervals)
            observations.append((original, tuple(records), result))
            return result

        composed_validation.validate_composed = observing_validate
        self.harness = _Harness(
            _CONFLICT_TEXT,
            self.records,
            self.observed,
            policy=policy,
            corpus_factory=_build_conflict_corpus,
        )
        harness = self.harness
        try:
            harness.reviewer.target_replacements = {
                "wone": "one",
                "wtwo": "two",
            }
            status, _headers, body = _strict_exchange_details(harness, ["1"])
            self.assertEqual(status, 200)
        finally:
            composed_validation.validate_composed = real_validate
        envelope = json.loads(body.decode("utf-8"))
        # The whole conflicting group is reverted: the completed
        # original, unchanged apart from the new root field.
        self.assertEqual(
            {
                key: value
                for key, value in envelope.items()
                if key != _FIELD
            },
            json.loads(harness.source.decode("utf-8")),
        )
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], _CONFLICT_TEXT
        )
        # The real combined validator ran exactly once for this
        # request, on the exact unmodified original, and TWO
        # individually accepted edits entered it.
        self.assertEqual(len(observations), 1)
        original, records, surviving = observations[0]
        self.assertEqual(original, _CONFLICT_TEXT)
        self.assertEqual(len(records), 2)
        for record in records:
            self.assertIsInstance(record, composed_validation.ComposedRecord)
            self.assertEqual(original[record.start:record.end], record.text)
        self.assertEqual(
            {record.text: record.replacement for record in records},
            {"wone": "one", "wtwo": "two"},
            "the entering records must be the two conflicting edits",
        )
        # The REAL combined result rejects the interacting group:
        # no record survives.
        self.assertEqual(surviving, frozenset())
        # Two ACTUAL separate batches (the cross-batch proof): each
        # conflicting site was really dispatched on its own wire
        # request with its own response-local id.
        self.assertEqual(len(harness.reviewer.requests), 2)
        wire_targets = []
        for record in harness.reviewer.requests:
            request_body = json.loads(record["body"].decode("utf-8"))
            targets = harness.reviewer.parse_targets(request_body)
            self.assertEqual(len(targets), 1)
            wire_targets.extend(str(target["id"]) for target in targets)
        self.assertEqual(len(set(wire_targets)), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )
        # The extension reflects the FINAL surviving edit set after
        # composed rollback: complete with EMPTY edits - the two
        # individually accepted proposals never appear as applied
        # edits.
        self.assertEqual(
            envelope.get(_FIELD),
            _complete_details(_CONFLICT_TEXT, 0, 0, []),
        )


class DetailsMappingFaultTests(_DetailsCase):
    """Presentation-fault proof for the unavailable mapping at the
    ACTUAL HTTP boundary (order 041-a, requirement 4/R6): when the
    exact edit-outcome proof cannot be established, the real
    presentation layer degrades the reserved field to the fixed
    small unavailable object - never a fabricated or partial
    mapping - while the real repaired answer, the actual call
    count and usage, the defined logprobs removal and every
    unrelated metadata value stay exactly as the repaired happy
    path delivers them.  The fault is injected at the exact
    take_repair_outcome extraction point (disclosed in the test)
    with the real engine/CPU/Pandoc/reviewer chain otherwise
    untouched; a supplementary direct real-hook boundary case
    follows (the pure builder-fault matrix is the primary unit's
    scope, not repeated here)."""

    def test_http_outcome_loss_degrades_to_unavailable(self) -> None:
        # Real HTTP presentation-fault proof (order 041-a R6): the
        # actual engine/CPU/Pandoc/reviewer chain runs end to end
        # (real detector, real acceptance, real reviewer exchange,
        # real patcher) and the ONLY injected fault is the loss of
        # the final outcome at the exact take_repair_outcome
        # extraction point - the real request-local slot is drained
        # and None returned instead, as if the authoritative final
        # edits had been lost.  DISCLOSED FAULT INJECTION: the
        # instance attribute engine.take_repair_outcome is
        # temporarily shadowed (drain-then-lose) and deleted in
        # finally; no fake repair/acceptance, no runtime edit.  The
        # presentation layer must degrade the reserved field to the
        # fixed small unavailable object while the repaired answer,
        # the actual call count and known usage, the defined
        # logprobs removal and every unrelated metadata value stay
        # exactly as the repaired happy path delivers them.
        answer = _SHORT
        base = json.loads(_json_completed(answer))
        base["output"][0]["content"][0]["logprobs"] = [
            {"token": "synthetic", "logprob": 0.0}
        ]
        base["metadata"] = {"k": "v"}
        payload = json.dumps(
            base, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_payload(harness, payload)
        engine = harness.proxy_server.engine
        real_take = engine.take_repair_outcome

        def faulted_take() -> None:
            # Drain the REAL request-local slot, then lose the
            # outcome (the disclosed single-point fault).
            real_take()
            return None

        engine.take_repair_outcome = faulted_take
        try:
            status, _headers, body = _strict_exchange_details(harness, ["1"])
        finally:
            del engine.take_repair_outcome
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        # The actual repair still happens (real chain, unchanged
        # answer and call count): the repaired answer, one actual
        # call, the validated known usage.
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"],
            _expected_repaired(answer),
        )
        self.assertEqual(len(harness.reviewer.requests), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 1, "reviewer_usage": dict(_U1)}],
        )
        # The reserved field degrades to the fixed small
        # unavailable object - never a fabricated or partial
        # mapping.
        self.assertEqual(
            envelope.get(_FIELD),
            repair_details.unavailable(
                repair_details.EDIT_MAPPING_UNAVAILABLE
            ),
        )
        # Defined logprobs removal still applies at the defined
        # location...
        part = envelope["output"][0]["content"][0]
        self.assertNotIn("logprobs", part)
        # ...and every other unrelated metadata value is preserved
        # exactly: only the new root field differs from the upstream
        # base, whose one expected change is the repaired answer
        # text (plus the defined logprobs removal).
        self.assertEqual(envelope["metadata"], {"k": "v"})
        expected_base = copy.deepcopy(
            {key: value for key, value in base.items() if key != _FIELD}
        )
        expected_base["output"][0]["content"][0]["text"] = (
            _expected_repaired(answer)
        )
        del expected_base["output"][0]["content"][0]["logprobs"]
        self.assertEqual(
            {key: value for key, value in envelope.items() if key != _FIELD},
            expected_base,
        )

    def test_hook_mismatched_outcome_yields_unavailable(self) -> None:
        # Supplementary real-hook boundary proof: the real
        # presentation hook on the real engine's request-local
        # final outcome must degrade to the fixed unavailable
        # object for a non-matching delivered text (the mapping is
        # never fabricated) and prove the exact complete object for
        # the matching call.  The real repair runs against the real
        # detector/acceptance and the owned fake reviewer endpoint.
        answer = _SHORT
        self.harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        engine = harness.proxy_server.engine
        spans = _find_spans(answer, _TARGET)
        self.assertEqual(len(spans), 2)
        delta = len(_REPLACEMENT) - len(_TARGET)
        edits = [
            _expected_edit(s, e, _REPLACEMENT, [delta] * i)
            for i, (s, e) in enumerate(spans)
        ]
        # Fresh request-local slot; one REAL repair run sets the
        # authoritative final outcome (real detector, real reviewer
        # exchange against the owned fake endpoint).
        engine.begin_request()
        repaired = engine.repair(answer)
        self.assertNotEqual(repaired, answer)
        # Mismatched delivered text: the fixed unavailable object.
        self.assertEqual(
            engine.take_repair_details(
                answer, "bogus-delivered", 0, 0
            ),
            repair_details.unavailable(
                repair_details.EDIT_MAPPING_UNAVAILABLE
            ),
        )
        # A fresh slot + real repair run: the matching hook call
        # proves the exact complete object.
        engine.begin_request()
        repaired = engine.repair(answer)
        self.assertEqual(
            engine.take_repair_details(answer, repaired, 0, 0),
            _complete_details(answer, 0, 0, edits),
        )


class DetailsBypassNegativeTests(_DetailsCase):
    """Opted-in NEGATIVE mode at the actual HTTP boundary (order
    041-a, requirement 5/6): a syntactically VALID control header on
    a response that is unsupported, malformed or non-200 is NOT a
    400 (the header value itself is valid) - the existing fallback
    behavior is preserved byte-for-byte, NO repair-details success
    object is added (and no unavailable object either: the
    reserved field is presentation for supported completed plain
    answers only), zero review calls, and the private control
    header is still consumed from the main forwarding."""

    def _assert_main_header_consumed(self, harness: _Harness) -> None:
        self.assertEqual(len(harness.main_requests), 1)
        main_headers = {
            key.lower(): value
            for key, value in harness.main_requests[0]["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)

    def test_opted_in_unsupported_envelope_no_field_no_review(self) -> None:
        # A completed 200 envelope whose message carries an extra
        # content part is NOT a supported plain answer: it passes
        # through byte-identically with zero review and no
        # llm_language_repair anywhere (the field is presentation
        # for supported answers only, never an error marker).
        answer = _SHORT
        base = json.loads(_json_completed(answer))
        base["output"][0]["content"].append(
            {"type": "refusal", "refusal": "synthetic"}
        )
        payload = json.dumps(
            base, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        self.harness = _Harness(
            answer, self.records, self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_payload(harness, payload)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        self.assertEqual(body, payload)
        envelope = json.loads(body.decode("utf-8"))
        self.assertNotIn(_FIELD, envelope)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(
            self.records, [{"review_calls": 0, "reviewer_usage": "unknown"}]
        )
        self._assert_main_header_consumed(harness)

    def test_opted_in_malformed_sse_no_field_no_review(self) -> None:
        # Declared stream:true, opted in, but the upstream SSE body
        # is a single MALFORMED frame (no parseable SSE event at
        # all): the EXISTING strict bounded main-failure contract
        # (order 017-a complete-SSE capture) applies unchanged - a
        # 502 main error - with zero review, no synthetic terminal,
        # no completion invention, no echo of the stream bytes and
        # no llm_language_repair anywhere (a 502 error body carries
        # no presentation extension).
        malformed = b"data: not-json\n\n"
        self.harness = _Harness(
            _SHORT, self.records, self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_sse(harness, malformed)
        status, _headers, body = _strict_exchange_details(
            harness, ["1"], body=_contract_body(True),
        )
        self.assertEqual(status, 502)
        self.assertNotIn(b"not-json", body)
        self.assertNotIn(b"llm_language_repair", body)
        self.assertNotIn(b'"type":"response.completed"', body)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(self.records, [])
        self._assert_main_header_consumed(harness)

    def test_opted_in_upstream_error_status_preserved_no_field(self) -> None:
        # Opted in but the main upstream answers 500: the status
        # and the error body are preserved exactly (existing
        # non-200 semantics), zero review, and NO details object of
        # any kind is added to an error body.
        error_body = b'{"error":{"code":"synthetic"}}'
        self.harness = _Harness(
            _SHORT, self.records, self.observed,
            review_usage=[dict(_U1)],
        )
        harness = self.harness
        _swap_main_status(harness, 500, error_body)
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 500)
        self.assertEqual(body, error_body)
        self.assertNotIn(b"llm_language_repair", body)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(self.records, [])
        self._assert_main_header_consumed(harness)

    def test_opted_in_capture_limit_raw_fallback_no_field_no_review(
        self,
    ) -> None:
        # The existing transparent capture-limit fallback (order
        # 038-a R3, decision D0; the accepted 038 case
        # BoundsAndFallbackTests.test_capture_limit_fallback_raw_in_order_no_repair
        # is this exact bounded pattern) is preserved by the
        # opt-in: a main SSE body OVER the total capture limit is
        # relayed raw and complete in order with NO repair-details
        # object of any kind and ZERO review - all CPU/review work
        # is bypassed, so corpus/replacement acceptance is
        # irrelevant to this negative.  Disclosed test cap:
        # MAIN_BODY_MAX_BYTES is patched to 4096 for this exchange
        # only (production value is 8,000,000, config.py) and the
        # context manager restores the module constant.
        padding = "sintetno-razmislek " * 4000
        payloads = _stream_payloads(_ANSWER, reasoning=padding)
        source = _render_stream(payloads)
        self.assertGreater(len(source), 4096)
        self.sse_harness = _SseDetailsHarness(self.records, self.observed)
        harness = self.sse_harness
        harness.upstream(before=source, after=b"")
        with mock.patch.object(proxy, "MAIN_BODY_MAX_BYTES", 4096):
            status, _headers, body = _post_raw(
                harness.proxy_port,
                "/v1/responses",
                _contract_body_stream(),
                [_DETAILS_HEADER.encode("ascii") + b": 1"],
            )
        self.assertEqual(status, 200)
        # Raw and complete in order: byte-identical relay of the
        # oversized main body, NO proxy field anywhere in it.
        self.assertEqual(body, source)
        self.assertNotIn(b"llm_language_repair", body)
        self.assertEqual(harness.review_calls(), 0)
        self.assertEqual(self.records, [])
        # The private control header is consumed from the main
        # forward (normalized lowercase view: the scripted upstream
        # records on-wire original-case names).
        requests = harness.main_requests()
        self.assertEqual(len(requests), 1)
        main_headers = {
            key.lower(): value for key, value in requests[0]["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)


# ---------------------------------------------------------------------------
# R7d30 legacy-engine interface compatibility (actual HTTP boundary):
# the handler must keep working with a pre-041 engine that implements
# only the old transport-supported interface (identity repair() -> str
# plus the old lifecycle/trace/usage methods) and has NO new 041
# ledger/details methods (take_repair_outcome, take_repair_details).
# ---------------------------------------------------------------------------


class _LegacyEngine:
    """Pre-041 engine interface (R7d30): identity ``repair() -> str``
    with a call counter, plus the OLD required lifecycle/trace/usage
    methods (begin_request, abort_request, take_details,
    mark_usage_observed).  Deliberately MISSING the new 041
    ledger/details methods (take_repair_outcome, take_repair_details).
    The fake engine is intentionally OUTSIDE the named handler
    boundary: the actual proxy handler, guards, transforms and
    delivery are all real; this is an interface-compatibility proof,
    not a CPU proof."""

    def __init__(self) -> None:
        self.repair_calls = 0
        self.calls: list[str] = []

    def repair(self, value: str) -> str:
        self.repair_calls += 1
        self.calls.append("repair")
        return value

    def begin_request(self, scope: object) -> None:
        self.calls.append("begin_request")

    def abort_request(self, observer: object) -> None:
        self.calls.append("abort_request")

    def take_details(self) -> list[dict[str, object]]:
        self.calls.append("take_details")
        return []

    def mark_usage_observed(self) -> None:
        self.calls.append("mark_usage_observed")


class DetailsLegacyEngineCompatTests(_DetailsCase):
    """R7d30: the ACTUAL proxy handlers (named boundary here) with a
    legacy engine that lacks the new 041 ledger/details methods.
    Off: 200 with the exact prior text and normal fields (identity
    legacy repair -> byte-identical completed JSON); on: 200 with the
    same exact text plus the fixed
    unavailable/EDIT_MAPPING_UNAVAILABLE field; exactly one repair
    call, zero review calls, no new-method calls, and the control
    header consumed from the main forward.

    EXPECTED RED ON THE PRE-FIX RUNTIME (58455cc): the strict handler
    calls owner.engine.take_repair_outcome() unconditionally
    (proxy.py:2350, AttributeError -> 502) even when off, and
    owner.engine.take_repair_details when opted in (proxy.py:2335);
    the transparent _transform path calls them unguarded as well
    (proxy.py:2544/2557).  All four cases fail 502 != 200 there;
    they turn green on the primary's interface-compat fix.  Root
    boundary evidence: root-041-first-legacy-failure-boundary.json
    (do_POST line 2350, missing take_repair_outcome, 502)."""

    def _swap_legacy_engine(self) -> _LegacyEngine:
        if self.harness is not None:
            self.harness.proxy_server.engine = _LegacyEngine()
            return self.harness.proxy_server.engine
        assert self.sse_harness is not None
        self.sse_harness.proxy_server.engine = _LegacyEngine()
        return self.sse_harness.proxy_server.engine

    def _assert_legacy_calls(self, engine: _LegacyEngine) -> None:
        # Exactly one repair, zero review, and the proxy never
        # reached the new ledger/details methods (they do not exist
        # on the legacy engine at all).
        self.assertEqual(engine.repair_calls, 1)
        self.assertNotIn("take_repair_outcome", engine.calls)
        self.assertNotIn("take_repair_details", engine.calls)

    def test_strict_off_legacy_engine_exact_prior_behavior(self) -> None:
        # No control header: the completed JSON is delivered
        # byte-identically through the identity legacy repair (the
        # transform short-circuits on an unchanged text), with the
        # old usage observation and no extension.
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        engine = self._swap_legacy_engine()
        status, _headers, body = _strict_exchange_details(harness, [])
        self.assertEqual(status, 200)
        self.assertEqual(body, harness.source)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        self.assertNotIn(_FIELD, envelope)
        self._assert_legacy_calls(engine)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(len(harness.main_requests), 1)
        # Old usage observation through the old take_details (empty).
        self.assertEqual(
            self.records,
            [{"review_calls": 0, "reviewer_usage": "unknown"}],
        )

    def test_strict_on_legacy_engine_unavailable_field(self) -> None:
        # Opted in with a legacy engine: 200, the SAME exact text as
        # the off case, every other field equal to the upstream
        # source, and the reserved root field is exactly the fixed
        # unavailable/EDIT_MAPPING_UNAVAILABLE object (the engine
        # cannot map the final outcome, so the fixed presentation
        # fault is disclosed, not invented).
        answer = _SHORT
        self.harness = _Harness(answer, self.records, self.observed)
        harness = self.harness
        engine = self._swap_legacy_engine()
        status, _headers, body = _strict_exchange_details(harness, ["1"])
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope.get(_FIELD),
            _unavailable("EDIT_MAPPING_UNAVAILABLE"),
        )
        expected = json.loads(harness.source.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        del envelope[_FIELD]
        self.assertEqual(envelope, expected)
        self.assertEqual(body.count(b'"llm_language_repair"'), 1)
        self._assert_legacy_calls(engine)
        self.assertEqual(len(harness.reviewer.requests), 0)
        # The control header is consumed from the main forward.
        self.assertEqual(len(harness.main_requests), 1)
        main_headers = {
            key.lower(): value
            for key, value in harness.main_requests[0]["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)
        self.assertEqual(
            self.records,
            [{"review_calls": 0, "reviewer_usage": "unknown"}],
        )

    def test_transparent_off_legacy_engine_exact_prior_behavior(self) -> None:
        # Transparent handler, no control header: byte-identical
        # completed JSON relay through the identity legacy repair,
        # old usage observation, no extension.
        answer = _SHORT
        payload = _json_completed(answer)
        self.sse_harness = _SseDetailsHarness(self.records, self.observed)
        harness = self.sse_harness
        harness.upstream(
            before=payload, after=b"", content_type="application/json"
        )
        engine = self._swap_legacy_engine()
        status, _headers, body = _post_raw(
            harness.proxy_port, "/v1/responses", _contract_body(False), [],
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, payload)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        self.assertNotIn(_FIELD, envelope)
        self._assert_legacy_calls(engine)
        self.assertEqual(harness.review_calls(), 0)
        self.assertEqual(len(harness.main_requests()), 1)
        self.assertEqual(
            self.records,
            [{"review_calls": 0, "reviewer_usage": "unknown"}],
        )

    def test_transparent_on_legacy_engine_unavailable_field(self) -> None:
        # Transparent handler, opted in: 200, the SAME exact text as
        # the off case, every other field equal to the upstream
        # source, the fixed unavailable/EDIT_MAPPING_UNAVAILABLE
        # field, and the control header consumed from the main
        # forward.
        answer = _SHORT
        payload = _json_completed(answer)
        self.sse_harness = _SseDetailsHarness(self.records, self.observed)
        harness = self.sse_harness
        harness.upstream(
            before=payload, after=b"", content_type="application/json"
        )
        engine = self._swap_legacy_engine()
        status, _headers, body = _post_raw(
            harness.proxy_port,
            "/v1/responses",
            _contract_body(False),
            [_DETAILS_HEADER.encode("ascii") + b": 1"],
        )
        self.assertEqual(status, 200)
        envelope = json.loads(body.decode("utf-8"))
        self.assertEqual(
            envelope.get(_FIELD),
            _unavailable("EDIT_MAPPING_UNAVAILABLE"),
        )
        expected = json.loads(payload.decode("utf-8"))
        self.assertEqual(
            envelope["output"][0]["content"][0]["text"], answer
        )
        del envelope[_FIELD]
        self.assertEqual(envelope, expected)
        self.assertEqual(body.count(b'"llm_language_repair"'), 1)
        self._assert_legacy_calls(engine)
        self.assertEqual(harness.review_calls(), 0)
        requests = harness.main_requests()
        self.assertEqual(len(requests), 1)
        main_headers = {
            key.lower(): value for key, value in requests[0]["headers"].items()
        }
        self.assertNotIn(_DETAILS_HEADER.lower(), main_headers)
        self.assertEqual(
            self.records,
            [{"review_calls": 0, "reviewer_usage": "unknown"}],
        )


if __name__ == "__main__":
    unittest.main()
