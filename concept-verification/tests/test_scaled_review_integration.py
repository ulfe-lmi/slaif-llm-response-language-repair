"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE.

040-a independent scaled-review integration regressions (secondary).

Black-box real-HTTP regressions against the actual runnable chain:
real TCP client -> real ConceptProxy (strict JSON mode, real
admission/intake/setup/capture) -> real RepairEngine (pinned Pandoc
3.7.0.2 via CONCEPT_PANDOC, real read-only tiny SQLite corpus, real
runtime detection, real acceptance, real composed validation) -> real
ResponsesReviewer with its real owned transport child -> fake main
upstream and fake reviewer endpoint, both synthetic loopback servers
OUTSIDE the boundary.  No live model calls: synthetic text only, fake
keys only.

First checkpoint (order 040-a, requirements 1, 2, 4, 8):
  * the default reviewer wire request carries the explicit
    reasoning:{effort:"low"} field while the forwarded main request
    stays untouched;
  * an answer longer than one 1024-codepoint unit with more than four
    eligible occurrences is actually reviewed and repaired at every
    site across >= 2 sequential batches with unique response-local
    occurrence IDs and exact original-occurrence mapping;
  * a clean answer with no eligible suspicion makes zero review calls
    and returns the byte-identical completed original.

EXPECTED_OLD_RUNTIME_FAILURES: on the accepted old runtime (hardcoded
detect maximum 4, one batch, no reasoning effort on the wire) the two
positive cases are EXPECTED TO FAIL at the actual missing
wire/scaled behavior (no reasoning field on the reviewer wire; only
four of six sites reviewed in a single call), which is the ordered
old-baseline evidence - distinct from fixture or import failures.  The
clean baseline control must PASS on the old runtime and keep passing
on the 040 runtime with the same test bytes.
"""

from __future__ import annotations

import contextlib
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
from qwen_client import ResponsesReviewer  # noqa: E402
from tests.test_responses_contract import _json_completed  # noqa: E402
from tests.test_review_batch import (  # noqa: E402
    _build_corpus,
    _contract_body,
    _real_spec,
)

# ---------------------------------------------------------------------------
# Fixtures (synthetic; every detector-suspected word is corpus-unknown,
# every filler word is corpus-known with counts above the legacy
# threshold so it never adds a candidate).
# ---------------------------------------------------------------------------
# Two eligible occurrences, single batch on any epoch.
_SHORT = "To je presenetliv zapisano, to je presenetliv zapisano."
# No eligible suspicion on any epoch: clean baseline control.
_CLEAN = "To je tako zapisano, to je tako zapisano."
_FILLER_UNIT = "tako je tako zapisano"
_REPLACEMENT = "tako"
_TARGET = "presenetliv"
# Default batch context window (order 040-a keeps 240 as the default
# context_codepoints); used to verify the exact original-occurrence
# mapping of every wire target.
_CONTEXT_WINDOW = 240


def _long_answer(occurrences: int = 6) -> str:
    """Deterministic answer with exactly ``occurrences`` eligible
    occurrences and total length EXACTLY 1025 original Unicode code
    points: one code point past the 1024 default unit, so the default
    allowance is B = ceil(1025/1024) = 2 and the default target
    allowance 2 * 4 = 8 >= occurrences.

    The padding is an exact tiling (no loop): the gap between the
    joined target sentences and 1025 must be a whole number of
    ``" " + filler-unit`` steps, and the construction verifies the
    exact length invariant itself, so every assertion on the length
    matches the construction by definition."""
    unit = "To je presenetliv zapisano."
    text = " ".join([unit] * occurrences)
    gap = 1025 - len(text)
    step = 1 + len(_FILLER_UNIT)
    if gap <= 0 or gap % step != 0:
        raise AssertionError(
            "fixture construction cannot reach exactly 1025 code points"
        )
    text += (" " + _FILLER_UNIT) * (gap // step)
    assert len(text) == 1025
    assert 1024 < len(text) <= 2048
    assert text.count(_TARGET) == occurrences
    return text


def _expected_repaired(answer: str) -> str:
    return answer.replace(_TARGET, _REPLACEMENT)


def _target_spans(answer: str) -> list[tuple[int, int]]:
    spans = []
    start = answer.find(_TARGET)
    while start != -1:
        spans.append((start, start + len(_TARGET)))
        start = answer.find(_TARGET, start + 1)
    return spans


# ---------------------------------------------------------------------------
# Raw loopback fake boundaries (OUTSIDE the proxy/reviewer/engine).
# ---------------------------------------------------------------------------
class _LoopbackServer:
    """One finite owned raw-socket server on loopback port 0.

    The owned (non-daemon) server thread handles client connections in
    order by calling the test-provided handler for each client.  Every
    accept iteration is a bounded native wait, and ``close`` actually
    joins the thread - no daemon-and-abandon.
    """

    def __init__(self, name: str, handler: Callable[[socket.socket], None]) -> None:
        self._name = name
        self._handler = handler
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._clients: list[socket.socket] = []

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name=self._name, daemon=False
        )
        self._thread.start()

    def _run(self) -> None:
        self._listener.settimeout(0.25)
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            self._clients.append(client)
            try:
                self._handler(client)
            except OSError:
                pass
            finally:
                with contextlib.suppress(OSError):
                    client.close()

    def close(self) -> bool:
        """Stop, close and actually join the owned server."""
        self._stop.set()
        with contextlib.suppress(OSError):
            self._listener.close()
        for client in list(self._clients):
            with contextlib.suppress(OSError):
                client.close()
        thread = self._thread
        if thread is not None:
            thread.join(5.0)
        return thread is None or not thread.is_alive()


def _read_http_request(sock: socket.socket) -> tuple[str, dict[str, str], bytes]:
    """Read one bounded raw HTTP/1.1 request (synthetic loopback)."""
    sock.settimeout(10.0)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(65536)
        if not chunk:
            raise OSError("peer closed before the request was complete")
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    while len(body) < length:
        chunk = sock.recv(65536)
        if not chunk:
            break
        body += chunk
    return lines[0], headers, body


def _send_http_response(sock: socket.socket, payload: bytes, content_type: str) -> None:
    head = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: " + content_type.encode() + b"\r\n"
        b"Content-Length: " + str(len(payload)).encode() + b"\r\n"
        b"\r\n"
    )
    sock.sendall(head + payload)


def _read_http_response(sock: socket.socket) -> tuple[int, dict[str, str], bytes]:
    sock.settimeout(20.0)
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(65536)
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
    length = int(headers.get("content-length", "0"))
    while len(body) < length:
        chunk = sock.recv(65536)
        if not chunk:
            break
        body += chunk
    return status, headers, body


def _post_to_proxy(sock: socket.socket, body: bytes) -> None:
    sock.sendall(
        b"POST /v1/responses HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"\r\n"
        + body
    )


def _hold_until_close(sock: socket.socket, release: threading.Event) -> bool:
    """Hold one synthetic upstream connection until the peer closes
    it.  True when the peer closed it first (the expected quiet
    close), False when the test released the hold first (teardown)."""
    sock.settimeout(0.25)
    while not release.is_set():
        try:
            chunk = sock.recv(1)
        except TimeoutError:
            continue
        except OSError:
            return True
        if not chunk:
            return True
    return False


def _main_complete_handler(
    requests: list[dict[str, object]],
    payload: bytes,
    released: threading.Event,
    request_event: threading.Event,
    closed: threading.Event,
) -> Callable[[socket.socket], None]:
    """Synthetic main upstream: one completed JSON response, then hold
    until the proxy closes the request-scoped connection."""

    def handle(client: socket.socket) -> None:
        line, headers, body = _read_http_request(client)
        requests.append({"line": line, "headers": headers, "body": body})
        request_event.set()
        _send_http_response(client, payload, "application/json")
        if _hold_until_close(client, released):
            closed.set()

    return handle


# ---------------------------------------------------------------------------
# Dynamic fake reviewer endpoint (OUTSIDE the boundary).
# ---------------------------------------------------------------------------
class _DynamicReviewer:
    """Scripted dynamic reviewer endpoint.

    For EVERY received batch request it echoes one proposal per
    supplied target id (the response-local ids are parsed from the
    actual wire prompt, never assumed), so the same fixture drives the
    old single-batch ``t0..`` ids and any new cross-batch id scheme.
    The envelope of call ``i`` (1-based) carries the test-supplied
    usage for ``i`` (or omits it for the unknown-usage matrix).  Each
    call's arrival is signalled through its own Event (no fixed settle
    sleeps anywhere in this module).
    """

    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []
        self.call_events: list[threading.Event] = []
        self.usage: list[dict[str, int] | None] = []
        self.replacement: str = _REPLACEMENT
        self.released = threading.Event()

    def handler(self) -> Callable[[socket.socket], None]:
        def handle(client: socket.socket) -> None:
            line, headers, body = _read_http_request(client)
            event = threading.Event()
            self.requests.append(
                {"line": line, "headers": headers, "body": body, "event": event}
            )
            index = len(self.requests)
            self.call_events.append(event)
            usage = self.usage[index - 1] if index - 1 < len(self.usage) else None
            payload = self._payload_for(json.loads(body.decode("utf-8")), usage)
            event.set()
            _send_http_response(client, payload, "application/json")
            _hold_until_close(client, self.released)

        return handle

    def _payload_for(
        self, request_body: dict[str, object], usage: dict[str, int] | None
    ) -> bytes:
        targets = self.parse_targets(request_body)
        inner = {
            "version": "batch-v1",
            "proposals": [
                {
                    "id": target["id"],
                    "keep": False,
                    "replacement": self.replacement,
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

    @staticmethod
    def parse_targets(request_body: dict[str, object]) -> list[dict[str, object]]:
        """Parse the batch-v1 target list out of the actual wire prompt.

        The prompt is one instruction line followed by the serialized
        batch-v1 payload (schema batch-v1 keeps this framing); the
        payload is located by its exact compact prefix so the
        instruction text (which itself contains JSON examples) can
        never be parsed as data.
        """
        prompt = str(request_body["input"][0]["content"][0]["text"])
        marker = '{"version":"batch-v1","targets":'
        index = prompt.find(marker)
        if index < 0:
            raise AssertionError("wire prompt carries no batch-v1 payload")
        payload = json.loads(prompt[index:])
        targets = payload.get("targets")
        if not isinstance(targets, list):
            raise AssertionError("batch-v1 payload carries no target list")
        return [dict(item) for item in targets]

    def wait_calls(self, count: int, limit: float = 15.0) -> bool:
        """Finite event wait for ``count`` actually-arrived calls."""
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            if self._calls_done(count):
                return True
            time.sleep(0.01)
        return self._calls_done(count)

    def _calls_done(self, count: int) -> bool:
        return (
            len(self.call_events) >= count
            and all(event.is_set() for event in self.call_events[:count])
        )


# ---------------------------------------------------------------------------
# Harness: real strict proxy + real engine + real reviewer + fakes.
# ---------------------------------------------------------------------------
def _usage_observer(
    records: list[dict[str, object]], observed: threading.Event
) -> Callable[[dict[str, object]], None]:
    def observe(record: dict[str, object]) -> None:
        records.append(record)
        observed.set()

    return observe


class _Harness:
    """One real ConceptProxy (strict JSON mode) instance with the
    answer's completed JSON bound to the fake main upstream, the real
    RepairEngine (pinned Pandoc, tiny real corpus) and the real
    ResponsesReviewer pointed at the dynamic fake reviewer endpoint."""

    def __init__(
        self,
        answer: str,
        records: list[dict[str, object]],
        observed: threading.Event,
        *,
        review_usage: list[dict[str, int] | None] | None = None,
    ) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = _build_corpus(Path(self.temp.name))
        self.source = _json_completed(answer)
        self.reviewer = _DynamicReviewer()
        self.reviewer.usage = review_usage or []
        self.main_requests: list[dict[str, object]] = []
        self.main_released = threading.Event()
        self.main_event = threading.Event()
        self.main_closed = threading.Event()
        self.main_server = _LoopbackServer(
            "main-upstream",
            _main_complete_handler(
                self.main_requests,
                self.source,
                self.main_released,
                self.main_event,
                self.main_closed,
            ),
        )
        self.review_server = _LoopbackServer(
            "review-upstream", self.reviewer.handler()
        )
        self.main_server.start()
        self.review_server.start()
        reviewer = ResponsesReviewer(
            f"http://127.0.0.1:{self.review_server.port}/v1",
            "test-model",
            timeout=8.0,
            api_key="fake-key",
        )
        engine = proxy.RepairEngine(self.db, reviewer, pandoc=_real_spec())
        self.proxy_server = proxy.ConceptProxy(
            ("127.0.0.1", 0),
            f"http://127.0.0.1:{self.main_server.port}",
            engine,
            usage_observer=_usage_observer(records, observed),
        )
        self.threads: list[threading.Thread] = []
        thread = threading.Thread(
            target=self.proxy_server.serve_forever, name="proxy-serve", daemon=False
        )
        thread.start()
        self.threads.append(thread)

    def exchange(self) -> tuple[int, dict[str, str], bytes]:
        """One client POST of the nonstreaming contract body; returns
        the proxy response triple."""
        client = socket.create_connection(
            ("127.0.0.1", self.proxy_server.server_port), timeout=20
        )
        try:
            _post_to_proxy(client, _contract_body(False))
            return _read_http_response(client)
        finally:
            client.close()

    def close(self) -> None:
        self.main_released.set()
        self.reviewer.released.set()
        with contextlib.suppress(Exception):
            self.proxy_server.shutdown()
        with contextlib.suppress(Exception):
            self.proxy_server.server_close()
        for server in (self.main_server, self.review_server):
            with contextlib.suppress(Exception):
                server.close()
        for thread in self.threads:
            thread.join(8.0)
        self.temp.cleanup()


class _ScaledHarnessCase(unittest.TestCase):
    """Shared lifecycle for one real chain exchange per test."""

    def setUp(self) -> None:
        self.records: list[dict[str, object]] = []
        self.observed = threading.Event()
        self.harness: _Harness | None = None

    def tearDown(self) -> None:
        if self.harness is not None:
            self.harness.close()

    def _answer_text(self, body: bytes) -> str:
        envelope = json.loads(body.decode("utf-8"))
        output = envelope["output"]
        content = output[0]["content"]
        return content[0]["text"]


class ScaledReviewFirstCheckpointTests(_ScaledHarnessCase):
    """First checkpoint: low-on-wire default, length-scaled >4 multi-
    batch repair, and the clean zero-call baseline control."""

    def test_default_reviewer_request_carries_explicit_low_effort_on_wire(
        self,
    ) -> None:
        """A1/requirement 4: the default reviewer wire field is exactly
        reasoning:{effort:"low"}, while the forwarded main request is
        untouched (no reviewer reasoning may reach the main request)."""
        harness = _Harness(_SHORT, self.records, self.observed)
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(self._answer_text(body), _expected_repaired(_SHORT))
        self.assertTrue(harness.main_event.wait(15.0))
        # Exactly one eligible occurrence batch on any epoch.
        self.assertTrue(harness.reviewer.wait_calls(1))
        self.assertEqual(len(harness.reviewer.requests), 1)
        record = harness.reviewer.requests[0]
        self.assertEqual(record["line"].split(" ")[1], "/v1/responses")
        request_body = json.loads(record["body"].decode("utf-8"))
        # The old runtime omits the field entirely: this assertion is
        # the actual missing-wire-behavior baseline.
        self.assertEqual(request_body.get("reasoning"), {"effort": "low"})
        # Fixed request contract invariants on every epoch.
        self.assertIs(request_body["stream"], False)
        self.assertIs(request_body["store"], False)
        self.assertEqual(request_body["model"], "test-model")
        # The forwarded main request must not gain reviewer settings.
        self.assertEqual(len(harness.main_requests), 1)
        main_body = json.loads(harness.main_requests[0]["body"].decode("utf-8"))
        self.assertNotIn("reasoning", main_body)

    def test_length_scaled_answer_repairs_more_than_four_sites_across_batches(
        self,
    ) -> None:
        """A1/A2/requirement 2: an answer with 1024 < N <= 2048 code
        points and six eligible occurrences is reviewed at EVERY site
        across two sequential batches (default 4 targets per call),
        with unique response-local ids and exact original-occurrence
        mapping.  The old runtime reviews at most four sites in one
        call: its failure here is the actual missing-scaled-behavior
        baseline."""
        answer = _long_answer()
        spans = _target_spans(answer)
        self.assertEqual(len(spans), 6)
        # The fixture is exactly 1025 code points by construction
        # (see _long_answer): one past the 1024 default unit, so the
        # default allowance is B = 2.
        self.assertGreater(len(answer), 1024)
        self.assertLessEqual(len(answer), 2048)
        harness = _Harness(answer, self.records, self.observed)
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        # Every eligible site actually repaired; no unapproved text.
        repaired = self._answer_text(body)
        # Exact whole-text equality: every eligible site repaired and
        # no other byte changed (the filler already contains
        # replacement words, so per-word counts would not discriminate
        # the edits).
        self.assertEqual(repaired, _expected_repaired(answer))
        self.assertEqual(repaired.count(_TARGET), 0)
        # Two sequential batches for six targets at the default
        # four-per-call: the old runtime makes exactly one.
        self.assertTrue(harness.reviewer.wait_calls(2))
        self.assertEqual(len(harness.reviewer.requests), 2)
        # Per-batch wire contract and exact original-occurrence mapping.
        collected_ids: list[str] = []
        seen_ids: set[str] = set()
        expected_spans = list(spans)
        for batch_index, record in enumerate(harness.reviewer.requests):
            request_body = json.loads(record["body"].decode("utf-8"))
            self.assertIs(request_body["stream"], False)
            self.assertIs(request_body["store"], False)
            self.assertEqual(request_body.get("reasoning"), {"effort": "low"})
            targets = harness.reviewer.parse_targets(request_body)
            if batch_index == 0:
                self.assertEqual(len(targets), 4)
            else:
                self.assertEqual(len(targets), 2)
            batch_ids = [str(target["id"]) for target in targets]
            self.assertEqual(len(set(batch_ids)), len(batch_ids))
            for target in targets:
                target_id = str(target["id"])
                # Globally unique across batches: no id repeats.
                self.assertNotIn(target_id, seen_ids)
                seen_ids.add(target_id)
                collected_ids.append(target_id)
                # Exact original-occurrence mapping: the target and its
                # bounded context slices must equal the original at the
                # next unreviewed occurrence (original order preserved).
                start, end = expected_spans.pop(0)
                self.assertEqual(
                    target["target"], answer[start:end],
                    "target text does not match the exact original slice",
                )
                window = _CONTEXT_WINDOW
                self.assertEqual(
                    target["before_target"], answer[max(0, start - window):start]
                )
                self.assertEqual(
                    target["after_target"], answer[end:min(len(answer), end + window)]
                )
                self.assertIs(target["truncated_before"], start > window)
                self.assertIs(target["truncated_after"], end + window < len(answer))
        self.assertEqual(expected_spans, [])
        self.assertEqual(len(collected_ids), 6)
        # No filler request and no extra dispatch.
        self.assertEqual(len(harness.main_requests), 1)

    def test_clean_answer_makes_zero_review_calls_and_returns_exact_original(
        self,
    ) -> None:
        """Requirement 1 (0 clean): no eligible suspicion means zero
        review calls, never an empty/filler request, and the byte-
        identical completed original is delivered."""
        harness = _Harness(_CLEAN, self.records, self.observed)
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(body, harness.source)
        self.assertTrue(harness.main_event.wait(15.0))
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(self.records, [{"review_calls": 0, "reviewer_usage": "unknown"}])
        # The clean control must also close its owned upstream
        # connection quietly.
        self.assertTrue(harness.main_closed.wait(10.0))


if __name__ == "__main__":
    unittest.main()
