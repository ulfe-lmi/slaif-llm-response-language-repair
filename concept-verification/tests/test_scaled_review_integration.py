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
import os
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
import request_lifecycle  # noqa: E402

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
_FINE_TILE = "tako"
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


def _padded_answer(occurrences: int, target_len: int) -> str:
    """Deterministic answer with exactly ``occurrences`` eligible
    occurrences and EXACTLY ``target_len`` original Unicode code
    points.

    The padding is an exact tiling (no loop): the gap between the
    joined target sentences and ``target_len`` must decompose into
    whole ``" " + _FILLER_UNIT`` steps (22 code points) plus a bounded
    number of whole ``" tako"`` fine-tile steps (5 code points).  The
    construction verifies the exact length invariant itself, so every
    length assertion matches the construction by definition."""
    unit = "To je presenetliv zapisano."
    text = " ".join([unit] * occurrences)
    gap = target_len - len(text)
    step = 1 + len(_FILLER_UNIT)
    fine = 1 + len(_FINE_TILE)
    if gap <= 0:
        raise AssertionError("padding target is below the base length")
    tiles = None
    for b in range(0, step):
        rem = gap - fine * b
        if rem >= 0 and rem % step == 0:
            tiles = (rem // step, b)
            break
    if tiles is None:
        raise AssertionError("fixture cannot reach the exact length")
    a, b = tiles
    text += (" " + _FILLER_UNIT) * a + (" " + _FINE_TILE) * b
    assert len(text) == target_len
    assert text.count(_TARGET) == occurrences
    return text


def _repaired_first_n(answer: str, n: int) -> str:
    """Expected text when exactly the first ``n`` eligible
    occurrences (original order; the detector ranks all occurrences
    of the fixture identically, so ties break by original position)
    are replaced and the rest stay untouched."""
    out = []
    last = 0
    start = answer.find(_TARGET)
    count = 0
    while start != -1 and count < n:
        out.append(answer[last:start])
        out.append(_REPLACEMENT)
        last = start + len(_TARGET)
        count += 1
        start = answer.find(_TARGET, last)
    out.append(answer[last:])
    return "".join(out)


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


class _MainState:
    """Per-exchange state of the synthetic main upstream: the
    completed JSON payload and the request/closed events are swapped
    per exchange so one owned proxy can serve several exchanges.
    ``hold_before_headers`` keeps the connection accepted and the
    request fully read but sends no response byte (main held before
    headers)."""

    def __init__(self, payload: bytes, hold_before_headers: bool = False) -> None:
        self.payload = payload
        self.hold_before_headers = hold_before_headers
        self.released = threading.Event()
        self.request_event = threading.Event()
        self.closed = threading.Event()


def _main_complete_handler(
    requests: list[dict[str, object]], state: _MainState
) -> Callable[[socket.socket], None]:
    """Synthetic main upstream: one completed JSON response per
    exchange, then hold until the proxy closes the request-scoped
    connection."""

    def handle(client: socket.socket) -> None:
        line, headers, body = _read_http_request(client)
        requests.append({"line": line, "headers": headers, "body": body})
        state.request_event.set()
        if state.hold_before_headers:
            if _hold_until_close(client, state.released):
                state.closed.set()
            return
        _send_http_response(client, state.payload, "application/json")
        if _hold_until_close(client, state.released):
            state.closed.set()

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
        # Per-call raw payload overrides for the failure matrix:
        # call i (1-based) sends faults[i-1] verbatim when set.
        self.fault_payloads: list[bytes | None] = []
        # Call i (1-based) whose connection is held without any
        # response: the real owned exchange must be bounded by its
        # own deadline or the request's cancellation.
        self.hold_from_call: int | None = None

    def handler(self) -> Callable[[socket.socket], None]:
        def handle(client: socket.socket) -> None:
            line, headers, body = _read_http_request(client)
            event = threading.Event()
            self.requests.append(
                {"line": line, "headers": headers, "body": body, "event": event}
            )
            index = len(self.requests)
            self.call_events.append(event)
            if (
                self.hold_from_call is not None
                and index == self.hold_from_call
            ):
                # Arrived and held: never answer this call.
                event.set()
                _hold_until_close(client, self.released)
                return
            fault = (
                self.fault_payloads[index - 1]
                if index - 1 < len(self.fault_payloads)
                else None
            )
            if fault is not None:
                payload = fault
            else:
                usage = (
                    self.usage[index - 1]
                    if index - 1 < len(self.usage)
                    else None
                )
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

    def begin_exchange(self) -> None:
        """Reset the per-exchange wire state of this endpoint (the
        server itself stays owned across exchanges of one proxy)."""
        self.requests = []
        self.call_events = []
        self.fault_payloads = []
        self.hold_from_call = None
        self.usage = []

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
        review_timeout: float = 8.0,
        reviewer_override: object | None = None,
        hold_main_before_headers: bool = False,
    ) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = _build_corpus(Path(self.temp.name))
        self.source = _json_completed(answer)
        self.reviewer = _DynamicReviewer()
        self.reviewer.usage = review_usage or []
        self.main_requests: list[dict[str, object]] = []
        self.main_state = _MainState(self.source, hold_main_before_headers)
        self.main_server = _LoopbackServer(
            "main-upstream",
            _main_complete_handler(self.main_requests, self.main_state),
        )
        self.review_server = _LoopbackServer(
            "review-upstream", self.reviewer.handler()
        )
        self.main_server.start()
        self.review_server.start()
        reviewer = (
            reviewer_override
            if reviewer_override is not None
            else ResponsesReviewer(
                f"http://127.0.0.1:{self.review_server.port}/v1",
                "test-model",
                timeout=review_timeout,
                api_key="fake-key",
            )
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

    @property
    def main_event(self) -> threading.Event:
        return self.main_state.request_event

    @property
    def main_closed(self) -> threading.Event:
        return self.main_state.closed

    def set_answer(self, answer: str) -> None:
        """Point the next exchange of this owned proxy at a new
        completed answer (fresh per-exchange events and fake state)."""
        self.source = _json_completed(answer)
        self.main_state = _MainState(self.source)
        self.main_server._handler = _main_complete_handler(
            self.main_requests, self.main_state
        )
        self.reviewer.begin_exchange()

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
        self.main_state.released.set()
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



# ---------------------------------------------------------------------------
# Expanded ordered matrix (order 040-a, requirement 8).
# ---------------------------------------------------------------------------
_BOUNDARY_LENGTHS = (1023, 1024, 1025, 2048, 2049)
_BOUNDARY_OCCURRENCES = 5
# Failure-matrix fixture: nine eligible sites at exactly 2049 code
# points (one past the second 1024 unit: B = 3, three batches 4+4+1).
_FAILURE_OCCURRENCES = 9
_FAILURE_LENGTH = 2049


class CallBudgetBoundaryTests(_ScaledHarnessCase):
    """Default call-budget boundary: B = ceil(N/1024) with five
    eligible occurrences at N = 1023/1024/1025/2048/2049 code points.

    N <= 1024 keeps B = 1 (at most 4 targets: the first four in
    original order are reviewed in one call, the fifth is untouched) -
    this old- and new-runtime-identical behavior is the boundary
    control.  N >= 1025 raises the allowance to at least 8 targets,
    so all five sites are reviewed in two sequential calls (4+1) and
    every site is repaired.  On the old runtime the N >= 1025 cases
    are EXPECTED TO FAIL at the actual missing scaled behavior (four
    of five sites reviewed in one call, one site left unreviewed).
    """

    def test_call_budget_boundaries_at_default_characters_per_call(self) -> None:
        for n in _BOUNDARY_LENGTHS:
            with self.subTest(length=n):
                answer = _padded_answer(_BOUNDARY_OCCURRENCES, n)
                self.assertEqual(len(answer), n)
                self.assertEqual(len(_target_spans(answer)), _BOUNDARY_OCCURRENCES)
                budget = (n + 1023) // 1024  # ceil(n/1024)
                selected = min(_BOUNDARY_OCCURRENCES, budget * 4)
                expected_calls = (selected + 3) // 4
                expected_text = (
                    _expected_repaired(answer)
                    if selected == _BOUNDARY_OCCURRENCES
                    else _repaired_first_n(answer, selected)
                )
                harness = _Harness(answer, self.records, self.observed)
                self.harness = harness
                try:
                    status, _headers, body = harness.exchange()
                    self.assertEqual(status, 200)
                    self.assertEqual(
                        self._answer_text(body),
                        expected_text,
                        "scaled selection/repair mismatch",
                    )
                    self.assertTrue(harness.reviewer.wait_calls(expected_calls))
                    self.assertEqual(
                        len(harness.reviewer.requests),
                        expected_calls,
                        "actual dispatch count must equal the bounded batches",
                    )
                    # Every dispatched batch stays within the four
                    # targets-per-call default.
                    for record in harness.reviewer.requests:
                        request_body = json.loads(record["body"].decode("utf-8"))
                        self.assertLessEqual(
                            len(harness.reviewer.parse_targets(request_body)), 4
                        )
                finally:
                    harness.close()
        self.harness = None


def _incomplete_envelope() -> bytes:
    """A structurally present but incomplete batch envelope: the
    assistant message inside a completed envelope carries the
    contradictory in-progress marker (fixed rejection label)."""
    return (
        b'{"object":"response","status":"completed","output":['
        b'{"type":"message","role":"assistant","status":"in_progress",'
        b'"content":[{"type":"output_text","text":""}]}]}'
    )


class BatchFailureMatrixTests(_ScaledHarnessCase):
    """Any second-batch transport/structural/incomplete failure must
    STOP later batches, discard uncommitted proposals and return the
    completed original answer TEXT (byte-identical here), with the
    actual attempted-call count and unknown usage recorded - never a
    partial repair, never a zeroed attempt, never a later call.  The
    fixture carries nine eligible sites (B = 3, batches 4+4+1) so the
    suppressed third batch genuinely EXISTS after the failed second
    batch.  On the old runtime these cases are EXPECTED TO FAIL at the
    actual missing multi-batch behavior: the second call never exists,
    so the fault never triggers and a four-of-nine partial repair is
    delivered instead of the completed original.
    """

    def _run_failure_case(
        self,
        *,
        review_timeout: float = 8.0,
        fault: bytes | None = None,
        hold_second: bool = False,
    ) -> None:
        # Nine eligible sites with 2048 < N <= 3072 code points: the
        # default allowance is B = 3 (up to 12 targets), so all nine
        # sites are selected into three batches (4+4+1).  A second-
        # batch failure therefore has a GENUINELY AVAILABLE third
        # batch that the runtime must suppress - six sites/B=2 could
        # not prove later-call suppression.
        answer = _padded_answer(_FAILURE_OCCURRENCES, _FAILURE_LENGTH)
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_timeout=review_timeout,
        )
        self.harness = harness
        if fault is not None:
            harness.reviewer.fault_payloads = [None, fault]
        if hold_second:
            # The second call's connection is never answered: the
            # real owned exchange must be bounded by its own
            # configured absolute whole-exchange deadline.
            harness.reviewer.hold_from_call = 2
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(
            body,
            harness.source,
            "batch failure must deliver the completed original text",
        )
        self.assertTrue(harness.reviewer.wait_calls(2))
        # No later batch after the failed one: exactly two actual
        # dispatches (the failed second counts as attempted) and the
        # available third batch was never sent.
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )

    def test_second_batch_invalid_json_returns_original_no_later_calls(self) -> None:
        self._run_failure_case(fault=b"{not json at all")

    def test_second_batch_incomplete_envelope_returns_original_no_later_calls(
        self,
    ) -> None:
        self._run_failure_case(fault=_incomplete_envelope())

    def test_second_batch_timeout_returns_original_no_later_calls(self) -> None:
        # Short configured whole-exchange deadline for this exchange:
        # the held second call fails at its own bounded deadline.
        self._run_failure_case(hold_second=True, review_timeout=2.0)


class UsageAggregationTests(_ScaledHarnessCase):
    """Actual reviewer usage across all attempted calls: aggregated
    ONLY when every attempted result supplies validated usage,
    unknown (never partial-as-total, never zero, sticky across the
    attempted set) otherwise.  Missing or invalid usage is
    bookkeeping ONLY: it must not reject the structurally valid
    proposals of that batch or suppress the CPU-accepted repair
    (existing batch parser contract, order 040-a R6/R7) - the
    completed repaired answer is still delivered.  Fresh requests
    start with cleared request-local state.  On the old runtime the
    two-batch cases are EXPECTED TO FAIL at the actual missing scaled
    behavior (one attempted call, one result's usage).
    """

    _U1 = {"input_tokens": 11, "output_tokens": 17, "total_tokens": 28}
    _U2 = {"input_tokens": 13, "output_tokens": 19, "total_tokens": 32}

    def _summed(self) -> dict[str, int]:
        return {
            "input_tokens": self._U1["input_tokens"] + self._U2["input_tokens"],
            "output_tokens": self._U1["output_tokens"] + self._U2["output_tokens"],
            "total_tokens": self._U1["total_tokens"] + self._U2["total_tokens"],
        }

    def test_two_batches_aggregate_known_usage(self) -> None:
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(self._U1), dict(self._U2)],
        )
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(self._answer_text(body), _expected_repaired(answer))
        self.assertTrue(harness.reviewer.wait_calls(2))
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": self._summed()}],
        )

    def test_second_batch_missing_usage_is_unknown_never_partial(self) -> None:
        # The second batch's envelope omits usage: bookkeeping only.
        # Its valid proposals are still CPU-accepted and ALL six
        # edits are composed - the missing usage may only make the
        # aggregate unknown, never the answer original or partial.
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(self._U1), None],
        )
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(self._answer_text(body), _expected_repaired(answer))
        self.assertTrue(harness.reviewer.wait_calls(2))
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )

    def test_first_batch_missing_usage_is_sticky_unknown(self) -> None:
        # The first batch's envelope omits usage and the second
        # supplies validated usage: the aggregate over the attempted
        # set is still unknown - never the known second batch's
        # usage as a partial-as-total - while the repair is complete.
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[None, dict(self._U2)],
        )
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(self._answer_text(body), _expected_repaired(answer))
        self.assertTrue(harness.reviewer.wait_calls(2))
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )

    def test_fresh_request_resets_request_local_state(self) -> None:
        answer = _long_answer()
        # Request A: six occurrences, two batches, known usage.
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(self._U1), dict(self._U2)],
        )
        self.harness = harness
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(self._answer_text(body), _expected_repaired(answer))
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": self._summed()}],
        )
        # Request B on the SAME owned proxy: clean answer; no earlier
        # request's calls or usage may leak into its record.
        harness.set_answer(_CLEAN)
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(body, harness.source)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(
            self.records,
            [
                {"review_calls": 2, "reviewer_usage": self._summed()},
                {"review_calls": 0, "reviewer_usage": "unknown"},
            ],
        )




# ---------------------------------------------------------------------------
# Cancellation matrix (order 040-a, requirement 6 and 8): before /
# between / during calls with exact attempted-call counts,
# known-unknown usage, owned-child cleanup and no leak into the next
# request.  Real TCP FIN disconnects drive the real request-owned
# cancellation scope; only the narrow timing coordination below is a
# test-owned barrier - it observes real state and never substitutes
# for the product behavior.
# ---------------------------------------------------------------------------
import request_lifecycle  # noqa: E402


def _owned_child_pids() -> set[int]:
    """The pids of the live/zombie children of THIS test process."""
    my_pid = os.getpid()
    pids: set[int] = set()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            stat = Path(f"/proc/{entry}/stat").read_text()
        except OSError:
            continue
        fields = stat.rsplit(")", 1)[1].split()
        if int(fields[1]) == my_pid:
            pids.add(int(entry))
    return pids


def _wait_no_children(limit: float = 10.0) -> set[int]:
    """Bounded wait for the owned review-exchange child to be
    terminated and reaped after an abort."""
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        pids = _owned_child_pids()
        if not pids:
            return set()
        time.sleep(0.02)
    return _owned_child_pids()


class _BarrierSpy:
    """Narrow timing proof, labeled separately from full HTTP
    integration: a transparent forwarding spy over the REAL
    ResponsesReviewer.review_batch.  It never replaces the behavior
    or the result of a call; it only coordinates the test around the
    real first-call completion and the real request scope's observed
    cancellation state.
    """

    def __init__(self, real: ResponsesReviewer) -> None:
        self._real = real
        self.calls = 0
        self.first_completed = threading.Event()
        self.halt_after_first: bool = False

    def review_batch(self, sentence: str, targets: object) -> object:
        self.calls += 1
        result = self._real.review_batch(sentence, targets)
        if self.halt_after_first and self.calls == 1:
            # Return the REAL first-call result only after the REAL
            # request scope carries the observed cancellation, so the
            # engine can never dispatch the next batch before the
            # dedicated cancellation is visible at its boundary.
            self.first_completed.set()
            scope = request_lifecycle.active_scope()
            deadline = time.monotonic() + 20.0
            while scope is None or not scope.cancelled():
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        "real scope never cancelled during the barrier"
                    )
                time.sleep(0.01)
        return result


class CancellationMatrixTests(_ScaledHarnessCase):
    """User cancellation halts later dispatch, kills/reaps the owned
    exchange child, emits exactly one content-free abort observation
    with the actual attempted-call count and validated
    known-or-unknown usage, and leaves no request-local state for the
    next request.  On the old runtime the during-second-call case is
    EXPECTED TO FAIL at the actual missing multi-batch behavior (no
    second dispatch ever happens, so the gate never becomes ready);
    the before/any-call and between-calls cases document the single-
    call invariants the old runtime already satisfies.
    """

    def _fin_exchange(self, harness: _Harness, ready: Callable[[], bool]) -> dict:
        """One exchange whose client performs a real write-half close
        (FIN) once ``ready`` holds.

        ONE authoritative reader thread owns the client socket after
        the POST: it captures EVERY byte the proxy sends on the
        connection until the proxy-side close (EOF).  The test never
        reads the socket itself, so no spurious response byte can be
        consumed before the quiet-abort assertion.  The result dict
        carries the captured stream (must be empty), the observed EOF
        (must be True: the proxy actually closed) and any reader
        error.  On every exit path the socket is closed BEFORE the
        final join so a residual reader cannot strand the thread.
        """
        client = socket.create_connection(
            ("127.0.0.1", harness.proxy_server.server_port), timeout=20
        )
        captured: list[bytes] = []
        state: dict = {"eof": False, "error": None, "reader_done": threading.Event()}

        def job() -> None:
            try:
                _post_to_proxy(client, _contract_body(False))
                # Single authoritative reader: every byte until the
                # proxy-side close; a bounded cap only prevents an
                # unbounded capture - any byte at all fails the test.
                while True:
                    chunk = client.recv(65536)
                    if not chunk:
                        state["eof"] = True
                        break
                    captured.append(chunk)
                    if len(captured) >= 4:
                        break
            except OSError as exc:
                state["error"] = exc
            finally:
                state["reader_done"].set()

        thread = threading.Thread(target=job, daemon=False)
        thread.start()
        joined = False
        try:
            deadline = time.monotonic() + 20.0
            while not ready():
                if time.monotonic() >= deadline:
                    raise AssertionError("FIN gate never became ready")
                time.sleep(0.01)
            client.shutdown(socket.SHUT_WR)
            self.assertTrue(self.observed.wait(20.0), "no abort observation")
            self.assertTrue(
                state["reader_done"].wait(20.0),
                "single authoritative reader did not finish",
            )
        finally:
            # Close before the final join on EVERY path (including
            # gate/assertion failures): unblocks any residual reader,
            # then actually joins the owned client thread.
            with contextlib.suppress(OSError):
                client.close()
            thread.join(15.0)
            joined = not thread.is_alive()
        self.assertTrue(joined, "client reader thread not joined")
        return {
            "bytes": b"".join(captured),
            "eof": state["eof"],
            "error": state["error"],
        }

    def test_cancellation_before_any_call_zero_calls_unknown_usage(self) -> None:
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            hold_main_before_headers=True,
        )
        self.harness = harness
        result = self._fin_exchange(harness, harness.main_event.is_set)
        # Pre-dispatch cancellation: zero attempted calls, explicit
        # unknown usage, no reviewer traffic, quiet close (no byte on
        # the closed client, and the proxy actually closed it).
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(
            self.records, [{"review_calls": 0, "reviewer_usage": "unknown"}]
        )
        self.assertEqual(result["bytes"], b"")
        self.assertIs(result["eof"], True)

    def test_cancellation_between_calls_halts_later_dispatch(self) -> None:
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(UsageAggregationTests._U1), dict(UsageAggregationTests._U2)],
        )
        self.harness = harness
        real_reviewer = ResponsesReviewer(
            f"http://127.0.0.1:{harness.review_server.port}/v1",
            "test-model",
            timeout=8.0,
            api_key="fake-key",
        )
        spy = _BarrierSpy(real_reviewer)
        spy.halt_after_first = True
        # Swap the harness engine's reviewer for the spy over the
        # REAL reviewer BEFORE the exchange: the spy forwards every
        # call to the real one (same transport target, same owned
        # exchange) and only coordinates the barrier around the real
        # first-call result and the real scope state.
        harness.proxy_server.engine.reviewer = spy
        self._fin_exchange(harness, spy.first_completed.is_set)
        # Exactly one actual dispatch: the later batch was halted.
        self.assertEqual(len(harness.reviewer.requests), 1)
        self.assertEqual(spy.calls, 1)
        # One attempted call with validated usage: aggregate is that
        # call's known usage, never unknown and never zero.
        self.assertEqual(
            self.records,
            [
                {
                    "review_calls": 1,
                    "reviewer_usage": dict(UsageAggregationTests._U1),
                }
            ],
        )
        self.assertEqual(_wait_no_children(), set())

    def test_cancellation_during_second_call_counts_two_unknown_usage_no_leak(
        self,
    ) -> None:
        answer = _long_answer()
        harness = _Harness(
            answer,
            self.records,
            self.observed,
            review_usage=[dict(UsageAggregationTests._U1), dict(UsageAggregationTests._U2)],
        )
        self.harness = harness
        # The second call's connection is never answered: the real
        # owned exchange is in flight when the client FINs.
        harness.reviewer.hold_from_call = 2

        def ready() -> bool:
            return (
                len(harness.reviewer.call_events) >= 2
                and harness.reviewer.call_events[1].is_set()
            )

        result = self._fin_exchange(harness, ready)
        # Quiet abort: no spurious byte on the closed client, and the
        # proxy actually closed the connection.
        self.assertEqual(result["bytes"], b"")
        self.assertIs(result["eof"], True)
        # Both calls actually attempted at the owned dispatch
        # boundary; the failed second call makes the aggregate
        # unknown (the first call's known usage is never
        # partial-as-total).
        self.assertEqual(len(harness.reviewer.requests), 2)
        self.assertEqual(
            self.records,
            [{"review_calls": 2, "reviewer_usage": "unknown"}],
        )
        # The owned exchange child was terminated and reaped.
        self.assertEqual(_wait_no_children(), set())
        # No request-local state leaks into the NEXT request on the
        # same owned proxy: a clean answer observes zero calls and
        # explicit unknown usage.
        harness.set_answer(_CLEAN)
        status, _headers, body = harness.exchange()
        self.assertEqual(status, 200)
        self.assertEqual(body, harness.source)
        self.assertEqual(len(harness.reviewer.requests), 0)
        self.assertEqual(
            self.records,
            [
                {"review_calls": 2, "reviewer_usage": "unknown"},
                {"review_calls": 0, "reviewer_usage": "unknown"},
            ],
        )

if __name__ == "__main__":
    unittest.main()
