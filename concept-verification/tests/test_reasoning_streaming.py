"""038-a independent transport regression tests: reasoning streaming.

EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE.

Secondary independent test executor for work order 038-a (stream upstream
reasoning before repaired answer delivery).  Black-box real-HTTP
regression tests against the actual transparent handler: real
ConceptProxy in the normal launcher's transparent forwarding mode
(``passthrough_mode=True``, the CONCEPT_PASSTHROUGH default), real
RepairEngine (installed pinned Pandoc 3.7.0.2 plus tiny real SQLite
corpus), real ResponsesReviewer; only the main upstream and the reviewer
endpoint are synthetic fakes OUTSIDE that boundary (order 038-a,
requirements 7-8).  No live model calls: synthetic text only.

Fixtures use the ACTUAL observed native event order (strategic
qualification 2026-11-15, qwen3.8-27b-rtx3090 through the owner
gateway): response.created, response.in_progress,
output_item.added(reasoning), reasoning_part.added,
reasoning_text.delta (several), reasoning_text.done,
reasoning_part.done, output_item.done(reasoning),
output_item.added(message), content_part.added, output_text.delta,
output_text.done, content_part.done, output_item.done(message),
response.completed.  The documented summary_text family is covered by a
separate variant test.  Field shapes follow the documented Responses
streaming API for those observed event names.

Barrier proof (A1): the fake upstream flushes the validated reasoning
prefix and then blocks on a test-owned Event.  The test releases the
upstream only AFTER the client has already received reasoning bytes, so
client receipt is proved BEFORE upstream completion is released.  While
blocked, no answer delta/completed/answer text may have reached the
client (answer withheld).

Baseline expectation (pre-implementation): every test in
EXPECTED_BASELINE_FAILURES is EXPECTED TO FAIL on the current baseline,
because the merged PR43 transparent handler buffers the whole
potentially repairable SSE body before any emission (reasoning withheld
until completed capture).  Those failures are the ordered "expose
current baseline lack of early reasoning" evidence and are distinct
from fixture/setup bugs: all non-barrier content tests must PASS on the
baseline and must keep passing after implementation.
"""

from __future__ import annotations

from collections.abc import Callable
import contextlib
import http.client
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
from qwen_client import ResponsesReviewer  # noqa: E402
from tests.test_review_batch import (  # noqa: E402
    _FakeUpstream,
    _build_corpus,
    _contract_body,
    _prop,
    _real_spec,
)
from tests.test_responses_contract import _review_payload  # noqa: E402

# ---------------------------------------------------------------------------
# Synthetic fixtures (no live text; all words corpus-known where the
# detector is exercised).
# ---------------------------------------------------------------------------
_ANSWER = "To je presenetliv zapisano, to je presenetliv zapisano."
_EDITED_ANSWER = "To je presenetljiv zapisano, to je presenetljiv zapisano."
# Accepted via the strict possible-typo route (one code-point
# insertion, EXACT positive adjacent count); fixture review
# 038-a: do not rely on the batch-parser default "tako".
_CLEAN = "To je tako zapisano, to je tako zapisano."
_USER_INPUT = "Odgovori krateko v slovenscini."

# Unique reasoning canary: appears only in reasoning frames of a stream.
_REASONING = "razmislek-z-038 preverim obliko zveze."
_REASONING_DEL1 = "razmislek-z-038: "
_REASONING_DEL2 = "preverim obliko zveze."
# Colon-less core so it matches both the single-delta
# _REASONING ("razmislek-z-038 preverim ...") and the
# split-delta _REASONING_DEL1 ("razmislek-z-038: ").
_CANARY = b"razmislek-z-038"

# Continuation canaries for mislabelled / malformed-frame negatives.
_REASONING_2 = "razmislek-z-038 nadalje: preverim zvezo."
_REASONING_3 = "razmislek-z-038 nadalje 2: preverim obliko."

_LIFECYCLE_RESPONSE = {"id": "r_1", "object": "response",
                       "status": "in_progress", "output": []}
_USAGE = {"input_tokens": 3, "output_tokens": 4, "total_tokens": 7}


# ---------------------------------------------------------------------------
# Frame builders: canonical compact-JSON data frames, so zero-review
# streams are byte-identical through the unmodified handler and
# transformed streams differ only at documented answer/logprobs sites.
# ---------------------------------------------------------------------------
def _data(payload: dict) -> bytes:
    return b"data: " + json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _frame(payload: dict, delim: bytes = b"\n\n") -> bytes:
    return _data(payload) + delim


def _created(seq: int = 0) -> dict:
    return {"type": "response.created", "sequence_number": seq,
            "response": dict(_LIFECYCLE_RESPONSE)}


def _in_progress(seq: int = 1, response: dict | None = None) -> dict:
    return {"type": "response.in_progress", "sequence_number": seq,
            "response": response if response is not None
            else dict(_LIFECYCLE_RESPONSE)}


def _reasoning_item_added(seq: int) -> dict:
    return {"type": "response.output_item.added", "sequence_number": seq,
            "output_index": 0,
            "item": {"type": "reasoning", "id": "rs_1",
                     "status": "in_progress", "summary": [],
                     "content": []}}


def _reasoning_part_added(seq: int, part: dict | None = None) -> dict:
    return {"type": "response.reasoning_part.added", "sequence_number": seq,
            "item_id": "rs_1", "output_index": 0,
            "part": part if part is not None
            else {"type": "reasoning_text", "text": ""}}


def _reasoning_delta(seq: int, delta: str) -> dict:
    return {"type": "response.reasoning_text.delta", "sequence_number": seq,
            "item_id": "rs_1", "output_index": 0, "delta": delta}


def _reasoning_done(seq: int, text: str) -> dict:
    return {"type": "response.reasoning_text.done", "sequence_number": seq,
            "item_id": "rs_1", "output_index": 0, "text": text}


def _reasoning_part_done(seq: int, text: str) -> dict:
    return {"type": "response.reasoning_part.done", "sequence_number": seq,
            "item_id": "rs_1", "output_index": 0,
            "part": {"type": "reasoning_text", "text": text}}


def _reasoning_item_done(seq: int, text: str) -> dict:
    return {"type": "response.output_item.done", "sequence_number": seq,
            "output_index": 0,
            "item": {"type": "reasoning", "id": "rs_1", "status": "completed",
                     "summary": [],
                     "content": [{"type": "reasoning_text", "text": text}]}}


def _message_item_added(seq: int) -> dict:
    return {"type": "response.output_item.added", "sequence_number": seq,
            "output_index": 1,
            "item": {"type": "message", "id": "m_1", "status": "in_progress",
                     "role": "assistant", "content": []}}


def _answer_part_added(seq: int) -> dict:
    return {"type": "response.content_part.added", "sequence_number": seq,
            "item_id": "m_1", "output_index": 1, "content_index": 0,
            "part": {"type": "output_text", "text": ""}}


def _answer_delta(seq: int, text: str, **extra: object) -> dict:
    value = {"type": "response.output_text.delta", "sequence_number": seq,
             "item_id": "m_1", "output_index": 1, "content_index": 0,
             "delta": text}
    value.update(extra)
    return value


def _answer_done(seq: int, text: str, **extra: object) -> dict:
    value = {"type": "response.output_text.done", "sequence_number": seq,
             "item_id": "m_1", "output_index": 1, "content_index": 0,
             "text": text}
    value.update(extra)
    return value


def _answer_part_done(seq: int, text: str,
                      part_extra: dict | None = None) -> dict:
    part = {"type": "output_text", "text": text, "content_index": 0}
    if part_extra:
        part.update(part_extra)
    return {"type": "response.content_part.done", "sequence_number": seq,
            "item_id": "m_1", "output_index": 1, "content_index": 0,
            "part": part}


def _message_item_done(seq: int, text: str,
                       part_extra: dict | None = None) -> dict:
    part = {"type": "output_text", "text": text, "content_index": 0}
    if part_extra:
        part.update(part_extra)
    return {"type": "response.output_item.done", "sequence_number": seq,
            "output_index": 1,
            "item": {"type": "message", "id": "m_1", "status": "completed",
                     "role": "assistant", "content": [part]}}


def _completed(seq: int, answer: str, reasoning: str = _REASONING,
               part_extra: dict | None = None,
               response_extra: dict | None = None) -> dict:
    part = {"type": "output_text", "text": answer, "content_index": 0}
    if part_extra:
        part.update(part_extra)
    response = {"id": "r_1", "object": "response", "status": "completed",
                "usage": dict(_USAGE),
                "output": [
                    {"type": "reasoning", "id": "rs_1", "status": "completed",
                     "summary": [],
                     "content": [{"type": "reasoning_text",
                                  "text": reasoning}]},
                    {"type": "message", "id": "m_1", "status": "completed",
                     "role": "assistant", "output_index": 1,
                     "content": [part]},
                ]}
    if response_extra:
        response.update(response_extra)
    return {"type": "response.completed", "sequence_number": seq,
            "response": response}


def _stream_payloads(
    answer: str,
    *,
    reasoning: str = _REASONING,
    split_reasoning: bool = False,
    reasoning_deltas: tuple[str, ...] | None = None,
    part_extra: dict | None = None,
    response_extra: dict | None = None,
    answer_delta_extra: dict | None = None,
    answer_done_extra: dict | None = None,
) -> list[dict]:
    """The actual-observed single-reasoning single-answer stream.

    ``reasoning_deltas`` (when given) splits the reasoning summary into
    several reasoning_text.delta frames; the done/part/item/completed
    copies carry the full ``reasoning`` text (their concatenation).
    """
    if reasoning_deltas is not None:
        deltas = [(4 + index, delta)
                  for index, delta in enumerate(reasoning_deltas)]
        next_seq = 4 + len(reasoning_deltas)
    elif split_reasoning:
        deltas = [(4, _REASONING_DEL1), (5, _REASONING_DEL2)]
        next_seq = 6
    else:
        deltas = [(4, reasoning)]
        next_seq = 5
    payloads: list[dict] = [
        _created(0),
        _in_progress(1),
        _reasoning_item_added(2),
        _reasoning_part_added(3),
    ]
    for delta_seq, delta_text in deltas:
        payloads.append(_reasoning_delta(delta_seq, delta_text))
    payloads += [
        _reasoning_done(next_seq, reasoning),
        _reasoning_part_done(next_seq + 1, reasoning),
        _reasoning_item_done(next_seq + 2, reasoning),
        _message_item_added(next_seq + 3),
        _answer_part_added(next_seq + 4),
        _answer_delta(next_seq + 5, answer, **(answer_delta_extra or {})),
        _answer_done(next_seq + 6, answer, **(answer_done_extra or {})),
        _answer_part_done(next_seq + 7, answer, part_extra),
        _message_item_done(next_seq + 8, answer, part_extra),
        _completed(next_seq + 9, answer, reasoning, part_extra,
                   response_extra),
    ]
    return payloads


def _render_stream(payloads: list[dict], delim: bytes = b"\n\n") -> bytes:
    return b"".join(_frame(payload, delim) for payload in payloads)


def _prefix_cut(split_reasoning: bool, n_deltas: int | None = None) -> int:
    """Index of the first AFTER frame (0-based) in _stream_payloads.

    The early prefix ends with the last reasoning delta: for the
    single-delta stream that is frame index 4, for the two-delta split
    stream index 5.  The barrier holds before any answer event exists
    upstream.  ``n_deltas`` overrides for custom delta counts.
    """
    if n_deltas is not None:
        return 4 + n_deltas
    return 6 if split_reasoning else 5


# ---------------------------------------------------------------------------
# Fake upstream (outside the claimed boundary) with a mid-stream barrier.
# ---------------------------------------------------------------------------
class _QuietThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address) -> None:
        # Connection resets behind barriers/truncation are expected in
        # these tests; keep the suite output clean and deterministic.
        return


class _StreamUpstream(BaseHTTPRequestHandler):
    """Scripted SSE upstream.

    ``before`` bytes are written (optionally in tiny ``chunk`` pieces to
    force arbitrary byte/UTF-8/CRLF split points), then the handler
    signals ``barrier_entered`` and blocks on ``release`` until released
    or ``barrier_timeout``.  ``after`` bytes follow (optionally cut by
    ``truncated_after``) and the connection closes (body EOF).
    ``pre_after_sleep`` simulates a slow upstream between barrier and
    completion.  Content-Length always declares the original full body
    length; with ``truncated_after`` set fewer bytes are actually sent,
    reproducing the actual short-body-with-declared-length transport
    boundary (fixture review 038-a D1).
    """

    protocol_version = "HTTP/1.1"
    before: bytes = b""
    after: bytes = b""
    chunk: int = 65536
    inter_frame_sleep: float = 0.0
    release: threading.Event | None = None
    barrier_timeout: float = 4.0
    pre_after_sleep: float = 0.0
    truncated_after: int | None = None
    send_content_length: bool = True
    status: int = 200
    content_type: str = "text/event-stream"
    error_body: bytes = b"{}"
    recorded: list[dict] = []
    request_received: threading.Event | None = None
    barrier_entered: threading.Event | None = None
    barrier_entered_at: float | None = None

    def do_POST(self) -> None:  # noqa: N802
        cls = self.__class__
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        cls.recorded.append(
            {"path": self.path, "headers": dict(self.headers.items()),
             "body": body})
        if cls.request_received is not None:
            cls.request_received.set()
        if cls.status != 200:
            self.send_response(cls.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(cls.error_body)))
            self.end_headers()
            self.wfile.write(cls.error_body)
            self.close_connection = True
            return
        # Declared-length fidelity (fixture review 038-a D1): the
        # Content-Length always advertises the ORIGINAL full body
        # len(before + after).  When ``truncated_after`` is set the
        # upstream sends FEWER bytes than declared and then closes -
        # the actual short-body-with-declared-length failure boundary,
        # not a framing-complete shorter entity.
        declared = len(cls.before) + len(cls.after)
        self.send_response(200)
        self.send_header("Content-Type", cls.content_type)
        if cls.send_content_length:
            self.send_header("Content-Length", str(declared))
        self.end_headers()
        self._write_all(cls.before)
        if cls.release is not None:
            if cls.barrier_entered is not None:
                cls.barrier_entered_at = time.monotonic()
                cls.barrier_entered.set()
            if not cls.release.wait(cls.barrier_timeout):
                # Test never released: the upstream finishes anyway so no
                # fixture thread can outlive the test.
                pass
        if cls.pre_after_sleep:
            time.sleep(cls.pre_after_sleep)
        tail = (cls.after if cls.truncated_after is None
                else cls.after[: cls.truncated_after])
        self._write_all(tail)
        self.close_connection = True

    def _write_all(self, data: bytes) -> None:
        cls = self.__class__
        for offset in range(0, len(data), cls.chunk):
            self.wfile.write(data[offset: offset + cls.chunk])
            self.wfile.flush()
            if cls.inter_frame_sleep:
                time.sleep(cls.inter_frame_sleep)

    def log_message(self, *_args: object) -> None:
        return


class _ClientCapture:
    """Incremental streaming client state (one connection per exchange).

    ``first_canary_snapshot`` is captured by the client thread at the
    instant the reasoning canary is first observed (the upstream release
    Event is necessarily still unset then).  ``pre_release_snapshot``
    is captured by the test thread immediately BEFORE the release Event
    is set.  Both are immutable pre-release observations: the withholding
    assertions run against them, never against the final ``blob``
    (barrier review 038-a: a correct implementation must fail any
    no-answer assertion made on final bytes).
    """

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.status: int | None = None
        self.error: BaseException | None = None
        self.canary_seen = threading.Event()
        self.canary_seen_at: float | None = None
        self.first_canary_snapshot: bytes = b""
        self.pre_release_snapshot: bytes = b""
        self.finished = threading.Event()

    @property
    def blob(self) -> bytes:
        return b"".join(self.chunks)


def _start_client(port: int, body: bytes, cap: _ClientCapture,
                  canary: bytes = _CANARY) -> threading.Thread:
    def task() -> None:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=25)
            conn.request(
                "POST", "/v1/responses", body=body,
                headers={"Content-Length": str(len(body)),
                         "Content-Type": "application/json",
                         "Authorization": "Bearer main-key-synthetic"})
            response = conn.getresponse()
            cap.status = response.status
            while True:
                piece = response.read1(16384)
                if not piece:
                    break
                cap.chunks.append(piece)
                if not cap.canary_seen.is_set() and canary in cap.blob:
                    cap.canary_seen_at = time.monotonic()
                    cap.first_canary_snapshot = b"".join(cap.chunks)
                    cap.canary_seen.set()
            conn.close()
        except BaseException as exc:  # noqa: BLE001 - recorded, asserted
            cap.error = exc
        finally:
            cap.finished.set()

    thread = threading.Thread(target=task, daemon=True)
    thread.start()
    return thread


def _one_shot(port: int, body: bytes, timeout: float = 25.0
              ) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.request(
        "POST", "/v1/responses", body=body,
        headers={"Content-Length": str(len(body)),
                 "Content-Type": "application/json",
                 "Authorization": "Bearer main-key-synthetic"})
    response = conn.getresponse()
    payload = response.read()
    conn.close()
    return response.status, payload


class _ReasoningHarness:
    """Real transparent proxy + real engine + fake main/review upstreams."""

    def __init__(self, review_payload: bytes = b"{}", *,
                 capture_timeout: float = 15.0,
                 delivery_timeout: float = 15.0) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = _build_corpus(Path(self.temp.name))
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
        _FakeUpstream.payload = review_payload
        _FakeUpstream.status = 200
        _FakeUpstream.delay = 0.0
        _FakeUpstream.recorded = []
        self.upstream_server = _QuietThreadingHTTPServer(
            ("127.0.0.1", 0), _StreamUpstream)
        self.review_server = _QuietThreadingHTTPServer(
            ("127.0.0.1", 0), _FakeUpstream)
        reviewer = ResponsesReviewer(
            f"http://127.0.0.1:{self.review_server.server_port}/v1",
            "test-model", timeout=5.0, api_key="review-key-synthetic")
        engine = proxy.RepairEngine(self.db, reviewer, pandoc=_real_spec())
        self.proxy_server = proxy.ConceptProxy(
            ("127.0.0.1", 0),
            f"http://127.0.0.1:{self.upstream_server.server_port}",
            engine,
            timeout=capture_timeout,
            delivery_timeout=delivery_timeout,
            passthrough_mode=True,
        )
        self.servers = [self.upstream_server, self.review_server,
                        self.proxy_server]
        self.threads: list[threading.Thread] = []
        for server in self.servers:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)

    @property
    def proxy_port(self) -> int:
        return self.proxy_server.server_address[1]

    def upstream(self, *, before: bytes, after: bytes,
                 barrier: bool = False, chunk: int = 65536,
                 inter_frame_sleep: float = 0.0,
                 barrier_timeout: float = 4.0,
                 pre_after_sleep: float = 0.0,
                 truncated_after: int | None = None) -> threading.Event:
        _StreamUpstream.before = before
        _StreamUpstream.after = after
        _StreamUpstream.chunk = chunk
        _StreamUpstream.inter_frame_sleep = inter_frame_sleep
        _StreamUpstream.barrier_timeout = barrier_timeout
        _StreamUpstream.pre_after_sleep = pre_after_sleep
        _StreamUpstream.truncated_after = truncated_after
        _StreamUpstream.send_content_length = True
        _StreamUpstream.status = 200
        _StreamUpstream.content_type = "text/event-stream"
        _StreamUpstream.barrier_entered = threading.Event()
        _StreamUpstream.barrier_entered_at = None
        if barrier:
            _StreamUpstream.release = threading.Event()
            return _StreamUpstream.release
        _StreamUpstream.release = None
        return threading.Event()

    def upstream_500(self, body: bytes = b'{"error":{"code":"synthetic"}}') -> None:
        _StreamUpstream.status = 500
        _StreamUpstream.error_body = body
        _StreamUpstream.before = b""
        _StreamUpstream.after = b""
        _StreamUpstream.release = None

    def review_calls(self) -> int:
        return len(_FakeUpstream.recorded)

    def close(self) -> None:
        release = _StreamUpstream.release
        if release is not None:
            release.set()
        for server in self.servers:
            with contextlib.suppress(Exception):
                server.shutdown()
        for thread in self.threads:
            thread.join(8)
        for server in self.servers:
            with contextlib.suppress(Exception):
                server.server_close()
        self.temp.cleanup()


# ---------------------------------------------------------------------------
# Barrier choreography.
# ---------------------------------------------------------------------------
def _contract_body_stream() -> bytes:
    return _contract_body(True, _USER_INPUT)


def _early_exchange(harness: _ReasoningHarness, body: bytes, cap: _ClientCapture,
                    early_wait: float = 1.5,
                    release_marker: bytes = _CANARY,
                    wait_barrier_entered: bool = False,
                    hold: float = 0.0,
                    pre_release_check: Callable[[], None] | None = None) -> bool:
    """Release the upstream only after the client saw ``release_marker``.

    Returns True when early client receipt was proved (marker observed
    while the upstream was still blocked on its barrier).  The marker
    defaults to the reasoning canary; a test may gate release on its
    intended FINAL prefix marker instead, so the barrier can never be
    released before every frame the pre-release assertions need has
    reached the client (followup review 4: release-marker race).  With
    ``wait_barrier_entered`` the fixture's explicit barrier-entered
    signal is also awaited BEFORE the immutable snapshot, proving the
    fake flushed its whole blocked section.  With ``hold > 0`` the gate
    stays held for a bounded negative observation window first;
    ``pre_release_snapshot`` is then refreshed to the LATEST pre-release
    client view (after the hold, before release) and ``pre_release_check``
    runs while the gate is still held, so both observe the whole negative
    hold interval - never the post-release final output.  The
    pre-release observations are stored on ``cap``
    (``first_canary_snapshot``, ``pre_release_snapshot``) BEFORE the
    release Event is set; the final ``cap.blob`` is only valid after
    this function returns.
    """
    thread = _start_client(harness.proxy_port, body, cap, canary=release_marker)
    early = cap.canary_seen.wait(early_wait)
    if early and wait_barrier_entered:
        if not _StreamUpstream.barrier_entered.wait(early_wait):
            # Fixture liveness failure (fake never reached its barrier):
            # no release, no early-receipt claim.
            early = False
    if early:
        if hold > 0:
            # Bounded negative observation window while the gate is
            # still held; corroborates absence, never a positive proof.
            deadline = time.monotonic() + hold
            while time.monotonic() < deadline and not cap.finished.is_set():
                time.sleep(0.02)
        # Immutable observation while the upstream is still blocked,
        # refreshed AFTER the optional hold so it covers the whole
        # pre-release client view (negative hold interval included).
        cap.pre_release_snapshot = cap.blob
        if pre_release_check is not None:
            pre_release_check()
        release = _StreamUpstream.release
        if release is not None:
            release.set()
    cap.finished.wait(20)
    thread.join(5)
    return early


# ---------------------------------------------------------------------------
# Expected-final derivation: documented wire semantics only (A2 exact
# original-span edits at every answer copy; protected reasoning bytes
# untouched).  Not a reimplementation of the internal transform.
# ---------------------------------------------------------------------------
def _expected_edited(payloads: list[dict], new_answer: str) -> bytes:
    import copy

    expected: list[bytes] = []
    first_delta_done = False
    for payload in payloads:
        value = copy.deepcopy(payload)
        event_type = value.get("type")
        if event_type == "response.output_text.delta":
            if value.get("item_id") == "m_1":
                delta = value.get("delta")
                if type(delta) is str and delta and not first_delta_done:
                    value["delta"] = new_answer
                    first_delta_done = True
        elif event_type == "response.output_text.done":
            if value.get("item_id") == "m_1":
                value["text"] = new_answer
        elif event_type == "response.content_part.done":
            part = value.get("part")
            if (isinstance(part, dict) and part.get("type") == "output_text"
                    and value.get("item_id") == "m_1"):
                part["text"] = new_answer
        elif event_type == "response.output_item.done":
            item = value.get("item")
            if isinstance(item, dict) and item.get("type") == "message" \
                    and item.get("id") == "m_1":
                content = item.get("content")
                if isinstance(content, list) and content:
                    content[0]["text"] = new_answer
        elif event_type == "response.completed":
            response = value.get("response")
            if isinstance(response, dict):
                for item in response.get("output", []):
                    if not isinstance(item, dict) or item.get("type") != "message":
                        continue
                    if item.get("id") != "m_1":
                        continue
                    content = item.get("content")
                    if isinstance(content, list) and content:
                        content[0]["text"] = new_answer
        expected.append(_frame(value))
    return b"".join(expected)


# ---------------------------------------------------------------------------
# A. Barrier / early-receipt / withholding tests (A1).
# ---------------------------------------------------------------------------
class _ReasoningStreamingBase(unittest.TestCase):
    def setUp(self) -> None:
        self.review_payload: bytes = b"{}"
        self.harness = _ReasoningHarness(review_payload=self.review_payload)

    def tearDown(self) -> None:
        self.harness.close()


class EarlyReasoningBarrierTests(_ReasoningStreamingBase):
    """A1: initial reasoning received before main completion, answer
    withheld.  These tests are EXPECTED TO FAIL on the pre-038 baseline
    (whole-body buffering); see EXPECTED_BASELINE_FAILURES."""

    def test_zero_review_reasoning_arrives_while_upstream_blocked(self) -> None:
        # Zero-review control: no detector candidates in the answer, so a
        # correct stream is returned byte-identically with zero review
        # calls, while the reasoning prefix reaches the client before the
        # upstream is released.
        payloads = _stream_payloads(_CLEAN)
        source = _render_stream(payloads)
        cut = _prefix_cut(False) + 1
        # Frame-aligned split: re-render prefix and tail from payloads.
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        release = self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering before "
            "any emission)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        # Answer withheld while upstream is still blocked: no answer
        # delta/done/completed and no answer text may have been emitted.
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.output_text.done", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Zero review: completed answer unchanged, raw bytes end-to-end.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_accepted_edit_reasoning_before_release_answer_withheld(self) -> None:
        # Accepted-edit control: both detector occurrences are accepted
        # (replacement "tako"); the answer is withheld while blocked and
        # the final stream carries the exact repaired answer at every
        # answer copy with reasoning bytes untouched.
        self.review_payload = _review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ])
        self.harness.close()
        self.harness = _ReasoningHarness(
            review_payload=self.review_payload)
        payloads = _stream_payloads(_ANSWER)
        source = _render_stream(payloads)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering before "
            "any emission)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"presenetliv", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        final = cap.blob
        self.assertEqual(final, _expected_edited(payloads, _EDITED_ANSWER))
        self.assertNotIn(b"presenetliv", final)
        # Reasoning prefix exactly once: each reasoning frame carries the
        # canary exactly once across the five reasoning copies.
        self.assertEqual(final.count(_CANARY), 5)
        self.assertEqual(self.harness.review_calls(), 1)

    def test_answer_delta_flushed_still_withheld_before_completion(self) -> None:
        # Completion-gate proof (followup review 4): the fake flushes
        # the complete reasoning prefix AND the message answer delta,
        # then blocks BEFORE the terminal answer copies and completion.
        # A correct runtime buffers from the first message frame, so the
        # reasoning prefix reaches the client while the ALREADY-FLUSHED
        # answer text stays withheld and no review is dispatched while
        # the gate is held.  This exercises the actual completion gate
        # rather than relying on the fake's answer absence (A1/A2
        # block before any answer byte exists upstream).  Zero-review
        # control: A1.
        self.review_payload = _review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ])
        self.harness.close()
        self.harness = _ReasoningHarness(
            review_payload=self.review_payload)
        payloads = _stream_payloads(_ANSWER)
        # Single-delta stream layout: frame 10 is the answer delta
        # (sequence 10); frames 11-14 are the terminal answer copies
        # and completion.
        cut = 11
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        # Fixture liveness: the answer delta is really flushed BEFORE
        # the barrier, and the reasoning canary is in the prefix.
        self.assertIn(_CANARY, before)
        self.assertIn(b"presenetliv", before)
        self.assertNotIn(b"response.completed", before)
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()

        def no_review_while_held() -> None:
            self.assertEqual(
                self.harness.review_calls(), 0,
                "review dispatched while the completion gate was held")

        early = _early_exchange(
            self.harness, _contract_body_stream(), cap,
            wait_barrier_entered=True, hold=0.5,
            pre_release_check=no_review_while_held)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering before "
            "any emission)")
        # The fake flushed the answer delta and is provably blocked.
        self.assertTrue(_StreamUpstream.barrier_entered.is_set())
        # Immutable pre-release view (release Event unset, gate held,
        # refreshed after the 0.5 s negative hold interval): reasoning
        # received, answer withheld although already flushed upstream,
        # no completion, no second status.  The no-review check ran
        # after the hold and before release, while the gate was held.
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"presenetliv", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.output_text.done", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # After release: full capture completed first, then the exact
        # edited stream at every answer copy with reasoning untouched,
        # and exactly one review (the legitimate post-completion one).
        self.assertEqual(cap.blob, _expected_edited(payloads, _EDITED_ANSWER))
        self.assertNotIn(b"presenetliv", cap.blob)
        self.assertEqual(cap.blob.count(_CANARY), 5)
        self.assertEqual(self.harness.review_calls(), 1)

    def test_split_utf8_crlf_frames_early_receipt(self) -> None:
        # Arbitrary byte chunking (3-byte writes) forces UTF-8 code-point
        # and frame-delimiter (CRLF) splits inside the early prefix; the
        # client must still receive the reasoning while the upstream is
        # blocked, and the final bytes must be the source byte-identically
        # (CRLF delimiters preserved).
        del1 = "razmislek-z-038: pre"
        del2 = "verim obliko \u017eve\u0161e."  # zveže. with ž and š
        reasoning = del1 + del2
        payloads = _stream_payloads(
            _CLEAN, reasoning=reasoning, reasoning_deltas=(del1, del2))
        source = _render_stream(payloads, delim=b"\r\n\r\n")
        cut = _prefix_cut(False, n_deltas=2) + 1
        before = _render_stream(payloads[:cut], delim=b"\r\n\r\n")
        after = _render_stream(payloads[cut:], delim=b"\r\n\r\n")
        self.harness.upstream(
            before=before, after=after, barrier=True,
            chunk=3, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning delivery across split UTF-8/CRLF frames "
            "while upstream blocked: expected pre-038 baseline failure "
            "(whole-body SSE buffering before any emission)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# Tests expected to fail on the pre-038 baseline (feature absent), kept
# distinct from fixture/setup failures:
EXPECTED_BASELINE_FAILURES: tuple[str, ...] = (
    # A: early reasoning receipt (the 038-a feature itself)
    "EarlyReasoningBarrierTests.test_zero_review_reasoning_arrives_while_upstream_blocked",
    "EarlyReasoningBarrierTests.test_accepted_edit_reasoning_before_release_answer_withheld",
    "EarlyReasoningBarrierTests.test_split_utf8_crlf_frames_early_receipt",
    # A: completion gate with the answer delta already flushed upstream
    # (followup review 4): fails on baseline at the same early-receipt
    # assertion as the other A tests.
    "EarlyReasoningBarrierTests.test_answer_delta_flushed_still_withheld_before_completion",
    # B: valid early prefix that the baseline withholding also hides
    # (the classification-specific assertions inside these tests are
    # the draft-guard portion; they only become observable once early
    # receipt exists)
    "LabelAndAmbiguityTests.test_duplicate_event_label_not_safe_prefix",
    "LabelAndAmbiguityTests.test_mismatched_event_label_not_safe_prefix",
    "LabelAndAmbiguityTests.test_sse_optional_space_label_variants",
    "LabelAndAmbiguityTests.test_unknown_shape_reasoning_part_not_safe_prefix",
    "LabelAndAmbiguityTests.test_malformed_json_frame_stops_early_forwarding",
    # C5 now proves blocked-upstream early receipt for the DOCUMENTED
    # summary-part family (followup review 4), so it baseline-fails
    # like the other early-receipt tests.  It also pins the documented
    # response.reasoning_summary_part.*/response.reasoning_summary_text.*
    # family in the runtime allowlist: a candidate that only accepts the
    # native qwen family fails this test even after the 038 baseline
    # (independent finding, see secondary-review.json).
    "OrderAndReplayTests.test_summary_text_family_variant",
)


# ---------------------------------------------------------------------------
# B. SSE label/type classification and ambiguous/malformed-frame guards.
# Contract clarification R1: early-prefix safety must consider BOTH the
# SSE event: label and the JSON type; mismatched/duplicate labels and
# ambiguous data cannot be classified as safe reasoning from JSON type
# alone.  Frame-identity replay suppression: original frames and
# boundaries are preserved; no byte-length slicing.
# ---------------------------------------------------------------------------
def _labelled_frame(label_lines: tuple[str, ...], payload: dict,
                    delim: bytes = b"\n\n") -> bytes:
    lines = b"".join(line.encode("utf-8") + b"\n" for line in label_lines)
    return lines + _data(payload) + delim


def _nospace_frame(label_line: str | None, payload: dict,
                   delim: bytes = b"\n\n") -> bytes:
    """Valid SSE with the optional post-colon space omitted everywhere."""
    out = b""
    if label_line is not None:
        out += label_line.encode("utf-8") + b"\n"
    out += (b"data:"
            + json.dumps(payload, ensure_ascii=False,
                         separators=(",", ":")).encode("utf-8")
            + delim)
    return out


def _raw_frame(data_text: str, delim: bytes = b"\n\n") -> bytes:
    return b"data: " + data_text.encode("utf-8") + delim


def _blocked_probe(harness: _ReasoningHarness, body: bytes, cap: _ClientCapture,
                   settle: float = 0.8) -> bytes:
    """Wait until the upstream reached its barrier, snapshot the client
    bytes after a settle delay (while the upstream is still blocked),
    then release and finish.  Returns the blocked snapshot."""
    thread = _start_client(harness.proxy_port, body, cap)
    if not _StreamUpstream.barrier_entered.wait(3.0):
        raise AssertionError("upstream did not reach its barrier")
    time.sleep(settle)
    snapshot = cap.blob
    cap.pre_release_snapshot = snapshot
    release = _StreamUpstream.release
    if release is not None:
        release.set()
    cap.finished.wait(20)
    thread.join(5)
    return snapshot


class LabelAndAmbiguityTests(_ReasoningStreamingBase):
    def test_duplicate_event_label_not_safe_prefix(self) -> None:
        # R1: a duplicate event: label cannot be classified as safe
        # reasoning from JSON type alone; the frame must be buffered (and
        # preserved raw, in order) rather than forwarded early.
        del1, del2 = _REASONING_DEL1, "preverim zvezo nadalje."
        reasoning = del1 + del2
        payloads = _stream_payloads(_CLEAN, reasoning=reasoning,
                                    reasoning_deltas=(del1, del2))
        dup = _labelled_frame(
            ("event: response.reasoning_text.delta",
             "event: response.reasoning_text.delta"), payloads[5])
        frames = [_frame(payload) for payload in payloads]
        frames[5] = dup
        source = b"".join(frames)
        cut = _prefix_cut(False, n_deltas=2) + 1
        self.harness.upstream(
            before=source[: source.index(frames[cut])], after=source[source.index(frames[cut]):],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn("preverim zvezo nadalje.".encode(), snapshot)
        self.assertNotIn(b"event: response.reasoning_text.delta", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Original frame (duplicate labels) preserved raw, once, in order.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_mismatched_event_label_not_safe_prefix(self) -> None:
        # R1: an event: label that disagrees with the JSON type (label
        # claims answer text, payload is reasoning) must not be forwarded
        # early - clients may dispatch by label.
        del1, del2 = _REASONING_DEL1, "preverim zvezo nadalje 3."
        reasoning = del1 + del2
        payloads = _stream_payloads(_CLEAN, reasoning=reasoning,
                                    reasoning_deltas=(del1, del2))
        mismatch = _labelled_frame(
            ("event: response.output_text.delta",), payloads[5])
        frames = [_frame(payload) for payload in payloads]
        frames[5] = mismatch
        source = b"".join(frames)
        cut = _prefix_cut(False, n_deltas=2) + 1
        start = source.index(frames[cut])
        self.harness.upstream(
            before=source[:start], after=source[start:],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        # Release is gated on the first safe canary (delta 1): the
        # second delta frame is itself the unsafe mismatch boundary, so
        # on a correct runtime its text - and the full reasoning text
        # - stays buffered; waiting for it can never succeed (followup
        # review 5: the marker must not span the boundary).
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn("preverim zvezo nadalje 3.".encode(), snapshot)
        self.assertNotIn(b"event: response.output_text.delta", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_mislabelled_frames_preserved_order_and_bytes(self) -> None:
        # Content control (no barrier): both mislabel variants preserved
        # byte-identically and in original order through the actual
        # transparent handler (zero review).
        del1, del2, del3 = (
            _REASONING_DEL1, "preverim zvezo nadalje.",
            "razmislek-z-038 tretji: preverim obliko.")
        reasoning = del1 + del2 + del3
        payloads = _stream_payloads(
            _CLEAN, reasoning=reasoning,
            reasoning_deltas=(del1, del2, del3))
        frames = [_frame(payload) for payload in payloads]
        frames[5] = _labelled_frame(
            ("event: response.reasoning_text.delta",
             "event: response.reasoning_text.delta"), payloads[5])
        frames[6] = _labelled_frame(
            ("event: response.output_text.delta",), payloads[6])
        source = b"".join(frames)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertEqual(body.count(b"event: response.reasoning_text.delta"), 2)
        self.assertEqual(body.count(b"event: response.output_text.delta"), 1)
        self.assertLess(body.index(_CANARY),
                        body.index(b"event: response.reasoning_text.delta"))
        self.assertLess(body.index(b"event: response.reasoning_text.delta"),
                        body.index(b"event: response.output_text.delta"))
        self.assertLess(body.index(b"event: response.output_text.delta"),
                        body.index(b"response.output_text.delta"))
        self.assertEqual(self.harness.review_calls(), 0)

    def test_sse_optional_space_label_variants(self) -> None:
        # SSE consumes exactly ONE optional ASCII space after the field
        # colon; BOTH valid spacing variants (standard "field: value" and
        # compact "field:value") must be classified identically as safe
        # reasoning prefix while the upstream is blocked, and the raw
        # bytes (spacing included) are preserved in the final stream.
        del1, del2 = _REASONING_DEL1, "preverim zvezo razmik."
        reasoning = del1 + del2
        payloads = _stream_payloads(_CLEAN, reasoning=reasoning,
                                    reasoning_deltas=(del1, del2))
        frames = [_frame(payload) for payload in payloads]
        # Frame 4: standard spaced label (label agrees with JSON type).
        frames[4] = _labelled_frame(
            ("event: response.reasoning_text.delta",), payloads[4])
        # Frame 5: compact no-space label and no-space data field.
        frames[5] = _nospace_frame("event:response.reasoning_text.delta",
                                   payloads[5])
        source = b"".join(frames)
        cut = _prefix_cut(False, n_deltas=2) + 1
        start = source.index(frames[cut])
        self.harness.upstream(
            before=source[:start], after=source[start:],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        # Release is gated on the FINAL second-frame marker: del2,
        # contained in the second (no-space) delta frame itself - NOT
        # the concatenated text spanning the two JSON frames (followup
        # review 5).  barrier_entered additionally proves the fake
        # flushed its entire blocked section before the immutable
        # snapshot; no sleep acts as a positive proof.
        early = _early_exchange(
            self.harness, _contract_body_stream(), cap,
            release_marker=del2.encode(),
            wait_barrier_entered=True)
        self.assertTrue(
            early,
            "no early reasoning delivery while upstream blocked: expected "
            "pre-038 baseline failure (whole-body SSE buffering)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertIn("preverim zvezo razmik.".encode(), snapshot,
                      "second (no-space) reasoning frame not delivered early")
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Raw spacing preserved byte-identically in the final stream.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_lifecycle_tool_snapshot_not_safe_prefix(self) -> None:
        # Draft-review reproducer: a lifecycle in_progress snapshot that
        # carries a tool (function_call) output item is NOT a
        # "lifecycle nonterminal event with no answer payload" and must
        # not be forwarded early; everything from it on is buffered.
        tool_response = {"id": "r_1", "object": "response",
                         "status": "in_progress",
                         "output": [{"type": "function_call",
                                     "id": "call_test",
                                     "name": "exec_command",
                                     "arguments": "{}",
                                     "status": "in_progress"}]}
        payloads = _stream_payloads(_CLEAN)
        frames = [_frame(payload) for payload in payloads]
        tool_frame = _frame(_in_progress(1, tool_response))
        frames[1] = tool_frame
        source = b"".join(frames)
        cut = 4  # after the clean reasoning delta (frame index 3)
        start = source.index(frames[cut])
        release = self.harness.upstream(
            before=source[:start], after=source[start:],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        snapshot = _blocked_probe(self.harness, _contract_body_stream(), cap)
        # While blocked: the tool payload must not have been forwarded.
        self.assertNotIn(b"function_call", snapshot)
        self.assertNotIn(b"call_test", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Final stream: tool snapshot preserved raw, in order, no review.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_unknown_shape_reasoning_part_not_safe_prefix(self) -> None:
        # Draft-review reproducer: a reasoning event wrapper whose item/
        # part carries an unknown shape is not blanket-allowed by type
        # spelling; the frame is buffered (and preserved), everything
        # before it (valid prefix) may be early.
        payloads = _stream_payloads(_CLEAN)
        frames = [_frame(payload) for payload in payloads]
        unknown_part = _reasoning_part_added(
            5, part={"type": "unknown_kind", "text": ""})
        unknown_frame = _frame(unknown_part)
        frames.insert(5, unknown_frame)
        source = b"".join(frames)
        # Barrier after the unknown-shape frame (inserted index 5).
        start = source.index(frames[6])
        self.harness.upstream(
            before=source[:start], after=source[start:],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap,
                                early_wait=1.5)
        self.assertTrue(
            early,
            "valid reasoning prefix before the unknown-shape frame was "
            "not delivered early: expected pre-038 baseline failure "
            "(whole-body SSE buffering)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"unknown_kind", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Unknown-shape frame preserved raw, once, in its original place.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_malformed_frames_preserved_no_loss(self) -> None:
        # Content control: a wellformed-but-malformed structure (JSON
        # array) and an object missing "type" must never be silently
        # lost or reordered; the completed stream is delivered whole
        # with zero review.  Malformed JSON is distinguished from
        # wellformed malformed structures (see B8 for the early-cut case).
        payloads = _stream_payloads(_CLEAN)
        frames = [_frame(payload) for payload in payloads]
        array_frame = _raw_frame("[1,2,3]")
        notype_frame = _raw_frame('{"no_type":"synthetic"}')
        insert_at = 6  # after the last reasoning delta, before the answer
        frames.insert(insert_at, array_frame)
        frames.insert(insert_at + 1, notype_frame)
        source = b"".join(frames)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertEqual(body.count(b"[1,2,3]"), 1)
        self.assertEqual(body.count(b'"no_type":"synthetic"'), 1)
        self.assertLess(body.index(b"[1,2,3]"),
                        body.index(b"response.output_text.delta"))
        self.assertLess(body.index(b'"no_type":"synthetic"'),
                        body.index(b"response.output_text.delta"))
        self.assertEqual(self.harness.review_calls(), 0)

    def test_malformed_json_frame_stops_early_forwarding(self) -> None:
        # A frame whose data is not valid JSON cannot be classified as
        # safe reasoning: the valid prefix before it may be early, the
        # malformed frame and everything after are buffered, and the
        # frame is preserved (no loss) in the final stream.
        payloads = _stream_payloads(_CLEAN)
        frames = [_frame(payload) for payload in payloads]
        malformed = _raw_frame("{this is not json")
        frames.insert(5, malformed)
        source = b"".join(frames)
        start = source.index(frames[6])
        self.harness.upstream(
            before=source[:start], after=source[start:],
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "valid reasoning prefix before the malformed frame was not "
            "delivered early: expected pre-038 baseline failure "
            "(whole-body SSE buffering)")
        # Pre-release observation (release Event still unset).
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"{this is not json", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# C. Ordering, exact-once prefix and replay-suppression tests (A2).
# Frame-identity semantics: the forwarded prefix is emitted exactly once
# and the transformed tail agrees with the final answer; suppression of
# prefix replay must use frame identity/ordinal mapping, not
# byte-length slicing (logprobs removal or JSON reserialization can
# change tail length).
# ---------------------------------------------------------------------------
def _expected_final(payloads: list[dict], new_answer: str | None = None,
                    strip_logprobs: bool = False) -> bytes:
    """Documented wire expectation: exact original-span answer edits at
    every answer copy; defined-location logprobs removal (answer sites
    only; protected reasoning and same-named user metadata untouched);
    everything else byte-identical."""
    import copy

    expected: list[bytes] = []
    first_delta_done = False
    for payload in payloads:
        value = copy.deepcopy(payload)
        event_type = value.get("type")
        if event_type == "response.output_text.delta":
            if value.get("item_id") == "m_1":
                if strip_logprobs and "logprobs" in value:
                    del value["logprobs"]
                if new_answer is not None:
                    delta = value.get("delta")
                    if type(delta) is str and delta and not first_delta_done:
                        value["delta"] = new_answer
                        first_delta_done = True
        elif event_type == "response.output_text.done":
            if value.get("item_id") == "m_1":
                if strip_logprobs and "logprobs" in value:
                    del value["logprobs"]
                if new_answer is not None:
                    value["text"] = new_answer
        elif event_type == "response.content_part.done":
            part = value.get("part")
            if (isinstance(part, dict) and part.get("type") == "output_text"
                    and value.get("item_id") == "m_1"):
                if strip_logprobs and "logprobs" in part:
                    del part["logprobs"]
                if new_answer is not None:
                    part["text"] = new_answer
        elif event_type == "response.output_item.done":
            item = value.get("item")
            if (isinstance(item, dict) and item.get("type") == "message"
                    and item.get("id") == "m_1"):
                content = item.get("content")
                if isinstance(content, list) and content:
                    part = content[0]
                    if strip_logprobs and isinstance(part, dict) \
                            and "logprobs" in part:
                        del part["logprobs"]
                    if new_answer is not None and isinstance(part, dict):
                        part["text"] = new_answer
        elif event_type == "response.completed":
            response = value.get("response")
            if isinstance(response, dict):
                for item in response.get("output", []):
                    if not isinstance(item, dict) \
                            or item.get("type") != "message":
                        continue
                    if item.get("id") != "m_1":
                        continue
                    content = item.get("content")
                    if isinstance(content, list) and content:
                        part = content[0]
                        if strip_logprobs and isinstance(part, dict) \
                                and "logprobs" in part:
                            del part["logprobs"]
                        if new_answer is not None and isinstance(part, dict):
                            part["text"] = new_answer
        expected.append(_frame(value))
    return b"".join(expected)


def _later_reasoning_payloads(answer: str) -> list[dict]:
    """A stream where a reasoning_text.delta arrives AFTER the answer
    delta (later reasoning after the answer).  The proxy must preserve
    original event order and never let later reasoning jump ahead of the
    (buffered) answer."""
    del1, del2 = _REASONING_DEL1, "preverim obliko zveze nadalje."
    reasoning = del1 + del2
    return [
        _created(0),
        _in_progress(1),
        _reasoning_item_added(2),
        _reasoning_part_added(3),
        _reasoning_delta(4, del1),
        _message_item_added(5),
        _answer_part_added(6),
        _answer_delta(7, answer),
        _reasoning_delta(8, del2),
        _reasoning_done(9, reasoning),
        _reasoning_part_done(10, reasoning),
        _reasoning_item_done(11, reasoning),
        _answer_done(12, answer),
        _answer_part_done(13, answer),
        _message_item_done(14, answer),
        _completed(15, answer, reasoning),
    ]


class OrderAndReplayTests(_ReasoningStreamingBase):
    def test_reasoning_prefix_exact_once_no_replay(self) -> None:
        # Zero-review control through the actual transparent handler:
        # every frame (reasoning and answer) appears exactly once in the
        # final bytes; no prefix replay, no loss, byte-identical.
        payloads = _stream_payloads(_CLEAN)
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertEqual(body.count(_CANARY), 5)
        for payload in payloads:
            frame = _frame(payload)
            self.assertEqual(body.count(frame), 1,
                             f"frame not exactly once: {frame[:60]!r}")
        self.assertEqual(self.harness.review_calls(), 0)

    def test_later_reasoning_after_answer_preserves_order(self) -> None:
        # Zero-review control: later reasoning after the answer delta
        # keeps its original position (after the answer, before the
        # terminal copies); no reordering, byte-identical.
        payloads = _later_reasoning_payloads(_CLEAN)
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        del1 = _frame(_reasoning_delta(4, _REASONING_DEL1))
        del2 = _frame(_reasoning_delta(8,
                                       "preverim obliko zveze nadalje."))
        self.assertLess(body.index(del1),
                        body.index(b"response.output_text.delta"))
        self.assertLess(body.index(b"response.output_text.delta"),
                        body.index(del2))
        self.assertLess(body.index(del2), body.index(b"response.completed"))
        self.assertEqual(self.harness.review_calls(), 0)

    def test_later_reasoning_after_answer_order_with_edit(self) -> None:
        # Accepted-edit control under the same out-of-band later
        # reasoning: the answer is repaired at every copy and the
        # reasoning order is still preserved in the final stream.
        self.review_payload = _review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ])
        self.harness.close()
        self.harness = _ReasoningHarness(review_payload=self.review_payload)
        payloads = _later_reasoning_payloads(_ANSWER)
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, _expected_final(payloads, _EDITED_ANSWER))
        self.assertNotIn(b"presenetliv", body)
        del1 = _frame(_reasoning_delta(4, _REASONING_DEL1))
        del2 = _frame(_reasoning_delta(8,
                                       "preverim obliko zveze nadalje."))
        self.assertLess(body.index(del1),
                        body.index(b"response.output_text.delta"))
        self.assertLess(body.index(b"response.output_text.delta"),
                        body.index(del2))
        self.assertLess(body.index(del2), body.index(b"response.completed"))
        self.assertEqual(self.harness.review_calls(), 1)

    def test_prefix_replay_immune_to_answer_length_shift(self) -> None:
        # Contract clarification R1: logprobs removal changes the tail
        # length relative to the source, so any prefix replay by
        # byte-length slicing would duplicate or lose frames.  The
        # reasoning prefix must appear exactly once, the answer edit
        # applied at every copy, and defined logprobs stripped only at
        # answer locations.
        self.review_payload = _review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ])
        self.harness.close()
        self.harness = _ReasoningHarness(review_payload=self.review_payload)
        payloads = _stream_payloads(
            _ANSWER,
            answer_delta_extra={"logprobs": []},
            answer_done_extra={"logprobs": [
                {"token": "to", "logprob": -0.5, "start_index": 0,
                 "end_index": 2}]},
            part_extra={"logprobs": None},
            response_extra={"metadata": {"logprobs": [{"token": "meta"}]}})
        # part_extra feeds both the part-done and item-done answer parts;
        # the completed message part carries its own logprobs via the
        # response envelope - emulate by post-editing the payload copy
        # used for source/expectation identically.
        for payload in payloads:
            if payload.get("type") == "response.completed":
                part = payload["response"]["output"][1]["content"][0]
                part["logprobs"] = None
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        expected = _expected_final(payloads, _EDITED_ANSWER,
                                   strip_logprobs=True)
        self.assertEqual(body, expected)
        self.assertNotIn(b"presenetliv", body)
        # Reasoning prefix exactly once despite the shifted tail length.
        self.assertEqual(body.count(_CANARY), 5)
        self.assertEqual(body.count(b'"logprobs"'), 1)
        self.assertIn(b'"metadata":{"logprobs":[{"token":"meta"}]}', body)
        self.assertEqual(self.harness.review_calls(), 1)

    def test_summary_text_family_variant(self) -> None:
        # The DOCUMENTED Responses summary-part family (reasoning summary
        # part events plus reasoning_summary_text delta/done,
        # part.type=summary_text) must stream early exactly like the
        # native reasoning_text family.  This is a BLOCKED-UPSTREAM
        # early-receipt proof: the summary canary must reach the client
        # while the fake is still blocked; a final-only content check is
        # not proof of early summary streaming (followup review 4).  The
        # final bytes remain the byte-identical zero-review control with
        # the final summary item shape retained.
        #
        # Protocol note: the runtime allowlist must cover these
        # documented event names (response.reasoning_summary_part.added/
        # done, response.reasoning_summary_text.delta/done); a candidate
        # that only accepts the native qwen family fails this early
        # receipt (independent finding, secondary-review.json).
        reasoning = "povzetek-z-038 preverim obliko."
        canary = b"povzetek-z-038"
        payloads = [
            _created(0),
            _in_progress(1),
            _reasoning_item_added(2),
            {"type": "response.reasoning_summary_part.added",
             "sequence_number": 3, "item_id": "rs_1", "output_index": 0,
             "summary_index": 0,
             "part": {"type": "summary_text", "text": ""}},
            {"type": "response.reasoning_summary_text.delta",
             "sequence_number": 4, "item_id": "rs_1", "output_index": 0,
             "summary_index": 0, "delta": reasoning},
            {"type": "response.reasoning_summary_text.done",
             "sequence_number": 6, "item_id": "rs_1", "output_index": 0,
             "summary_index": 0, "text": reasoning},
            {"type": "response.reasoning_summary_part.done",
             "sequence_number": 7, "item_id": "rs_1", "output_index": 0,
             "summary_index": 0,
             "part": {"type": "summary_text", "text": reasoning}},
            {"type": "response.output_item.done", "sequence_number": 8,
             "output_index": 0,
             "item": {"type": "reasoning", "id": "rs_1",
                      "status": "completed",
                      "summary": [{"type": "summary_text",
                                   "text": reasoning}],
                      "content": []}},
            _message_item_added(9),
            _answer_part_added(10),
            _answer_delta(11, _CLEAN),
            _answer_done(12, _CLEAN),
            _answer_part_done(13, _CLEAN),
            _message_item_done(14, _CLEAN),
        ]
        # Completed envelope with a summary-family reasoning item
        # (shape used by the repository's own reasoning fixtures).
        completed = _completed(15, _CLEAN, reasoning)
        del completed["response"]["output"][0]["content"]
        completed["response"]["output"][0]["summary"] = [
            {"type": "summary_text", "text": reasoning}]
        payloads.append(completed)
        source = _render_stream(payloads)
        # Blocked section: the whole summary prefix (through the
        # summary_part.done frame, index 7).  Release is gated on the
        # summary canary (first delivered by the delta frame) plus the
        # fake's barrier-entered signal, so the immutable pre-release
        # snapshot covers the entire flushed prefix.
        cut = 8
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(
            self.harness, _contract_body_stream(), cap,
            release_marker=canary, wait_barrier_entered=True)
        self.assertTrue(
            early,
            "no early summary-family reasoning delivery while upstream "
            "blocked: expected pre-038 baseline failure (whole-body SSE "
            "buffering before any emission)")
        # Pre-release observation: summary reasoning arrived early;
        # answer and completion withheld.
        snapshot = cap.pre_release_snapshot
        self.assertIn(canary, snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Final control: byte-identical zero-review delivery; the
        # summary canary appears once per summary copy (5 copies).
        self.assertEqual(cap.blob, source)
        self.assertEqual(cap.blob.count(canary), 5)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# D. Failure after headers: truncation, stalls, explicit failed events,
# upstream error statuses (A3).  After first headers/prefix a later
# upstream failure/truncation/timeout must be observably incomplete:
# no second HTTP response, no successful completed/[DONE] fabrication,
# no review of an incomplete main, no provisional answer release.
# ---------------------------------------------------------------------------
def _assert_incomplete_client_view(status: int, body: bytes) -> None:
    """Implementation-agnostic invariants for an incomplete upstream
    after headers: either a clean 502 before any emission, or a 200
    stream that simply ends without a terminal.  Never a fabricated
    completed/[DONE], never a released answer, never a second status."""
    assert status in (200, 502), f"unexpected status {status}"
    if status == 502:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            raise AssertionError(
                f"502 body is not a JSON error: {body[:120]!r}") from None
        assert isinstance(parsed, dict) and "error" in parsed, (
            f"502 body lacks error object: {body[:120]!r}")
        assert b"razmislek-z-038" not in body
        assert b"presenetliv" not in body
        return
    # 200 with an early-forwarded prefix: the stream must simply end.
    assert b'"type":"response.completed"' not in body
    assert b"[DONE]" not in body
    assert b"presenetliv" not in body
    assert b'"type":"response.output_text.done"' not in body


class FailureAfterHeadersTests(_ReasoningStreamingBase):
    def test_truncated_declared_body_incomplete_no_answer_no_review(self) -> None:
        # True short-body-with-declared-length (fixture review 038-a D1):
        # Content-Length declares the ORIGINAL full body, the upstream
        # sends fewer bytes (cut inside the answer delta frame) and
        # closes.  No completed fabrication, no answer release, no
        # review; either a clean 502 before emission or a 200 stream
        # that ends early.
        payloads = _stream_payloads(_ANSWER)
        source = _render_stream(payloads)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        delta_marker = after.index(b'"type":"response.output_text.delta"')
        truncated_after = delta_marker + 20
        self.harness.upstream(
            before=before, after=after, truncated_after=truncated_after)
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        _assert_incomplete_client_view(status, body)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_upstream_stall_after_prefix_bounded_incomplete(self) -> None:
        # The upstream flushes the reasoning prefix and then stalls past
        # the (short, 2 s) capture deadline.  The request must resolve
        # bounded and incomplete: no hang, no completed fabrication, no
        # answer release, no review.
        self.harness.close()
        self.harness = _ReasoningHarness(capture_timeout=2.0)
        payloads = _stream_payloads(_ANSWER)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=6.0)
        started = time.monotonic()
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream(), timeout=12.0)
        elapsed = time.monotonic() - started
        _assert_incomplete_client_view(status, body)
        self.assertLess(elapsed, 10.0,
                        "stalled upstream was not resolved within a "
                        f"bounded time ({elapsed:.1f}s)")
        self.assertEqual(self.harness.review_calls(), 0)

    def test_explicit_failed_event_faithful(self) -> None:
        # An explicit failed terminal event (the shape observed in the
        # strategic gateway characterization: provider emitted an event
        # unsupported by gateway) must be relayed faithfully: no
        # completed fabrication, no repair, no review.
        failed = {
            "type": "response.failed",
            "sequence_number": 2,
            "response": {"id": "r_1", "object": "response",
                         "status": "failed",
                         "error": {"code": "responses_stream_event_not_supported",
                                   "message": "synthetic upstream event "
                                              "unsupported by gateway"}},
        }
        source = _render_stream([_created(0), _in_progress(1), failed])
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertNotIn(b"response.completed", body)
        self.assertIn(b"response.failed", body)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_upstream_500_status_faithful(self) -> None:
        # An upstream HTTP error status (no 200 headers) is a normal
        # failure before any prefix: faithful status and body passthrough,
        # zero main work beyond the forward, zero review.
        body = b'{"error":{"code":"synthetic_upstream"}}'
        self.harness.upstream_500(body=body)
        status, received = _one_shot(self.harness.proxy_port,
                                     _contract_body_stream())
        self.assertEqual(status, 500)
        self.assertEqual(received, body)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_framing_complete_semantically_incomplete_faithful(self) -> None:
        # Framing-complete but semantically incomplete: correct
        # Content-Length, clean EOF, but NO terminal event (the stream
        # ends after a partial answer delta).  The relay stays faithful:
        # the exact bytes delivered, no completed/[DONE] fabrication, no
        # review of an incomplete main.
        payloads = [
            _created(0),
            _in_progress(1),
            _reasoning_item_added(2),
            _reasoning_part_added(3),
            _reasoning_delta(4, _REASONING),
            _reasoning_done(5, _REASONING),
            _reasoning_part_done(6, _REASONING),
            _reasoning_item_done(7, _REASONING),
            _message_item_added(8),
            _answer_part_added(9),
            _answer_delta(10, "To je presenetliv zapi"),
        ]
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertNotIn(b"response.completed", body)
        self.assertNotIn(b"[DONE]", body)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# E. Logprobs: defined-location removal, defined-only (A5).
# ---------------------------------------------------------------------------
class LogprobsAndMetadataTests(_ReasoningStreamingBase):
    def test_logprobs_defined_stripped_answer_only(self) -> None:
        # Defined Responses logprobs (key present, any value) are removed
        # at the recognized answer locations only, independently of
        # repair (zero-review answer).  Protected reasoning content keeps
        # its same-named key byte-identically; same-named user metadata
        # in the response envelope is never scanned.
        payloads = _stream_payloads(
            _CLEAN,
            answer_delta_extra={"logprobs": []},
            answer_done_extra={"logprobs": [
                {"token": "to", "logprob": -0.5, "start_index": 0,
                 "end_index": 2}]},
            part_extra={"logprobs": None},
            response_extra={"metadata": {"logprobs": [{"token": "meta"}]}})
        # Protected reasoning part: same-named key must survive verbatim.
        for payload in payloads:
            if payload.get("type") == "response.reasoning_part.done":
                payload["part"]["logprobs"] = [{"token": "reason"}]
            if payload.get("type") == "response.completed":
                payload["response"]["output"][1]["content"][0]["logprobs"] = None
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, _expected_final(payloads,
                                               strip_logprobs=True))
        # Only the protected reasoning part and the user metadata keep a
        # logprobs key.
        self.assertEqual(body.count(b'"logprobs"'), 2)
        # The exact reasoning part-done frame (with its logprobs) is
        # present verbatim in the final bytes.
        reasoning_part_payload = [
            p for p in payloads if p.get("type") == "response.reasoning_part.done"
        ][0]
        self.assertIn(_frame(reasoning_part_payload), body)
        self.assertIn(
            b'"metadata":{"logprobs":[{"token":"meta"}]}', body)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# F. Bounds and fallback: total capture limit (A4/A5).
# ---------------------------------------------------------------------------
class BoundsAndFallbackTests(_ReasoningStreamingBase):
    def test_capture_limit_fallback_raw_in_order_no_repair(self) -> None:
        # The existing transparent capture-limit fallback stays coherent:
        # a body over the total capture limit is relayed raw and complete
        # in order, with no repair (the reviewer is never called even
        # though the answer carries detector candidates).
        padding = "sintetno-razmislek " * 4000
        payloads = _stream_payloads(_ANSWER, reasoning=padding)
        source = _render_stream(payloads)
        self.assertGreater(len(source), 4096)
        self.harness.upstream(before=source, after=b"")
        with mock.patch.object(proxy, "MAIN_BODY_MAX_BYTES", 4096):
            status, body = _one_shot(self.harness.proxy_port,
                                     _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertIn("presenetliv".encode(), body)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# G. Cancellation, backpressure and phase budgets (A4).
# ---------------------------------------------------------------------------
def _raw_request_head(body: bytes) -> bytes:
    # Same request shape as the repository's established
    # _post_to_proxy fixture (no Connection: close, so the handle loop
    # covers the request and the later write-half close is the
    # cancellation).
    return (
        b"POST /v1/responses HTTP/1.1\r\n"
        b"Host: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n".encode("ascii")
        + b"\r\n" + body
    )


def _drain_until_eof(sock: socket.socket, limit: float = 10.0) -> tuple[bytes, bool]:
    """Bounded drain after a write-half close.  Returns (bytes, saw_eof).
    The bytes may legitimately include an early-forwarded prefix on a
    correct 038 implementation, so callers must not assert zero bytes."""
    sock.settimeout(0.25)
    data = b""
    deadline = time.monotonic() + limit
    while True:
        try:
            chunk = sock.recv(65536)
        except (TimeoutError, socket.timeout):
            if time.monotonic() >= deadline:
                return data, False
            continue
        except OSError:
            return data, True
        if not chunk:
            return data, True
        data += chunk
        if time.monotonic() >= deadline:
            return data, False


class CancellationAndBackpressureTests(_ReasoningStreamingBase):
    def test_client_disconnect_suppresses_review(self) -> None:
        # A client that disconnects during reasoning interrupts exactly
        # the request-owned main work; no review is dispatched for the
        # (eventually completed) answer, and the proxy stays healthy for
        # the next request.
        self.review_payload = _review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ])
        self.harness.close()
        self.harness = _ReasoningHarness(review_payload=self.review_payload)
        payloads = _stream_payloads(_ANSWER)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        client = socket.create_connection(("127.0.0.1",
                                           self.harness.proxy_port),
                                          timeout=20.0)
        try:
            body = _contract_body_stream()
            client.sendall(_raw_request_head(body))
            # The owned setup write completed: the synthetic main saw the
            # whole request BEFORE the client write-half-closes.
            self.assertTrue(_StreamUpstream.request_received.wait(15.0),
                            "main upstream never received the request")
            time.sleep(0.3)
            client.shutdown(socket.SHUT_WR)  # FIN
            # Give the disconnect watcher and the guard bounded time to
            # act; the answer has not completed yet (upstream still
            # blocked on its barrier), so no review may be dispatched.
            time.sleep(1.5)
            self.assertEqual(self.harness.review_calls(), 0,
                             "review dispatched after client disconnect")
            release = _StreamUpstream.release
            if release is not None:
                release.set()
            time.sleep(0.5)
            self.assertEqual(self.harness.review_calls(), 0)
            self.assertEqual(len(_StreamUpstream.recorded), 1)
            drained, saw_eof = _drain_until_eof(client)
            self.assertTrue(saw_eof,
                            "proxy did not close the cancelled connection")
        finally:
            client.close()
        # Proxy still healthy: a following clean zero-review request
        # round-trips byte-identically with zero review.
        clean = _stream_payloads(_CLEAN)
        clean_source = _render_stream(clean)
        self.harness.upstream(before=clean_source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, clean_source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_slow_upstream_not_charged_to_delivery_budget(self) -> None:
        # Phase budgets stay independent: upstream generation slower than
        # the (2 s) final-delivery budget must not consume it; the
        # delivery deadline anchors AFTER capture, so a normally reading
        # client still receives the complete stream.
        self.harness.close()
        self.harness = _ReasoningHarness(capture_timeout=12.0,
                                         delivery_timeout=2.0)
        payloads = _stream_payloads(_CLEAN)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, pre_after_sleep=4.0)
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream(), timeout=20.0)
        self.assertEqual(status, 200)
        self.assertEqual(body, _render_stream(payloads))
        self.assertEqual(self.harness.review_calls(), 0)

    def test_stalled_client_bounded_no_completed(self) -> None:
        # A stalled client (never reads) fills the socket buffer; the
        # bounded delivery deadline must close the connection quietly:
        # the partial body is never labelled completed, no second
        # response/status is emitted, and the request resolves in a
        # bounded wall time.
        self.harness.close()
        self.harness = _ReasoningHarness(capture_timeout=15.0,
                                         delivery_timeout=2.0)
        padding = "sintetno-razmislek " * 60000
        payloads = _stream_payloads(_CLEAN, reasoning=padding)
        source = _render_stream(payloads)
        self.assertGreater(len(source), 1_000_000)
        self.harness.upstream(before=source, after=b"")
        request = _contract_body_stream()
        started = time.monotonic()
        sock = socket.create_connection(("127.0.0.1",
                                         self.harness.proxy_port), timeout=10)
        try:
            sock.sendall(_raw_request_head(request))
            # Truly stall: sleep PAST the 2 s delivery budget without
            # reading, so the proxy's final-delivery write blocks on a
            # full socket buffer and the bounded deadline must fire.
            time.sleep(2.5)
            collected, saw_eof = _drain_until_eof(sock, limit=10.0)
        finally:
            sock.close()
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 12.0,
                        f"stalled client did not resolve bounded ({elapsed:.1f}s)")
        self.assertTrue(saw_eof, "connection was not closed")
        self.assertEqual(collected.count(b"HTTP/1.0"), 1,
                         "a second status line was emitted")
        head, _, body = collected.partition(b"\r\n\r\n")
        self.assertLess(len(body), len(source),
                        f"stalled client received the full body "
                        f"({len(body)} of {len(source)}) despite stalling")
        self.assertNotIn(b"response.completed", body)
        self.assertNotIn(b"[DONE]", body)
        self.assertNotIn(b"response.completed", head)


# ---------------------------------------------------------------------------
# H. PR43 passthrough regression guards (A5).
# ---------------------------------------------------------------------------
class PassthroughRegressionTests(_ReasoningStreamingBase):
    def test_nonrepairable_stream_request_streams_raw_early(self) -> None:
        # A Responses request that is NOT a repair candidate (declared
        # non-text response_format) must keep PR43 streaming passthrough:
        # bytes stream as the upstream sends them (reasoning reaches the
        # client while the upstream is still blocked), the stream ends
        # raw and byte-identical, and no review is ever dispatched.
        non_candidate = json.dumps(
            {"model": "m", "input": _USER_INPUT, "store": False,
             "stream": True,
             "response_format": {"type": "json_object"}},
            separators=(",", ":")).encode()
        payloads = _stream_payloads(_ANSWER)
        source = _render_stream(payloads)
        cut = _prefix_cut(False) + 1
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _early_exchange(self.harness, non_candidate, cap)
        self.assertTrue(
            early,
            "non-repairable streaming request did not stream early: "
            "PR43 passthrough regression")
        snapshot = cap.pre_release_snapshot
        self.assertIn(_CANARY, snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Raw, complete, in order, untouched (no repair, no review).
        self.assertEqual(cap.blob, source)
        self.assertIn("presenetliv".encode(), cap.blob)
        self.assertEqual(self.harness.review_calls(), 0)
