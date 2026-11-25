"""039-a independent transport regression tests: minimal lifecycle
snapshots.

EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE.

Secondary independent test executor for work order 039-a (stream
reasoning after minimal lifecycle snapshots).  Black-box real-HTTP
regression tests against the actual transparent handler: real
ConceptProxy in the normal launcher's transparent forwarding mode
(``passthrough_mode=True``), real RepairEngine (installed pinned
Pandoc 3.7.0.2 plus tiny real SQLite corpus), real ResponsesReviewer;
only the main upstream and the reviewer endpoint are synthetic fakes
OUTSIDE that boundary.  No live model calls: synthetic text only.

The observed V100/llama.cpp backend (structural evidence, not a
provider branch) emits response.created and response.in_progress
whose response carries EXACTLY id/object/status with status
in_progress and NO output field.  Order 039-a requirement 1 proves
that exact shape as a minimal nonterminal lifecycle variant for the
direct lifecycle classification branch only.

Gated exchanges are event-based: the producer's barrier-entered
event proves the fake flushed its whole blocked section and is
holding; the client marker event proves the safe prefix actually
reached the client (stream order: every byte before the marker is
already in the client buffer when the marker event fires); the
forbidden canary is tracked as its own client event while completion
is held.  The immutable pre-release snapshot is taken and checked
BEFORE the release Event is set.  No fixed settle sleep acts as a
proof, and every exchange releases the barrier, waits for client
finished and joins the client thread in a finally block (asserted).

EXPECTED_OLD_RUNTIME_FAILURES: the positive cases are EXPECTED TO
FAIL on the old runtime (pre-039, accepted 6f7ae1a code) at the
early-receipt assertion - the old classifier rejects the first
minimal frame as "lifecycle snapshot unproven" (output absent) and
the one-way boundary latches at frame zero, so no reasoning reaches
the client while completion is held.  Those failures are the ordered
old-runtime evidence, distinct from fixture/setup failure; every
other test must PASS on the old runtime and keep passing on the 039
runtime with the same test bytes.
"""

from __future__ import annotations

import http.client
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
import sse_streaming  # noqa: E402
import tests.test_reasoning_streaming as _trs  # noqa: E402
from tests.test_reasoning_streaming import (  # noqa: E402
    _ANSWER,
    _CLEAN,
    _ClientCapture,
    _EDITED_ANSWER,
    _StreamUpstream,
    _ReasoningHarness,
    _answer_delta,
    _answer_done,
    _answer_part_added,
    _answer_part_done,
    _completed,
    _contract_body_stream,
    _created,
    _expected_edited,
    _frame,
    _in_progress,
    _labelled_frame,
    _message_item_added,
    _message_item_done,
    _one_shot,
    _reasoning_delta,
    _reasoning_done,
    _reasoning_item_added,
    _reasoning_item_done,
    _reasoning_part_added,
    _reasoning_part_done,
    _render_stream,
    _start_client,
)
from tests.test_review_batch import _prop  # noqa: E402
from tests.test_responses_contract import _review_payload  # noqa: E402

# ---------------------------------------------------------------------------
# 039 minimal lifecycle fixtures (order 039-a requirement 1 shape).
# ---------------------------------------------------------------------------
_REASONING_039 = "razmislek-z-039 preverim obliko zveze."
_CANARY_039 = b"razmislek-z-039"
# Split-delta pieces for multi-delta streams: the first piece carries
# the safe canary, the last piece carries a DISTINCT later marker so
# later-reasoning latch proofs never span frames.
_DEL1_039 = "razmislek-z-039: "
_DEL2_039 = "preverim zvezo."
_DEL3_039 = "razmislek nadalje-3-z-039 preverim obliko."
_LATER_MARKER_039 = b"nadalje-3-z-039"


def _minimal_response(id_: str = "r_1") -> dict:
    return {"id": id_, "object": "response", "status": "in_progress"}


def _minimal_created(seq: int = 0, response: dict | None = None,
                     **event_extra: object) -> dict:
    value = {"type": "response.created", "sequence_number": seq,
             "response": (response if response is not None
                          else _minimal_response())}
    value.update(event_extra)
    return value


def _minimal_in_progress(seq: int = 1, response: dict | None = None,
                         **event_extra: object) -> dict:
    value = {"type": "response.in_progress", "sequence_number": seq,
             "response": (response if response is not None
                          else _minimal_response())}
    value.update(event_extra)
    return value


def _minimal_stream_payloads(answer: str, *,
                             reasoning: str = _REASONING_039) -> list[dict]:
    """Observed minimal start frames (exact requirement-1 shape: event
    keys exactly type/response/sequence_number; response keys exactly
    id/object/status; status exactly in_progress; output ABSENT)
    followed by the observed-order single-reasoning single-answer tail
    (same layout as the 038 fixture stream).  Returns JSON payloads;
    frame rendering (with or without an agreeing event label) happens
    at the call site."""
    return [
        _minimal_created(0),
        _minimal_in_progress(1),
        _reasoning_item_added(2),
        _reasoning_part_added(3),
        _reasoning_delta(4, reasoning),
        _reasoning_done(5, reasoning),
        _reasoning_part_done(6, reasoning),
        _reasoning_item_done(7, reasoning),
        _message_item_added(8),
        _answer_part_added(9),
        _answer_delta(10, answer),
        _answer_done(11, answer),
        _answer_part_done(12, answer),
        _message_item_done(13, answer),
        _completed(14, answer, reasoning),
    ]


def _lookalike_stream_payloads(lookalike: dict) -> list[dict]:
    """Negative template: a legacy FULL-snapshot created (safe on both
    epochs) followed by the lookalike at the direct in_progress
    position, then the observed reasoning/answer tail.  The boundary
    must latch at the lookalike on both epochs; the safe prefix before
    it is exactly frame 0."""
    return [
        _created(0),
        lookalike,
        _reasoning_item_added(2),
        _reasoning_part_added(3),
        _reasoning_delta(4, _REASONING_039),
        _reasoning_done(5, _REASONING_039),
        _reasoning_part_done(6, _REASONING_039),
        _reasoning_item_done(7, _REASONING_039),
        _message_item_added(8),
        _answer_part_added(9),
        _answer_delta(10, _CLEAN),
        _answer_done(11, _CLEAN),
        _answer_part_done(12, _CLEAN),
        _message_item_done(13, _CLEAN),
        _completed(14, _CLEAN, _REASONING_039),
    ]


# Unsafe minimal lookalikes: each is structurally close to the proven
# requirement-1 shape but violates at least one exact condition (extra
# keys, wrong types, missing keys, terminal/queued/unknown status,
# bool/string/negative sequence, non-direct event type, answer
# carrier).  All must stay conservative boundaries.
_LOOKALIKE_VARIANTS: tuple[tuple[str, dict], ...] = (
    ("extra_event_key", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": _minimal_response("r_l01"),
        "metadata": {"tag": "l01"}}),
    ("extra_response_key", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l02", "object": "response",
                     "status": "in_progress",
                     "usage": {"input_tokens": 1, "output_tokens": 2,
                               "total_tokens": 3}}}),
    ("output_null", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l03", "object": "response",
                     "status": "in_progress", "output": None}}),
    ("answer_output", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l04", "object": "response",
                     "status": "in_progress",
                     "output": [{"type": "message", "id": "m_1",
                                 "status": "in_progress",
                                 "role": "assistant",
                                 "content": [{"type": "output_text",
                                              "text": _CLEAN}]}]}}),
    ("wrong_object", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l05", "object": "chat.completion",
                     "status": "in_progress"}}),
    ("empty_id", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "", "object": "response",
                     "status": "in_progress"}}),
    ("id_not_string", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": 123, "object": "response",
                     "status": "in_progress"}}),
    ("id_missing", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"object": "response", "status": "in_progress"}}),
    ("object_missing", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l08", "status": "in_progress"}}),
    ("status_missing", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l09", "object": "response"}}),
    ("terminal_status", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l10", "object": "response",
                     "status": "completed"}}),
    ("queued_status", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l11", "object": "response",
                     "status": "queued"}}),
    ("unknown_status", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": {"id": "r_l12", "object": "response",
                     "status": "finished"}}),
    ("response_not_object", {
        "type": "response.in_progress", "sequence_number": 1,
        "response": "in_progress"}),
    ("seq_bool", {
        "type": "response.in_progress", "sequence_number": True,
        "response": _minimal_response("r_l13")}),
    ("seq_string", {
        "type": "response.in_progress", "sequence_number": "1",
        "response": _minimal_response("r_l14")}),
    ("seq_negative", {
        "type": "response.in_progress", "sequence_number": -1,
        "response": _minimal_response("r_l15")}),
    ("queued_event_type", {
        "type": "response.queued", "sequence_number": 1,
        "response": _minimal_response("r_l16")}),
)


# ---------------------------------------------------------------------------
# Gated exchanges: event-based proofs with guaranteed finally cleanup
# (barrier release, client finished-wait and thread join on every
# path, asserted).  No fixed settle sleep acts as a proof.
# ---------------------------------------------------------------------------
class _DualCapture(_ClientCapture):
    """Client state that additionally tracks FORBIDDEN markers as a
    client-thread event (first forbidden-byte observation) and records
    first-arrival byte offsets in stream order (the client thread is
    the single reader).  ``canary_seen`` doubles as the SAFE marker
    event (passed as ``canary`` to ``_start_dual_client``)."""

    def __init__(self) -> None:
        super().__init__()
        self.forbidden_seen = threading.Event()
        self.forbidden_offset: int | None = None
        self.marker_offset: int | None = None


def _start_dual_client(port: int, body: bytes, cap: _DualCapture,
                       safe_marker: bytes,
                       forbidden_markers: tuple[bytes, ...]) -> threading.Thread:
    """Streaming client that fires ``canary_seen`` when the complete
    SAFE marker bytes are in the buffer and ``forbidden_seen`` on the
    first forbidden marker bytes (single reader, stream order)."""

    def task() -> None:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port,
                                              timeout=25)
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
                blob = cap.blob
                if cap.marker_offset is None and safe_marker in blob:
                    cap.marker_offset = blob.find(safe_marker)
                    cap.canary_seen.set()
                if not cap.forbidden_seen.is_set():
                    for marker in forbidden_markers:
                        if marker in blob:
                            cap.forbidden_offset = blob.find(marker)
                            cap.forbidden_seen.set()
                            break
            conn.close()
        except BaseException as exc:  # noqa: BLE001 - recorded, asserted
            cap.error = exc
        finally:
            cap.finished.set()

    thread = threading.Thread(target=task, daemon=True)
    thread.start()
    return thread


def _cleanup_client(release: threading.Event | None,
                    cap: _ClientCapture, thread: threading.Thread) -> None:
    """Finally cleanup, asserted: release the gate, wait for client
    finished, join the client thread - on every path."""
    if release is not None:
        release.set()
    if not cap.finished.wait(20):
        raise AssertionError("client did not finish")
    thread.join(5)
    if thread.is_alive():
        raise AssertionError("client thread did not join")


def _gated_exchange(harness: _ReasoningHarness, body: bytes,
                    cap: _ClientCapture, early_wait: float = 1.5,
                    release_marker: bytes = _CANARY_039,
                    wait_barrier_entered: bool = True) -> bool:
    """Release the upstream only AFTER the client saw
    ``release_marker`` (and, by default, the fake proved via its
    barrier-entered signal that its whole blocked section was
    flushed).  Returns True when early client receipt was proved; the
    immutable pre-release snapshot is stored on
    ``cap.pre_release_snapshot`` BEFORE the release Event is set."""
    thread = _start_client(harness.proxy_port, body, cap,
                           canary=release_marker)
    early = False
    try:
        early = cap.canary_seen.wait(early_wait)
        if early and wait_barrier_entered:
            early = _StreamUpstream.barrier_entered.wait(early_wait)
        if early:
            cap.pre_release_snapshot = cap.blob
        return early
    finally:
        _cleanup_client(_StreamUpstream.release, cap, thread)


def _gated_neg_exchange(harness: _ReasoningHarness, body: bytes,
                        cap: _DualCapture, *, safe_marker: bytes,
                        forbidden_markers: tuple[bytes, ...],
                        barrier_wait: float = 3.0,
                        pre_release_check=None) -> bytes:
    """Negative-observation gated exchange (event-based, no settle
    sleep).  Waits for the producer's barrier-entered event (the fake
    proved its whole blocked section is flushed and it is holding),
    then the client SAFE marker event (stream order: every byte before
    the safe marker - including any wrongly forwarded forbidden frame
    ahead of it - is already in the client buffer).  The immutable
    pre-release snapshot is taken and ``pre_release_check`` runs while
    the gate is still held (forbidden event/bytes must be absent).
    finally: release, client finished-wait, thread join (asserted)."""
    thread = _start_dual_client(harness.proxy_port, body, cap,
                                safe_marker, forbidden_markers)
    snapshot = b""
    try:
        if not _StreamUpstream.barrier_entered.wait(barrier_wait):
            raise AssertionError("producer never reached its barrier")
        if not cap.canary_seen.wait(barrier_wait):
            raise AssertionError("safe prefix marker never reached the "
                                 "client while the gate was held")
        snapshot = cap.pre_release_snapshot = cap.blob
        if pre_release_check is not None:
            pre_release_check(snapshot)
        return snapshot
    finally:
        _cleanup_client(_StreamUpstream.release, cap, thread)


# ---------------------------------------------------------------------------
# Import identity: the claimed boundary is the runtime of the tree
# containing THIS file - no stale checkout via accidental sys.path,
# no fake of the module under test.
# ---------------------------------------------------------------------------
class ImportIdentityTests(unittest.TestCase):
    def test_proxy_bound_to_this_tree(self) -> None:
        self.assertEqual(Path(proxy.__file__).resolve(),
                         (ROOT / "proxy.py").resolve())
        self.assertEqual(Path(sse_streaming.__file__).resolve(),
                         (ROOT / "sse_streaming.py").resolve())

    def test_classifier_identity_is_the_proxy_classifier(self) -> None:
        # The exact helper module the proxy streaming path invokes is
        # the module object this file (and the 038 harness) imported.
        self.assertIs(proxy.sse_streaming, sse_streaming)
        self.assertIs(_trs.proxy, proxy)


# ---------------------------------------------------------------------------
class _MinimalStreamingBase(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _ReasoningHarness(review_payload=b"{}")

    def tearDown(self) -> None:
        self.harness.close()


# ---------------------------------------------------------------------------
# A. Positive minimal-lifecycle early receipt (order 039-a req 1/5).
# ---------------------------------------------------------------------------
class MinimalLifecycleEarlyReceiptTests(_MinimalStreamingBase):
    def test_minimal_created_inprogress_reasoning_early_zero_review(
            self) -> None:
        """The EXACT observed minimal created/in_progress start frames
        (output ABSENT) permit real reasoning receipt while completion
        is held; the answer stays withheld; the final stream is
        byte-identical with zero review.  EXPECTED TO FAIL on the old
        runtime at the early-receipt assertion (EXPECTED_OLD_RUNTIME_
        FAILURES) and PASS on the 039 runtime with the same bytes."""
        payloads = _minimal_stream_payloads(_CLEAN)
        # Existing strict label agreement still applies to the minimal
        # variant: the created frame carries an event label that agrees
        # with its JSON type (the observed upstream is data-only; both
        # spellings must classify identically).
        created_frame = _labelled_frame(("event: response.created",),
                                        payloads[0])
        progress_frame = _frame(payloads[1])
        tail_frames = [_frame(payload) for payload in payloads[2:]]
        item_frame = tail_frames[0]
        part_frame = tail_frames[1]
        delta_frame = tail_frames[2]
        source = (created_frame + progress_frame + item_frame + part_frame
                  + delta_frame + b"".join(tail_frames[3:]))
        before = created_frame + progress_frame + item_frame + part_frame \
            + delta_frame
        after = b"".join(tail_frames[3:])
        # Fixture sanity: the observed shape really carries no output
        # key and the tail carries the withheld answer.
        self.assertNotIn(b'"output"', created_frame)
        self.assertNotIn(b'"output"', progress_frame)
        self.assertIn(_CANARY_039, before)
        self.assertIn(_CLEAN.encode(), after)
        self.assertNotIn(b"response.completed", before)
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _gated_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning receipt after the exact minimal "
            "created/in_progress start frames while completion was "
            "held: expected pre-039 old-runtime failure (minimal "
            "lifecycle snapshot unproven, one-way boundary at frame "
            "zero)")
        # Pre-release observation (release Event unset, barrier held):
        # the minimal frames themselves were early-forwarded and the
        # reasoning canary arrived.
        snapshot = cap.pre_release_snapshot
        self.assertIn(created_frame, snapshot)
        self.assertIn(progress_frame, snapshot)
        self.assertIn(item_frame, snapshot)
        self.assertIn(part_frame, snapshot)
        self.assertIn(_CANARY_039, snapshot)
        # Answer withheld while upstream is still blocked.
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.output_text.done", snapshot)
        self.assertNotIn(b"response.content_part.added", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Zero-review control: completed answer unchanged, raw bytes
        # end-to-end, no prefix replay (reasoning canary appears once
        # per copy: delta/done/part/item/completed = 5).
        self.assertEqual(cap.blob, source)
        self.assertEqual(cap.blob.count(_CANARY_039), 5)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_minimal_prefix_accepted_exact_edit_answer_withheld(
            self) -> None:
        """Same observed minimal start frames, answer carries the
        038-verified detector candidates and the review accepts the
        EXACT replacements: reasoning received early while completion
        is held, answer withheld, final stream carries the exact
        repaired answer at every answer copy with reasoning bytes
        untouched and exactly one review.  EXPECTED TO FAIL on the old
        runtime at the early-receipt assertion."""
        self.harness.close()
        self.harness = _ReasoningHarness(review_payload=_review_payload([
            _prop("t0", keep=False, replacement="presenetljiv"),
            _prop("t1", keep=False, replacement="presenetljiv"),
        ]))
        payloads = _minimal_stream_payloads(_ANSWER)
        # Data-only start frames: the exact observed spelling.
        created_frame = _frame(payloads[0])
        progress_frame = _frame(payloads[1])
        tail_frames = [_frame(payload) for payload in payloads[2:]]
        before = (created_frame + progress_frame + tail_frames[0]
                  + tail_frames[1] + tail_frames[2])
        after = b"".join(tail_frames[3:])
        self.assertNotIn(b'"output"', created_frame)
        self.assertNotIn(b'"output"', progress_frame)
        self.assertIn(_CANARY_039, before)
        self.assertIn(_ANSWER.encode(), after)
        self.assertNotIn(b"response.completed", before)
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _gated_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early,
            "no early reasoning receipt after the exact minimal "
            "created/in_progress start frames while completion was "
            "held: expected pre-039 old-runtime failure (minimal "
            "lifecycle snapshot unproven, one-way boundary at frame "
            "zero)")
        snapshot = cap.pre_release_snapshot
        self.assertIn(created_frame, snapshot)
        self.assertIn(progress_frame, snapshot)
        self.assertIn(tail_frames[0], snapshot)
        self.assertIn(tail_frames[1], snapshot)
        self.assertIn(_CANARY_039, snapshot)
        # Answer withheld while upstream is still blocked.
        self.assertNotIn(b"presenetliv", snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.output_text.done", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Final: exact repaired answer at every answer copy, reasoning
        # untouched (canary once per copy = 5), exactly one review.
        self.assertEqual(cap.blob,
                         _expected_edited(payloads, _EDITED_ANSWER))
        self.assertNotIn(b"presenetliv", cap.blob)
        self.assertEqual(cap.blob.count(_CANARY_039), 5)
        self.assertEqual(self.harness.review_calls(), 1)

    def test_minimal_prefix_one_shot_byte_identical_zero_review(
            self) -> None:
        """Content control (no barrier): the completed minimal stream
        is delivered whole, byte-identically, with zero review on both
        epochs (old runtime: buffered whole-body path; 039 runtime:
        early prefix plus ordinal-validated tail)."""
        payloads = _minimal_stream_payloads(_CLEAN)
        source = _render_stream(payloads)
        self.harness.upstream(before=source, after=b"")
        status, body = _one_shot(self.harness.proxy_port,
                                 _contract_body_stream())
        self.assertEqual(status, 200)
        self.assertEqual(body, source)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# B. Unsafe minimal-lookalike boundaries (order 039-a req 2/5): the
# proven minimal variant is exact-key and exact-type; every lookalike
# stays a conservative boundary.  The safe full-snapshot prefix before
# the lookalike may be early, the lookalike and every later frame
# stay buffered while completion is held, and the final stream is
# byte-identical with the lookalike preserved once in its original
# place.  Must pass on the old runtime AND the 039 runtime.
# ---------------------------------------------------------------------------
class MinimalLookalikeBoundaryTests(_MinimalStreamingBase):
    def _run_lookalike(self, lookalike: dict) -> None:
        payloads = _lookalike_stream_payloads(lookalike)
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        safe_frame = frames[0]  # legacy FULL snapshot: safe on both epochs
        self.harness.upstream(
            before=b"".join(frames[:5]), after=b"".join(frames[5:]),
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _DualCapture()

        def held_view(snapshot: bytes) -> None:
            # While completion is held: the safe full-prefix bytes were
            # actually received; the lookalike and every later frame
            # (reasoning canary, answer, completion) were NOT.
            self.assertIn(safe_frame, snapshot)
            self.assertNotIn(frames[1], snapshot)
            self.assertFalse(cap.forbidden_seen.is_set(),
                             "forbidden lookalike bytes observed while "
                             "completion was held")
            self.assertNotIn(_CANARY_039, snapshot)
            self.assertNotIn(_CLEAN.encode(), snapshot)
            self.assertNotIn(b"response.output_text.delta", snapshot)
            self.assertNotIn(b"response.completed", snapshot)

        _gated_neg_exchange(
            self.harness, _contract_body_stream(), cap,
            safe_marker=safe_frame, forbidden_markers=(frames[1],),
            pre_release_check=held_view)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Lookalike preserved raw, exactly once, in its original place.
        self.assertEqual(cap.blob, source)
        self.assertEqual(cap.blob.count(frames[1]), 1)

    def test_extra_event_key_lookalike_stays_conservative(self) -> None:
        self._run_lookalike(_LOOKALIKE_VARIANTS[0][1])
        self.assertEqual(self.harness.review_calls(), 0)

    def test_lookalike_matrix_stays_conservative(self) -> None:
        for name, lookalike in _LOOKALIKE_VARIANTS:
            with self.subTest(variant=name):
                self._run_lookalike(lookalike)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# C. Embedded minimal-response carrier (order 039-a req 2/5): the
# minimal variant is scoped to the DIRECT lifecycle branch only; a
# reasoning event carrying a minimal-shaped response stays a boundary
# (conservative carrier check), latching all later reasoning.
# ---------------------------------------------------------------------------
class EmbeddedCarrierTests(_MinimalStreamingBase):
    def test_embedded_minimal_response_carrier_stays_buffered(self) -> None:
        full = _DEL1_039 + _DEL2_039 + _DEL3_039
        carrier = _reasoning_delta(5, _DEL2_039)
        carrier["response"] = _minimal_response("r_emb")
        payloads = [
            _created(0),
            _in_progress(1),
            _reasoning_item_added(2),
            _reasoning_part_added(3),
            _reasoning_delta(4, _DEL1_039),
            carrier,
            _reasoning_delta(6, _DEL3_039),
            _reasoning_done(7, full),
            _reasoning_part_done(8, full),
            _reasoning_item_done(9, full),
            _message_item_added(10),
            _answer_part_added(11),
            _answer_delta(12, _CLEAN),
            _answer_done(13, _CLEAN),
            _answer_part_done(14, _CLEAN),
            _message_item_done(15, _CLEAN),
            _completed(16, _CLEAN, full),
        ]
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        # The safe prefix ends at the first delta (frame 4); the
        # carrier frame (5) is the boundary, the later delta (6) and
        # everything after it must stay buffered.
        safe_frame = frames[4]
        self.harness.upstream(
            before=b"".join(frames[:6]), after=b"".join(frames[6:]),
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _DualCapture()

        def held_view(snapshot: bytes) -> None:
            self.assertIn(safe_frame, snapshot)
            self.assertIn(_CANARY_039, snapshot)
            self.assertFalse(cap.forbidden_seen.is_set(),
                             "embedded carrier or later reasoning "
                             "observed while completion was held")
            self.assertNotIn(b"r_emb", snapshot)
            self.assertNotIn(_LATER_MARKER_039, snapshot)
            self.assertNotIn(b"response.output_text.delta", snapshot)
            self.assertNotIn(b"response.completed", snapshot)

        _gated_neg_exchange(
            self.harness, _contract_body_stream(), cap,
            safe_marker=safe_frame,
            forbidden_markers=(b"r_emb", _LATER_MARKER_039),
            pre_release_check=held_view)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# D. Post-prefix latch (order 039-a req 3/5): an unknown event or an
# answer payload after a safe full-snapshot prefix irrevocably buffers
# ALL later frames, including later reasoning.  Must pass on both
# epochs (038 boundary semantics, unchanged by 039).
# ---------------------------------------------------------------------------
class PostPrefixLatchTests(_MinimalStreamingBase):
    def test_unknown_event_after_safe_prefix_latches_later_reasoning(
            self) -> None:
        full = _DEL1_039 + _DEL2_039 + _DEL3_039
        unknown = {"type": "response.synthetic_unknown_event",
                   "sequence_number": 5, "payload": {"tag": "unk039"}}
        payloads = [
            _created(0),
            _in_progress(1),
            _reasoning_item_added(2),
            _reasoning_part_added(3),
            _reasoning_delta(4, _DEL1_039),
            unknown,
            _reasoning_delta(6, _DEL3_039),
            _reasoning_done(7, full),
            _reasoning_part_done(8, full),
            _reasoning_item_done(9, full),
            _message_item_added(10),
            _answer_part_added(11),
            _answer_delta(12, _CLEAN),
            _answer_done(13, _CLEAN),
            _answer_part_done(14, _CLEAN),
            _message_item_done(15, _CLEAN),
            _completed(16, _CLEAN, full),
        ]
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        safe_frame = frames[4]
        self.harness.upstream(
            before=b"".join(frames[:7]), after=b"".join(frames[7:]),
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _DualCapture()

        def held_view(snapshot: bytes) -> None:
            self.assertIn(safe_frame, snapshot)
            self.assertIn(_CANARY_039, snapshot)
            self.assertFalse(cap.forbidden_seen.is_set(),
                             "unknown event or later reasoning observed "
                             "while completion was held")
            self.assertNotIn(b"synthetic_unknown_event", snapshot)
            self.assertNotIn(_LATER_MARKER_039, snapshot)
            self.assertNotIn(_CLEAN.encode(), snapshot)
            self.assertNotIn(b"response.output_text.delta", snapshot)
            self.assertNotIn(b"response.completed", snapshot)

        _gated_neg_exchange(
            self.harness, _contract_body_stream(), cap,
            safe_marker=safe_frame,
            forbidden_markers=(b"synthetic_unknown_event",
                               _LATER_MARKER_039),
            pre_release_check=held_view)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_answer_frame_after_safe_prefix_latches_later_reasoning(
            self) -> None:
        full = _DEL1_039 + _DEL2_039 + _DEL3_039
        payloads = [
            _created(0),
            _in_progress(1),
            _reasoning_item_added(2),
            _reasoning_part_added(3),
            _reasoning_delta(4, _DEL1_039),
            _message_item_added(5),
            _answer_part_added(6),
            _answer_delta(7, _CLEAN),
            _reasoning_delta(8, _DEL3_039),
            _reasoning_done(9, full),
            _reasoning_part_done(10, full),
            _reasoning_item_done(11, full),
            _answer_done(12, _CLEAN),
            _answer_part_done(13, _CLEAN),
            _message_item_done(14, _CLEAN),
            _completed(15, _CLEAN, full),
        ]
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        safe_frame = frames[4]
        # The answer delta is ALREADY FLUSHED to the fake before the
        # barrier: it must stay withheld together with the later
        # reasoning until the completed tail is delivered.
        self.assertIn(_CLEAN.encode(), b"".join(frames[:9]))
        self.harness.upstream(
            before=b"".join(frames[:9]), after=b"".join(frames[9:]),
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _DualCapture()

        def held_view(snapshot: bytes) -> None:
            self.assertIn(safe_frame, snapshot)
            self.assertIn(_CANARY_039, snapshot)
            self.assertFalse(cap.forbidden_seen.is_set(),
                             "answer or later reasoning observed while "
                             "completion was held")
            self.assertNotIn(b"response.output_text.delta", snapshot)
            self.assertNotIn(_CLEAN.encode(), snapshot)
            self.assertNotIn(_LATER_MARKER_039, snapshot)
            self.assertNotIn(b"response.completed", snapshot)

        _gated_neg_exchange(
            self.harness, _contract_body_stream(), cap,
            safe_marker=safe_frame,
            forbidden_markers=(b"response.output_text.delta",
                               _LATER_MARKER_039),
            pre_release_check=held_view)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# ---------------------------------------------------------------------------
# E. Full vLLM-style snapshot path unchanged (order 039-a req 2): the
# general full-snapshot validator retains behavior - full snapshots
# (empty or reasoning-only output) still early-forward, byte-identical
# zero-review final.  Must pass on the old runtime AND the 039
# runtime.
# ---------------------------------------------------------------------------
class FullSnapshotUnchangedTests(_MinimalStreamingBase):
    def test_full_snapshot_lifecycle_still_early_zero_review(self) -> None:
        payloads = _trs._stream_payloads(_CLEAN)
        source = _render_stream(payloads)
        cut = _trs._prefix_cut(False) + 1  # through the reasoning delta
        before = _render_stream(payloads[:cut])
        after = _render_stream(payloads[cut:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _gated_exchange(
            self.harness, _contract_body_stream(), cap,
            release_marker=_trs._CANARY)
        self.assertTrue(
            early, "full-snapshot early receipt regressed: the 038 "
            "vLLM-style snapshot path must be unchanged")
        snapshot = cap.pre_release_snapshot
        self.assertIn(_frame(payloads[0]), snapshot)
        self.assertIn(_frame(payloads[1]), snapshot)
        self.assertIn(_trs._CANARY, snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)

    def test_full_snapshot_with_reasoning_output_still_early(self) -> None:
        # A full in_progress snapshot whose output list holds the
        # proven reasoning item: the general full-snapshot validator
        # (unchanged by 039) must still early-forward it.
        reasoning_item = {"type": "reasoning", "id": "rs_1",
                          "status": "in_progress",
                          "summary": [], "content": []}
        payloads = [
            _created(0),
            _in_progress(1, {"id": "r_1", "object": "response",
                             "status": "in_progress",
                             "output": [reasoning_item]}),
            _reasoning_item_added(2),
            _reasoning_part_added(3),
            _reasoning_delta(4, _REASONING_039),
            _reasoning_done(5, _REASONING_039),
            _reasoning_part_done(6, _REASONING_039),
            _reasoning_item_done(7, _REASONING_039),
            _message_item_added(8),
            _answer_part_added(9),
            _answer_delta(10, _CLEAN),
            _answer_done(11, _CLEAN),
            _answer_part_done(12, _CLEAN),
            _message_item_done(13, _CLEAN),
            _completed(14, _CLEAN, _REASONING_039),
        ]
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        self.harness.upstream(
            before=b"".join(frames[:5]), after=b"".join(frames[5:]),
            barrier=True, inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        early = _gated_exchange(self.harness, _contract_body_stream(), cap)
        self.assertTrue(
            early, "full snapshot with proven reasoning output must "
            "still early-forward (038 validator unchanged)")
        snapshot = cap.pre_release_snapshot
        self.assertIn(frames[0], snapshot)
        self.assertIn(frames[1], snapshot)
        self.assertIn(_CANARY_039, snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# Positive cases expected to fail on the old runtime (pre-039) at the
# early-receipt assertion; they must pass on the 039 runtime with the
# same test bytes.  Every other test in this file must pass on both.
EXPECTED_OLD_RUNTIME_FAILURES: tuple[str, ...] = (
    "MinimalLifecycleEarlyReceiptTests."
    "test_minimal_created_inprogress_reasoning_early_zero_review",
    "MinimalLifecycleEarlyReceiptTests."
    "test_minimal_prefix_accepted_exact_edit_answer_withheld",
)
