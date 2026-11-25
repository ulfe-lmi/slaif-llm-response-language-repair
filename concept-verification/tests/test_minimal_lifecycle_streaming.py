"""039-a independent transport regression tests: minimal lifecycle
snapshots (checkpoint file - remaining ordered cases extend this
file after the checkpoint).

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

EXPECTED_OLD_RUNTIME_FAILURES: the positive case below is EXPECTED TO
FAIL on the old runtime (pre-039, accepted 6f7ae1a code) at the
early-receipt assertion - the old classifier rejects the first
minimal frame as "lifecycle snapshot unproven" (output absent) and
the one-way boundary latches at frame zero, so no reasoning reaches
the client while completion is held.  That failure is the ordered
old-runtime evidence and is distinct from fixture/setup failure:
the negative case must PASS on the old runtime and keep passing on
the 039 runtime with the same test bytes.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
import sse_streaming  # noqa: E402
import tests.test_reasoning_streaming as _trs  # noqa: E402
from tests.test_reasoning_streaming import (  # noqa: E402
    _ClientCapture,
    _StreamUpstream,
    _ReasoningHarness,
    _answer_delta,
    _answer_done,
    _answer_part_added,
    _answer_part_done,
    _completed,
    _contract_body_stream,
    _created,
    _frame,
    _labelled_frame,
    _message_item_added,
    _message_item_done,
    _reasoning_delta,
    _reasoning_done,
    _reasoning_item_added,
    _reasoning_item_done,
    _reasoning_part_added,
    _reasoning_part_done,
    _start_client,
)

# ---------------------------------------------------------------------------
# 039 minimal lifecycle fixtures (order 039-a requirement 1 shape).
# ---------------------------------------------------------------------------
_REASONING_039 = "razmislek-z-039 preverim obliko zveze."
_CANARY_039 = b"razmislek-z-039"
_CLEAN = _trs._CLEAN  # zero-review answer (038-established fixture)


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


# ---------------------------------------------------------------------------
# Gated exchange with guaranteed finally cleanup (release/join) on all
# paths - including the expected old-runtime receipt failure.
# ---------------------------------------------------------------------------
def _gated_exchange(harness: _ReasoningHarness, body: bytes,
                    cap: _ClientCapture, early_wait: float = 1.5,
                    release_marker: bytes = _CANARY_039,
                    wait_barrier_entered: bool = True) -> bool:
    """Release the upstream only AFTER the client saw
    ``release_marker`` (and, by default, the fake proved via its
    barrier-entered signal that its whole blocked section was
    flushed).  Returns True when early client receipt was proved.
    The immutable pre-release snapshot is stored on
    ``cap.pre_release_snapshot`` BEFORE the release Event is set.
    The barrier is released, the client is joined and the fixture
    thread cannot outlive the call on EVERY path (finally)."""
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
        release = _StreamUpstream.release
        if release is not None:
            release.set()
        cap.finished.wait(20)
        thread.join(5)


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
# A. Positive minimal-lifecycle early receipt (order 039-a req 1/5).
# ---------------------------------------------------------------------------
class _MinimalStreamingBase(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = _ReasoningHarness(review_payload=b"{}")

    def tearDown(self) -> None:
        self.harness.close()


class MinimalLifecycleEarlyReceiptTests(_MinimalStreamingBase):
    def test_minimal_created_inprogress_reasoning_early_zero_review(
            self) -> None:
        """The EXACT observed minimal created+in_progress start frames
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


# ---------------------------------------------------------------------------
# B. Unsafe minimal-lookalike boundary (order 039-a req 2/5): an
# extra event-level key on an otherwise-minimal in_progress frame is
# NOT the proven shape and must stay a boundary; the safe prefix
# before it may be early, everything from the lookalike on is
# buffered, and the final stream is byte-identical.  Must pass on the
# old runtime AND the 039 runtime.
# ---------------------------------------------------------------------------
class MinimalLookalikeBoundaryTests(_MinimalStreamingBase):
    def test_extra_event_key_lookalike_stays_conservative(self) -> None:
        lookalike = _minimal_in_progress(1, _minimal_response("r_l01"),
                                         metadata={"tag": "l01"})
        payloads = [
            _created(0),  # legacy FULL snapshot: safe on both epochs
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
        frames = [_frame(payload) for payload in payloads]
        source = b"".join(frames)
        before = b"".join(frames[:5])
        after = b"".join(frames[5:])
        self.harness.upstream(
            before=before, after=after, barrier=True,
            inter_frame_sleep=0.004, barrier_timeout=4.0)
        cap = _ClientCapture()
        snapshot = _trs._blocked_probe(self.harness, _contract_body_stream(),
                                       cap)
        # The safe full-snapshot prefix was early (streaming started);
        # the lookalike and every later frame stayed buffered.
        self.assertIn(frames[0], snapshot)
        self.assertNotIn(frames[1], snapshot)
        self.assertNotIn(b"r_l01", snapshot)
        self.assertNotIn(_CANARY_039, snapshot)
        self.assertNotIn(b"response.output_text.delta", snapshot)
        self.assertNotIn(b"response.completed", snapshot)
        self.assertNotIn(_CLEAN.encode(), snapshot)
        self.assertIsNone(cap.error, f"client error: {cap.error!r}")
        self.assertEqual(cap.status, 200)
        # Lookalike preserved raw, once, in its original place.
        self.assertEqual(cap.blob, source)
        self.assertEqual(self.harness.review_calls(), 0)


# Positive cases expected to fail on the old runtime (pre-039) at the
# early-receipt assertion; they must pass on the 039 runtime with the
# same test bytes.  Every other test in this file must pass on both.
EXPECTED_OLD_RUNTIME_FAILURES: tuple[str, ...] = (
    "MinimalLifecycleEarlyReceiptTests."
    "test_minimal_created_inprogress_reasoning_early_zero_review",
)
