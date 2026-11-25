"""Unit tests for the SSE early-reasoning prefix classifier (order 038-a).

The classifier (sse_streaming.classify_prefix_frame) is the R1
one-way buffer boundary: a complete frame is forwarded early only
when its event label and JSON type agree AND its structure is
explicitly proven answer-free.  These tests pin the label semantics
(one optional SSE space), the structural proof rules (proven
reasoning families only; message, tool, unknown and malformed shapes
buffer) and the frame-splitting helpers.  The production strict
parser proxy._event is used as the data parser throughout.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
from sse_streaming import (  # noqa: E402
    _minimal_lifecycle_denial_reason,
    classify_prefix_frame,
    emit_new_frames,
    split_complete_frames,
    strip_event_logprobs,
)


def _frame(label: str, obj: object, spaced: bool = False) -> str:
    sep = ": " if spaced else ":"
    return f"event{sep}{label}\ndata: {json.dumps(obj, separators=(',', ':'))}\n\n"


def _data_frame(obj: object) -> str:
    return f"data: {json.dumps(obj, separators=(',', ':'))}\n\n"


def _decision(block: str):
    return classify_prefix_frame(block, proxy._event)


_RESPONSE = {"object": "response", "status": "in_progress", "output": []}
_REASONING_ITEM = {
    "type": "reasoning",
    "id": "rs_1",
    "status": "in_progress",
    "summary": [],
}
_MESSAGE_ITEM = {
    "type": "message",
    "id": "msg_1",
    "status": "in_progress",
    "content": [{"type": "output_text", "text": ""}],
}
_FUNCTION_ITEM = {
    "type": "function_call",
    "id": "call_test",
    "name": "f",
    "arguments": {},
}


class TestLabelSemantics(unittest.TestCase):
    """SSE consumes exactly one optional space after the field colon."""

    def test_spaced_label_classifies(self):
        d = _decision(_frame("response.created", {"type": "response.created", "response": _RESPONSE}, spaced=True))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_unspaced_label_classifies(self):
        d = _decision(_frame("response.created", {"type": "response.created", "response": _RESPONSE}))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_double_space_label_buffers(self):
        block = "event:  response.created\ndata: " + json.dumps({"type": "response.created", "response": _RESPONSE}) + "\n\n"
        self.assertEqual((_decision(block).kind, _decision(block).reason), ("boundary", "event label/type mismatch"))

    def test_true_label_mismatch_buffers(self):
        block = _frame("response.output_text.delta", {"type": "response.reasoning_text.delta", "delta": "x"})
        self.assertEqual((_decision(block).kind, _decision(block).reason), ("boundary", "event label/type mismatch"))

    def test_duplicate_labels_buffer(self):
        obj = {"type": "response.created", "response": _RESPONSE}
        block = "event: response.created\nevent: response.created\ndata: " + json.dumps(obj) + "\n\n"
        self.assertEqual((_decision(block).kind, _decision(block).reason), ("boundary", "duplicate event label"))

    def test_label_without_data_buffers(self):
        self.assertEqual((_decision("event: response.created\n\n").kind, _decision("event: response.created\n\n").reason), ("boundary", "event label without data"))

    def test_framing_only_is_prefix(self):
        d = _decision(": keepalive\n\n")
        self.assertEqual((d.kind, d.reason, d.value), ("prefix", "framing only", None))

    def test_done_marker_buffers(self):
        for block in ("data: [DONE]\n\n", "data:[DONE]\n\n"):
            self.assertEqual((_decision(block).kind, _decision(block).reason), ("boundary", "terminal marker"), block)

    def test_unknown_field_line_buffers(self):
        obj = {"type": "response.created", "response": _RESPONSE}
        block = "event: response.created\nfoo: bar\ndata: " + json.dumps(obj) + "\n\n"
        self.assertEqual((_decision(block).kind, _decision(block).reason), ("boundary", "unknown field line"))


class TestLifecycleSnapshot(unittest.TestCase):
    """Only explicitly proven reasoning-only snapshots forward early."""

    def _lifecycle(self, etype, response):
        return _frame(etype, {"type": etype, "response": response})

    def test_empty_output_proven(self):
        d = _decision(self._lifecycle("response.created", _RESPONSE))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_terminal_status_boundary(self):
        # R1: a PRESENT status must be a known nonterminal response
        # status; terminal statuses are denied even with empty output.
        for status in ("completed", "failed", "incomplete", "cancelled"):
            with self.subTest(status=status):
                d = _decision(self._lifecycle("response.created", {"object": "response", "status": status, "output": []}))
                self.assertEqual((d.kind, d.reason), ("boundary", "lifecycle terminal or unknown status"))

    def test_unknown_wrongtype_and_null_status_boundary(self):
        for status in ("mystery", 123, None, ["in_progress"]):
            with self.subTest(status=status):
                d = _decision(self._lifecycle("response.created", {"object": "response", "status": status, "output": []}))
                self.assertEqual((d.kind, d.reason), ("boundary", "lifecycle terminal or unknown status"))

    def test_known_nonterminal_status_prefix(self):
        for status in ("queued", "in_progress"):
            with self.subTest(status=status):
                d = _decision(self._lifecycle("response.created", {"object": "response", "status": status, "output": []}))
                self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_absent_status_compatibility_prefix(self):
        d = _decision(self._lifecycle("response.created", {"object": "response", "output": []}))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_present_error_boundary(self):
        d = _decision(self._lifecycle("response.created", {"object": "response", "status": "in_progress", "error": {"code": "x"}, "output": []}))
        self.assertEqual((d.kind, d.reason), ("boundary", "lifecycle snapshot carries error"))

    def test_present_incomplete_details_boundary(self):
        d = _decision(self._lifecycle("response.created", {"object": "response", "status": "in_progress", "incomplete_details": {"reason": "max_output_tokens"}, "output": []}))
        self.assertEqual((d.kind, d.reason), ("boundary", "lifecycle snapshot carries incomplete_details"))

    def test_null_error_and_incomplete_details_compatibility_prefix(self):
        d = _decision(self._lifecycle("response.created", {"object": "response", "status": "in_progress", "error": None, "incomplete_details": None, "output": []}))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_completed_reasoning_item_in_snapshot_still_prefix(self):
        # The item object's own status is a different object: a
        # completed reasoning item inside a nonterminal snapshot is
        # still proven reasoning-only.
        done_item = dict(_REASONING_ITEM, status="completed")
        d = _decision(self._lifecycle("response.in_progress", {"object": "response", "status": "in_progress", "output": [done_item]}))
        self.assertEqual((d.kind, d.reason), ("prefix", "lifecycle no-answer-payload"))

    def test_reasoning_item_snapshot_proven(self):
        response = dict(_RESPONSE, output=[dict(_REASONING_ITEM)])
        self.assertEqual(_decision(self._lifecycle("response.in_progress", response)).kind, "prefix")

    def test_message_item_boundary(self):
        response = dict(_RESPONSE, output=[dict(_MESSAGE_ITEM)])
        self.assertEqual((_decision(self._lifecycle("response.in_progress", response)).kind, _decision(self._lifecycle("response.in_progress", response)).reason), ("boundary", "lifecycle message item"))

    def test_function_call_item_boundary(self):
        # Helper review 1 reproducer: a first tool item is a boundary.
        response = dict(_RESPONSE, output=[dict(_FUNCTION_ITEM)])
        self.assertEqual((_decision(self._lifecycle("response.created", response)).kind, _decision(self._lifecycle("response.created", response)).reason), ("boundary", "lifecycle non-reasoning item"))

    def test_missing_output_unproven(self):
        response = {"object": "response", "status": "in_progress"}
        self.assertEqual((_decision(self._lifecycle("response.created", response)).kind, _decision(self._lifecycle("response.created", response)).reason), ("boundary", "lifecycle snapshot unproven"))

    def test_missing_response_unproven(self):
        d = _decision(_frame("response.created", {"type": "response.created"}))
        self.assertEqual((d.kind, d.reason), ("boundary", "lifecycle snapshot unproven"))

    def test_summary_not_list_unproven(self):
        item = dict(_REASONING_ITEM, summary="oops")
        response = dict(_RESPONSE, output=[item])
        self.assertEqual((_decision(self._lifecycle("response.created", response)).kind, _decision(self._lifecycle("response.created", response)).reason), ("boundary", "lifecycle reasoning item unproven"))

    def test_part_text_nonstring_unproven(self):
        item = dict(_REASONING_ITEM, summary=[{"type": "summary_text", "text": 5}])
        response = dict(_RESPONSE, output=[item])
        self.assertEqual(_decision(self._lifecycle("response.created", response)).kind, "boundary")

    def test_unknown_part_type_unproven(self):
        item = dict(_REASONING_ITEM, summary=[{"type": "mystery"}])
        response = dict(_RESPONSE, output=[item])
        self.assertEqual(_decision(self._lifecycle("response.created", response)).kind, "boundary")

    def test_nondict_item_unproven(self):
        response = dict(_RESPONSE, output=["x"])
        self.assertEqual((_decision(self._lifecycle("response.created", response)).kind, _decision(self._lifecycle("response.created", response)).reason), ("boundary", "lifecycle unproven output item"))


class TestMinimalLifecycle(unittest.TestCase):
    """039-a R1/R2: the exact proven minimal start-frame variant.

    A top-level response.created / response.in_progress frame is
    forwardable when its complete structure is exactly
    {type, response, sequence_number} / {id, object, status} with a
    real nonnegative integer sequence, so output is ABSENT and no
    answer payload can ride in the frame.  Every deviation remains a
    boundary through the unchanged general validator, and the variant
    never applies to response.queued or to embedded carriers.
    """

    _RESPONSE_MINIMAL = {"id": "r_1", "object": "response",
                         "status": "in_progress"}

    def _minimal_value(self, etype, seq=0, response=None, **extra):
        value = {"type": etype, "sequence_number": seq,
                 "response": dict(self._RESPONSE_MINIMAL,
                                  **(response or {}))}
        value.update(extra)
        return value

    def _minimal_frame(self, etype, seq=0, label=True, response=None,
                       **extra):
        value = self._minimal_value(etype, seq, response, **extra)
        if not label:
            return _data_frame(value)
        return _frame(etype, value)

    # --- positive: the proven minimal shape forwards early ----------

    def test_created_minimal_prefix(self):
        d = _decision(self._minimal_frame("response.created"))
        self.assertEqual((d.kind, d.reason),
                         ("prefix", "lifecycle minimal no-answer-payload"))
        self.assertEqual(
            d.value,
            {"type": "response.created", "sequence_number": 0,
             "response": self._RESPONSE_MINIMAL})

    def test_in_progress_minimal_prefix(self):
        d = _decision(self._minimal_frame("response.in_progress", seq=1))
        self.assertEqual((d.kind, d.reason),
                         ("prefix", "lifecycle minimal no-answer-payload"))

    def test_minimal_without_label_prefix(self):
        # No event: label - the JSON type alone identifies the family.
        d = _decision(self._minimal_frame("response.created", label=False))
        self.assertEqual((d.kind, d.reason),
                         ("prefix", "lifecycle minimal no-answer-payload"))

    def test_minimal_sequence_values(self):
        for seq in (0, 1, 2**32):
            with self.subTest(seq=seq):
                d = _decision(self._minimal_frame("response.created", seq=seq))
                self.assertEqual(d.kind, "prefix", seq)

    def test_minimal_nonempty_id(self):
        d = _decision(self._minimal_frame(
            "response.created",
            response={"id": "x" * 50, "object": "response",
                      "status": "in_progress"}))
        self.assertEqual(d.kind, "prefix")

    def test_minimal_frame_logprobs_strip_noop(self):
        # The minimal response carries no defined logprobs location, so
        # the shared strip is a no-op and the raw block is forwarded.
        value = self._minimal_value("response.created")
        self.assertFalse(strip_event_logprobs(value))
        self.assertEqual(value, self._minimal_value("response.created"))

    # --- negative: every deviation remains a boundary ---------------

    def test_queued_is_outside_minimal_scope(self):
        # response.queued is not in the minimal variant; absent output
        # is still unproven for it (general validator unchanged).
        d = _decision(self._minimal_frame("response.queued"))
        self.assertEqual((d.kind, d.reason),
                         ("boundary", "lifecycle snapshot unproven"))

    def test_event_extra_key_boundary(self):
        d = _decision(self._minimal_frame("response.created", model="m"))
        self.assertEqual((d.kind, d.reason),
                         ("boundary", "lifecycle snapshot unproven"))

    def test_missing_sequence_number_boundary(self):
        value = self._minimal_value("response.created")
        del value["sequence_number"]
        d = _decision(_frame("response.created", value))
        self.assertEqual((d.kind, d.reason),
                         ("boundary", "lifecycle snapshot unproven"))

    def test_sequence_not_real_integer_boundary(self):
        for seq in (True, False, -1, 0.0, "0", None, [0]):
            with self.subTest(seq=repr(seq)):
                d = _decision(self._minimal_frame("response.created", seq=seq))
                self.assertEqual(d.kind, "boundary")

    def test_response_non_dict_boundary(self):
        for resp in (None, "x", 5, []):
            with self.subTest(resp=repr(resp)):
                value = self._minimal_value("response.created")
                value["response"] = resp
                d = _decision(_frame("response.created", value))
                self.assertEqual((d.kind, d.reason),
                                 ("boundary", "lifecycle snapshot unproven"))

    def test_response_extra_key_boundary(self):
        # Any fourth response key is not the exact minimal shape; in
        # particular output present as null is not an empty list.
        for extra in ({"output": None}, {"model": "m"}):
            with self.subTest(extra=repr(extra)):
                response = dict(self._RESPONSE_MINIMAL)
                response.update(extra)
                d = _decision(self._minimal_frame("response.created",
                                                  response=response))
                self.assertEqual((d.kind, d.reason),
                                 ("boundary", "lifecycle snapshot unproven"))

    def test_output_empty_list_is_full_snapshot_not_minimal(self):
        # Four response keys (with output: []) are the GENERAL
        # full-snapshot proof, not the minimal variant: unchanged old
        # behavior and old reason.
        response = dict(self._RESPONSE_MINIMAL, output=[])
        d = _decision(self._minimal_frame("response.created",
                                          response=response))
        self.assertEqual((d.kind, d.reason),
                         ("prefix", "lifecycle no-answer-payload"))

    def test_id_unproven_boundary(self):
        for rid in ("", 5, None, ["r"]):
            with self.subTest(rid=repr(rid)):
                response = dict(self._RESPONSE_MINIMAL, id=rid)
                d = _decision(self._minimal_frame("response.created",
                                                  response=response))
                self.assertEqual((d.kind, d.reason),
                                 ("boundary", "lifecycle snapshot unproven"))

    def test_object_unproven_boundary(self):
        for obj in ("resp", "Response", None, 1):
            with self.subTest(obj=repr(obj)):
                response = dict(self._RESPONSE_MINIMAL, object=obj)
                d = _decision(self._minimal_frame("response.created",
                                                  response=response))
                self.assertEqual((d.kind, d.reason),
                                 ("boundary", "lifecycle snapshot unproven"))

    def test_status_unproven_boundary(self):
        cases = {"queued": "lifecycle snapshot unproven",
                 "completed": "lifecycle terminal or unknown status",
                 "mystery": "lifecycle terminal or unknown status",
                 None: "lifecycle terminal or unknown status",
                 1: "lifecycle terminal or unknown status"}
        for status, reason in cases.items():
            with self.subTest(status=repr(status)):
                response = dict(self._RESPONSE_MINIMAL, status=status)
                d = _decision(self._minimal_frame("response.created",
                                                  response=response))
                self.assertEqual((d.kind, d.reason), ("boundary", reason))

    def test_malformed_and_duplicate_keys_boundary(self):
        block = ("event: response.created\n"
                 "data: {\"type\":\"response.created\",\"sequence_number\":0,"
                 "\"response\":{\"id\":\"r_1\"},"
                 "\"response\":{\"id\":\"r_1\"}}\n\n")
        self.assertEqual((_decision(block).kind, _decision(block).reason),
                         ("boundary", "malformed data"))
        self.assertEqual((_decision("event: response.created\ndata: {nope}\n\n").kind,
                          _decision("event: response.created\ndata: {nope}\n\n").reason),
                         ("boundary", "malformed data"))

    def test_label_mismatch_boundary(self):
        block = _frame("response.in_progress",
                       self._minimal_value("response.created"))
        self.assertEqual((_decision(block).kind, _decision(block).reason),
                         ("boundary", "event label/type mismatch"))

    def test_embedded_minimal_carrier_stays_conservative(self):
        # R2: the reasoning embedded-carrier check does NOT adopt the
        # minimal proof; an unproven response carrier still buffers.
        value = {"type": "response.reasoning_text.delta",
                 "sequence_number": 2, "delta": "x",
                 "response": dict(self._RESPONSE_MINIMAL)}
        d = _decision(_frame("response.reasoning_text.delta", value))
        self.assertEqual((d.kind, d.reason),
                         ("boundary", "embedded response unproven"))

    def test_full_snapshot_regression(self):
        # vLLM-style full snapshots keep their existing behavior and
        # reason, with or without sequence_number on the event.
        full = {"type": "response.created", "sequence_number": 0,
                "response": dict(self._RESPONSE_MINIMAL, output=[])}
        self.assertEqual(
            (_decision(_frame("response.created", full)).kind,
             _decision(_frame("response.created", full)).reason),
            ("prefix", "lifecycle no-answer-payload"))
        full_item = {"type": "response.in_progress",
                     "response": dict(self._RESPONSE_MINIMAL,
                                      output=[dict(_REASONING_ITEM)])}
        self.assertEqual(
            _decision(self._lifecycle_like("response.in_progress",
                                           full_item)).kind, "prefix")

    def _lifecycle_like(self, etype, value):
        return _frame(etype, value)

    # --- helper denial reasons (direct unit proof) -------------------

    def test_helper_reasons(self):
        base = self._minimal_value("response.created")
        self.assertIsNone(_minimal_lifecycle_denial_reason(base))
        self.assertIsNone(
            _minimal_lifecycle_denial_reason(
                self._minimal_value("response.in_progress", seq=7)))
        cases = [
            (dict(base, type="response.queued"),
             "minimal lifecycle event unproven"),
            (dict(base, type=None), "minimal lifecycle event unproven"),
            (dict(base, extra=1), "minimal lifecycle shape unproven"),
            ({k: v for k, v in base.items() if k != "sequence_number"},
             "minimal lifecycle shape unproven"),
            (dict(base, sequence_number=True),
             "minimal lifecycle sequence unproven"),
            (dict(base, sequence_number=-1),
             "minimal lifecycle sequence unproven"),
            (dict(base, sequence_number=0.0),
             "minimal lifecycle sequence unproven"),
            (dict(base, response="x"),
             "minimal lifecycle response unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, output=[])),
             "minimal lifecycle response shape unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, id="")),
             "minimal lifecycle id unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, id=5)),
             "minimal lifecycle id unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, object="resp")),
             "minimal lifecycle object unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, status="queued")),
             "minimal lifecycle status unproven"),
            (dict(base, response=dict(self._RESPONSE_MINIMAL, status="completed")),
             "minimal lifecycle status unproven"),
        ]
        for value, reason in cases:
            with self.subTest(reason=reason):
                self.assertEqual(_minimal_lifecycle_denial_reason(value),
                                 reason)


class TestReasoningFamilies(unittest.TestCase):
    """Proven reasoning shapes forward; everything else buffers."""

    def test_item_added_reasoning(self):
        d = _decision(_frame("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": _REASONING_ITEM}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning item"))

    def test_item_added_message_boundary(self):
        d = _decision(_frame("response.output_item.added", {"type": "response.output_item.added", "output_index": 1, "item": _MESSAGE_ITEM}))
        self.assertEqual((d.kind, d.reason), ("boundary", "message item"))

    def test_item_added_function_boundary(self):
        d = _decision(_frame("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": _FUNCTION_ITEM}))
        self.assertEqual((d.kind, d.reason), ("boundary", "non-reasoning item"))

    def test_item_added_missing_item_boundary(self):
        d = _decision(_frame("response.output_item.added", {"type": "response.output_item.added", "output_index": 0}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning item unproven"))

    def test_item_done_native_content_proven(self):
        # Characterization v8: done reasoning item carries
        # content_types=[reasoning_text] with empty summary.
        item = dict(_REASONING_ITEM, status="completed", summary=[],
                    content=[{"type": "reasoning_text", "text": "t"}])
        d = _decision(_frame("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": item}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning item"))

    def test_part_added_summary_family(self):
        d = _decision(_frame("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "summary_text", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning part"))

    def test_part_added_native_family(self):
        d = _decision(_frame("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "reasoning_text", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning part"))

    def test_part_missing_unproven(self):
        d = _decision(_frame("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1"}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning part unproven"))

    def test_part_unknown_type_unproven(self):
        d = _decision(_frame("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1", "part": {"type": "mystery", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning part unproven"))

    def test_text_delta(self):
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "output_index": 0, "content_index": 0, "delta": "t"}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning text"))

    def test_text_delta_nonstring_unproven(self):
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": 5}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning delta unproven"))

    def test_text_done(self):
        d = _decision(_frame("response.reasoning_text.done", {"type": "response.reasoning_text.done", "item_id": "rs_1", "output_index": 0, "content_index": 0}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning text"))

    def test_content_part_summary(self):
        d = _decision(_frame("response.content_part.added", {"type": "response.content_part.added", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "summary_text", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning summary part"))

    def test_content_part_output_text_boundary(self):
        d = _decision(_frame("response.content_part.added", {"type": "response.content_part.added", "item_id": "m_1", "output_index": 1, "content_index": 0, "part": {"type": "output_text", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "non-reasoning part"))

    def test_terminal_boundary(self):
        d = _decision(_frame("response.completed", {"type": "response.completed", "response": {"object": "response", "status": "completed", "output": [_MESSAGE_ITEM]}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "terminal event"))

    def test_unrecognized_type_boundary(self):
        d = _decision(_frame("response.server_event", {"type": "response.server_event", "message": "x"}))
        self.assertEqual((d.kind, d.reason), ("boundary", "unrecognized event type"))

    def test_summary_part_added(self):
        d = _decision(_frame("response.reasoning_summary_part.added", {"type": "response.reasoning_summary_part.added", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "summary_text", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning part"))

    def test_summary_part_done(self):
        d = _decision(_frame("response.reasoning_summary_part.done", {"type": "response.reasoning_summary_part.done", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "summary_text", "text": "s"}}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning part"))

    def test_summary_part_unknown_type_unproven(self):
        d = _decision(_frame("response.reasoning_summary_part.added", {"type": "response.reasoning_summary_part.added", "item_id": "rs_1", "part": {"type": "mystery", "text": ""}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning part unproven"))

    def test_summary_text_delta(self):
        d = _decision(_frame("response.reasoning_summary_text.delta", {"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "output_index": 0, "content_index": 0, "delta": "s"}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning text"))

    def test_summary_text_delta_nonstring_unproven(self):
        d = _decision(_frame("response.reasoning_summary_text.delta", {"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": 5}))
        self.assertEqual((d.kind, d.reason), ("boundary", "reasoning delta unproven"))


class TestEmbeddedCarriers(unittest.TestCase):
    """R6: a reasoning wrapper may not carry an answer payload.
    Unexpected structural carriers (response/item/part/output) must be
    proven reasoning-only or force the boundary (helper review 4)."""

    def _message_response(self):
        return {"object": "response", "status": "in_progress",
                "output": [{"type": "message", "content": [{"type": "output_text", "text": "hidden"}]}]}

    def test_delta_with_embedded_message_response_buffers(self):
        # Helper review 4 reproducer: a thinking delta embedding a
        # message response snapshot is not a proven prefix.
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "thinking", "response": self._message_response()}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded response unproven"))

    def test_delta_with_embedded_message_item_buffers(self):
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "thinking", "item": _MESSAGE_ITEM}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded item unproven"))

    def test_part_with_embedded_message_response_buffers(self):
        d = _decision(_frame("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1", "part": {"type": "summary_text", "text": ""}, "response": self._message_response()}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded response unproven"))

    def test_item_with_embedded_message_output_buffers(self):
        d = _decision(_frame("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": _REASONING_ITEM, "output": [_MESSAGE_ITEM]}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded output unproven"))

    def test_summary_delta_with_embedded_message_response_buffers(self):
        d = _decision(_frame("response.reasoning_summary_text.delta", {"type": "response.reasoning_summary_text.delta", "item_id": "rs_1", "delta": "s", "response": self._message_response()}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded response unproven"))

    def test_malformed_embedded_response_structure_buffers(self):
        # Wellformed JSON, malformed structure: output is not a list.
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "t", "response": {"object": "response", "output": "nope"}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded response unproven"))

    def test_embedded_terminal_status_response_buffers(self):
        # The same snapshot validator covers the embedded response
        # carrier: a terminal status forces the boundary even with an
        # empty output list.
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "t", "response": {"object": "response", "status": "completed", "output": []}}))
        self.assertEqual((d.kind, d.reason), ("boundary", "embedded response unproven"))

    def test_proven_reasoning_carriers_still_prefix(self):
        # A carrier that IS proven reasoning-only does not block.
        reasoning_response = {"object": "response", "status": "in_progress", "output": [dict(_REASONING_ITEM)]}
        d = _decision(_frame("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "delta": "t", "response": reasoning_response}))
        self.assertEqual((d.kind, d.reason), ("prefix", "reasoning text"))

    def test_prefix_value_is_parsed_object(self):
        # A prefix decision carries the very object the strict parser
        # yields for the same frame bytes, so the shared renderer's
        # re-render is byte-identical to the transform's own output.
        block = _frame("response.created", {"type": "response.created", "response": _RESPONSE})
        d = _decision(_frame("response.created", {"type": "response.created", "response": _RESPONSE}))
        self.assertEqual(d.value, {"type": "response.created", "response": _RESPONSE})
        parsed = proxy._event(block)
        self.assertEqual(d.value, parsed)


class TestObservedNativeFamily(unittest.TestCase):
    """Walk the observed qwen3.8-27b-rtx3090 family (v8): every frame up
    to and including the reasoning item's done event is a prefix; the
    first message frame is the boundary."""

    def test_family_prefix_then_boundary(self):
        reasoning_done_item = dict(_REASONING_ITEM, status="completed", summary=[],
                                   content=[{"type": "reasoning_text", "text": "t"}])
        family = [
            ("response.created", {"type": "response.created", "response": _RESPONSE}),
            ("response.in_progress", {"type": "response.in_progress", "response": _RESPONSE}),
            ("response.output_item.added", {"type": "response.output_item.added", "output_index": 0, "item": _REASONING_ITEM}),
            ("response.reasoning_part.added", {"type": "response.reasoning_part.added", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "reasoning_text", "text": ""}}),
            ("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "output_index": 0, "content_index": 0, "delta": "a"}),
            ("response.reasoning_text.delta", {"type": "response.reasoning_text.delta", "item_id": "rs_1", "output_index": 0, "content_index": 0, "delta": "b"}),
            ("response.reasoning_text.done", {"type": "response.reasoning_text.done", "item_id": "rs_1", "output_index": 0, "content_index": 0}),
            ("response.reasoning_part.done", {"type": "response.reasoning_part.done", "item_id": "rs_1", "output_index": 0, "content_index": 0, "part": {"type": "reasoning_text", "text": "ab"}}),
            ("response.output_item.done", {"type": "response.output_item.done", "output_index": 0, "item": reasoning_done_item}),
            ("response.output_item.added", {"type": "response.output_item.added", "output_index": 1, "item": _MESSAGE_ITEM}),
            ("response.content_part.added", {"type": "response.content_part.added", "item_id": "msg_1", "output_index": 1, "content_index": 0, "part": {"type": "output_text", "text": ""}}),
            ("response.output_text.delta", {"type": "response.output_text.delta", "item_id": "msg_1", "output_index": 1, "content_index": 0, "delta": "hi"}),
            ("response.output_text.done", {"type": "response.output_text.done", "item_id": "msg_1", "output_index": 1, "content_index": 0}),
            ("response.content_part.done", {"type": "response.content_part.done", "item_id": "msg_1", "output_index": 1, "content_index": 0, "part": {"type": "output_text", "text": "hi"}}),
            ("response.output_item.done", {"type": "response.output_item.done", "output_index": 1, "item": dict(_MESSAGE_ITEM, status="completed")}),
            ("response.completed", {"type": "response.completed", "response": {"object": "response", "status": "completed", "output": []}}),
        ]
        kinds = [_decision(_frame(label, obj)).kind for label, obj in family]
        self.assertEqual(kinds, ["prefix"] * 9 + ["boundary"] * 7)


class TestMalformedData(unittest.TestCase):
    """Malformed or structurally invalid data is a boundary, never an
    approved early prefix; malformed JSON and wellformed malformed
    structures are distinct denials."""

    def test_malformed_json(self):
        self.assertEqual((_decision("event: response.created\ndata: {not json\n\n").kind, _decision("event: response.created\ndata: {not json\n\n").reason), ("boundary", "malformed data"))

    def test_duplicate_keys(self):
        self.assertEqual((_decision('data: {"type":"response.created","type":"x"}\n\n').kind, _decision('data: {"type":"response.created","type":"x"}\n\n').reason), ("boundary", "malformed data"))

    def test_nonfinite_number(self):
        self.assertEqual((_decision('data: {"type":"response.created","response":{"nan":NaN}}\n\n').kind, _decision('data: {"type":"response.created","response":{"nan":NaN}}\n\n').reason), ("boundary", "malformed data"))

    def test_data_not_object(self):
        self.assertEqual((_decision("data: [1, 2]\n\n").kind, _decision("data: [1, 2]\n\n").reason), ("boundary", "malformed data"))

    def test_missing_type(self):
        self.assertEqual((_decision(_data_frame({"object": "response"})).kind, _decision(_data_frame({"object": "response"})).reason), ("boundary", "missing event type"))


class TestFrameHelpers(unittest.TestCase):
    """Frame splitting and incremental emission (one-way, in order)."""

    def test_split_crlf_and_lf(self):
        text = "a: 1\r\n\r\nb: 2\n\nc: 3\n\n"
        self.assertEqual(split_complete_frames(text), [("a: 1", "\r\n\r\n"), ("b: 2", "\n\n"), ("c: 3", "\n\n")])

    def test_split_unterminated_tail_dropped(self):
        self.assertEqual(split_complete_frames("a: 1\n\nb: 2"), [("a: 1", "\n\n")])

    def test_emit_incremental_each_frame_once(self):
        seen: list[tuple[str, str]] = []
        text = "a: 1\n\nb: 2\n\n"
        offset = emit_new_frames(text[:5], 0, lambda b, d: seen.append((b, d)))
        self.assertEqual((offset, seen), (0, []))
        offset = emit_new_frames(text[:8], offset, lambda b, d: seen.append((b, d)))
        self.assertEqual((offset, seen), (6, [("a: 1", "\n\n")]))
        offset = emit_new_frames(text, offset, lambda b, d: seen.append((b, d)))
        self.assertEqual((offset, seen), (12, [("a: 1", "\n\n"), ("b: 2", "\n\n")]))
        # A replay of the same prefix must emit nothing new.
        offset2 = emit_new_frames(text, offset, lambda b, d: seen.append((b, "replay")))
        self.assertEqual((offset2, seen[-1:]), (12, [("b: 2", "\n\n")]))


class TestParseErrorBoundary(unittest.TestCase):
    """Only the expected parse family buffers; control/programming
    errors propagate through owned handler cleanup unchanged."""

    def test_parse_value_error_buffers(self):
        def parser(_block: str) -> dict[str, object]:
            raise ValueError("injected parse failure")
        d = classify_prefix_frame("data: {}\n\n", parser)
        self.assertEqual((d.kind, d.reason), ("boundary", "malformed data"))

    def test_memory_error_propagates_same_object(self):
        injected = MemoryError("injected")

        def parser(_block: str) -> dict[str, object]:
            raise injected

        with self.assertRaises(MemoryError) as ctx:
            classify_prefix_frame("data: {}\n\n", parser)
        self.assertIs(ctx.exception, injected)



if __name__ == "__main__":
    unittest.main()
