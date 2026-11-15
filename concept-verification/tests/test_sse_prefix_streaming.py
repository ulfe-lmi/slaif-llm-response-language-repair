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
    classify_prefix_frame,
    emit_new_frames,
    split_complete_frames,
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
