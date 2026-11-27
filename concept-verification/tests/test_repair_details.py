"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE.

Focused units for the opt-in repair-details presentation module
(order 041-a, requirements 1-4): header parsing, the strict final-
applied-edit builder with its exact mapping proof, the bounded byte
cap, the reserved-field collision rule and a wire-contract check that
a selective standard-field parser ignores the extension.  Pure module
tests: no HTTP, no engine, no corpus, no model calls.  The selective
parser below is a wire-contract emulation - NOT installed-binary
Codex evidence (order 041-a, requirement 7).
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy  # noqa: E402
import repair_details as rd  # noqa: E402


class HeaderParsingTests(unittest.TestCase):
    def test_absent_means_off(self) -> None:
        self.assertEqual(rd.parse_details_values([]), 0)

    def test_single_zero_variants_mean_off(self) -> None:
        for value in ("0", "0 ", " 0", "  0  "):
            with self.subTest(value=value):
                self.assertEqual(rd.parse_details_values([value]), 0)

    def test_single_one_variants_mean_on(self) -> None:
        for value in ("1", "1 ", " 1", "  1  "):
            with self.subTest(value=value):
                self.assertEqual(rd.parse_details_values([value]), 1)

    def test_other_single_values_reject_fixed(self) -> None:
        for value in ("2", "", "  ", "1 1", "01", "10", "true", "yes", "x"):
            with self.subTest(value=value), self.assertRaises(rd.DetailsHeaderError):
                rd.parse_details_values([value])

    def test_duplicates_reject_even_when_equal_or_valid(self) -> None:
        for values in (["1", "1"], ["0", "0"], ["0", "1"], ["1", "0", "1"]):
            with self.subTest(values=values), self.assertRaises(rd.DetailsHeaderError):
                rd.parse_details_values(values)

    def test_rejection_is_fixed_and_content_free(self) -> None:
        secret = "super-secret-value"
        with self.assertRaises(rd.DetailsHeaderError) as ctx:
            rd.parse_details_values([secret])
        self.assertEqual(ctx.exception.reason, rd.REPAIR_DETAILS_HEADER_INVALID)
        self.assertEqual(str(ctx.exception), rd.REPAIR_DETAILS_HEADER_INVALID)
        self.assertNotIn(secret, str(ctx.exception))

    def test_collection_is_case_insensitive_and_keeps_duplicates(self) -> None:
        pairs = [
            ("X-LLM-Language-Repair-Details", "1"),
            ("Host", "x"),
            ("x-llm-language-repair-details", "2"),
            ("X-LLM-LANGUAGE-REPAIR-DETAILS", "3"),
        ]
        self.assertEqual(
            rd.details_header_values(pairs), ["1", "2", "3"]
        )

    def test_remove_strips_every_case_variant_and_preserves_rest(self) -> None:
        pairs = [
            ("X-LLM-Language-Repair-Details", "1"),
            ("Authorization", "Bearer t"),
            ("x-llm-language-repair-details", "9"),
            ("Host", "example"),
        ]
        kept = rd.remove_details_header(pairs)
        self.assertEqual(kept, [("Authorization", "Bearer t"), ("Host", "example")])
        # The input sequence is never mutated.
        self.assertEqual(len(pairs), 4)


class BuilderCompleteTests(unittest.TestCase):
    def _complete(self, original, edits, delivered, oi=1, ci=0):
        return rd.build_complete(original, edits, delivered, oi, ci)

    def test_exact_key_shape(self) -> None:
        extension = self._complete("abc def", [(4, 7, "xyz")], "abc xyz")
        self.assertEqual(
            set(extension),
            {
                "version",
                "status",
                "offset_unit",
                "output_index",
                "content_index",
                "original_text",
                "edits",
            },
        )
        self.assertEqual(extension["version"], 1)
        self.assertEqual(extension["status"], rd.STATUS_COMPLETE)
        self.assertEqual(extension["offset_unit"], "unicode_codepoints")
        self.assertEqual(extension["output_index"], 1)
        self.assertEqual(extension["content_index"], 0)
        self.assertEqual(extension["original_text"], "abc def")
        self.assertEqual(
            extension["edits"],
            [
                {
                    "original_start": 4,
                    "original_end": 7,
                    "repaired_start": 4,
                    "repaired_end": 7,
                    "replacement": "xyz",
                }
            ],
        )
        for edit in extension["edits"]:
            self.assertEqual(
                set(edit),
                {
                    "original_start",
                    "original_end",
                    "repaired_start",
                    "repaired_end",
                    "replacement",
                },
            )

    def test_no_correction_is_exact_original_plus_empty_edits(self) -> None:
        extension = self._complete("hello world", [], "hello world", oi=0, ci=0)
        self.assertEqual(extension["status"], rd.STATUS_COMPLETE)
        self.assertEqual(extension["original_text"], "hello world")
        self.assertEqual(extension["edits"], [])

    def test_multiple_length_changing_edits_rebased_arithmetically(self) -> None:
        # "a bb ccc dddd": b@2-3, c@5-7.  First edit 2cp->1cp (shift -1),
        # second edit "cc"->"cd" re-based to delivered offset 4.
        extension = self._complete(
            "a bb ccc dddd", [(2, 4, "b"), (5, 8, "cd")], "a b cd dddd"
        )
        self.assertEqual(
            extension["edits"],
            [
                {
                    "original_start": 2,
                    "original_end": 4,
                    "repaired_start": 2,
                    "repaired_end": 3,
                    "replacement": "b",
                },
                {
                    "original_start": 5,
                    "original_end": 8,
                    "repaired_start": 4,
                    "repaired_end": 6,
                    "replacement": "cd",
                },
            ],
        )

    def test_emoji_code_point_coordinates_not_bytes(self) -> None:
        # U+1F600 is ONE code point (4 UTF-8 bytes): the coordinates must
        # be code-point based.
        original = "pre\U0001F600post"
        extension = self._complete(original, [(3, 4, "x")], "prexpost")
        self.assertEqual(
            extension["edits"][0],
            {
                "original_start": 3,
                "original_end": 4,
                "repaired_start": 3,
                "repaired_end": 4,
                "replacement": "x",
            },
        )
        # A multi-code-point span replaced by one supplementary character.
        extension2 = self._complete(original, [(0, 3, "\U0001F600")], "\U0001F600\U0001F600post")
        self.assertEqual(
            extension2["edits"][0],
            {
                "original_start": 0,
                "original_end": 3,
                "repaired_start": 0,
                "repaired_end": 1,
                "replacement": "\U0001F600",
            },
        )

    def test_combining_marks_are_separate_code_points(self) -> None:
        # "e" + U+0301 combining acute = TWO code points.
        original = "cafe\u0301 x"
        extension = self._complete(original, [(3, 5, "\u00e9")], "caf\u00e9 x")
        self.assertEqual(
            extension["edits"][0],
            {
                "original_start": 3,
                "original_end": 5,
                "repaired_start": 3,
                "repaired_end": 4,
                "replacement": "\u00e9",
            },
        )

    def test_cRLF_are_single_code_points(self) -> None:
        original = "line1\r\nline2"
        extension = self._complete(original, [(5, 7, " ")], "line1 line2")
        self.assertEqual(
            extension["edits"][0],
            {
                "original_start": 5,
                "original_end": 7,
                "repaired_start": 5,
                "repaired_end": 6,
                "replacement": " ",
            },
        )

    def test_repeated_substrings_map_the_proved_spans_only(self) -> None:
        # Three identical "abc" occurrences: only the proved spans may be
        # reported, and the composition pins the exact delivered answer.
        extension = self._complete(
            "abc abc abc", [(0, 3, "AAA"), (8, 11, "BBB")], "AAA abc BBB"
        )
        self.assertEqual(
            extension["edits"],
            [
                {
                    "original_start": 0,
                    "original_end": 3,
                    "repaired_start": 0,
                    "repaired_end": 3,
                    "replacement": "AAA",
                },
                {
                    "original_start": 8,
                    "original_end": 11,
                    "repaired_start": 8,
                    "repaired_end": 11,
                    "replacement": "BBB",
                },
            ],
        )

    def test_adjacent_spans_are_disjoint_and_allowed(self) -> None:
        extension = self._complete("abcd", [(0, 2, "x"), (2, 4, "y")], "xy")
        self.assertEqual(len(extension["edits"]), 2)


class BuilderRejectionTests(unittest.TestCase):
    def _reason(self, original, edits, delivered):
        result = rd.build_complete(original, edits, delivered, 0, 0)
        self.assertEqual(result["status"], rd.STATUS_UNAVAILABLE)
        self.assertNotIn("original_text", result)
        self.assertNotIn("edits", result)
        return result["reason"]

    def test_unsorted_spans_unavailable(self) -> None:
        self.assertEqual(
            self._reason("abcd efgh", [(4, 8, "y"), (0, 4, "x")], "xy"),
            rd.EDIT_MAPPING_UNAVAILABLE,
        )

    def test_overlapping_spans_unavailable(self) -> None:
        self.assertEqual(
            self._reason("abcdef", [(0, 4, "x"), (2, 6, "y")], "whatever"),
            rd.EDIT_MAPPING_UNAVAILABLE,
        )

    def test_degenerate_and_out_of_range_spans_unavailable(self) -> None:
        self.assertEqual(
            self._reason("abc", [(1, 1, "x")], "axc"), rd.EDIT_MAPPING_UNAVAILABLE
        )
        self.assertEqual(
            self._reason("abc", [(0, 99, "x")], "x"), rd.EDIT_MAPPING_UNAVAILABLE
        )
        self.assertEqual(
            self._reason("abc", [(-1, 3, "x")], "x"), rd.EDIT_MAPPING_UNAVAILABLE
        )

    def test_identity_replacement_is_not_an_edit(self) -> None:
        self.assertEqual(
            self._reason("abc", [(0, 3, "abc")], "abc"),
            rd.EDIT_MAPPING_UNAVAILABLE,
        )

    def test_composition_must_equal_delivered(self) -> None:
        self.assertEqual(
            self._reason("abc def", [(4, 7, "xyz")], "abc NOPE"),
            rd.EDIT_MAPPING_UNAVAILABLE,
        )

    def test_type_strictness(self) -> None:
        # bool is not an int coordinate; strings are not indices.
        self.assertEqual(
            rd.build_complete("abc", [(0, 3, "x")], "xxx", True, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete("abc", [(0, 3, "x")], "xxx", 0, -1)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete("abc", [(0, 3, "x")], "xxx", "0", 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        # Edits are exact 3-tuples; lists, non-str replacements and None
        # items are not accepted.
        self.assertEqual(
            rd.build_complete("abc", [[0, 3, "x"]], "xxx", 0, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete("abc", [(0, 3, 5)], "xxx", 0, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete("abc", [None], "xxx", 0, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete(123, [], "abc", 0, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )
        self.assertEqual(
            rd.build_complete("abc", None, "abc", 0, 0)["reason"],
            rd.EDIT_MAPPING_UNAVAILABLE,
        )


class BoundedSizeTests(unittest.TestCase):
    def test_unavailable_object_shape_is_small_and_fixed(self) -> None:
        for reason in (
            rd.EDIT_MAPPING_UNAVAILABLE,
            rd.DETAILS_TOO_LARGE,
            rd.UPSTREAM_FIELD_COLLISION,
        ):
            with self.subTest(reason=reason):
                self.assertEqual(
                    rd.unavailable(reason),
                    {
                        "version": 1,
                        "status": "unavailable",
                        "reason": reason,
                    },
                )
                self.assertLess(len(json.dumps(rd.unavailable(reason)).encode()), 256)

    def _no_edit_result(self, text: str):
        return rd.build_complete(text, [], text, 0, 0)

    def test_provably_fitting_ascii_is_never_rejected(self) -> None:
        # 100k ASCII original serializes to ~100133B worst mode:
        # complete, and the exact worst-mode bytes stay under the cap.
        original = "a" * 100000
        extension = self._no_edit_result(original)
        self.assertEqual(extension["status"], rd.STATUS_COMPLETE)
        self.assertEqual(
            len(rd._worst_case_encoding(extension)),
            len(json.dumps(extension, ensure_ascii=True, separators=(",", ":")).encode("ascii")),
        )
        self.assertLess(len(rd._worst_case_encoding(extension)), rd.MAX_DETAILS_BYTES)

    def test_ascii_boundary_is_exact_not_a_loose_ceiling(self) -> None:
        # ASCII serializes to one byte per code point: the cap is
        # decided by the ACTUAL serialized bytes (1,048,443 code points
        # serialize to exactly 1,048,576B worst mode; one more does
        # not), never by a structural estimate.
        ok = self._no_edit_result("a" * 1048443)
        self.assertEqual(ok["status"], rd.STATUS_COMPLETE)
        self.assertEqual(len(rd._worst_case_encoding(ok)), rd.MAX_DETAILS_BYTES)
        too_large = self._no_edit_result("a" * 1048444)
        self.assertEqual(too_large["status"], rd.STATUS_UNAVAILABLE)
        self.assertEqual(too_large["reason"], rd.DETAILS_TOO_LARGE)
        self.assertNotIn("original_text", too_large)

    def test_supplementary_worst_case_boundary(self) -> None:
        # Twelve escape bytes per supplementary code point in the outer
        # serializer's fallback mode, decided by actual serialization.
        ok = self._no_edit_result("\U0001F600" * 87294)
        self.assertEqual(ok["status"], rd.STATUS_COMPLETE)
        self.assertEqual(len(rd._worst_case_encoding(ok)), 1047661)
        too_large = self._no_edit_result("\U0001F600" * 87400)
        self.assertEqual(too_large["reason"], rd.DETAILS_TOO_LARGE)

    def test_actually_oversize_non_bmp_is_rejected(self) -> None:
        # 100k supplementary code points actually serialize to
        # ~1.2MB: over the cap in the worst mode.
        too_large = self._no_edit_result("\U0001F600" * 100000)
        self.assertEqual(too_large["status"], rd.STATUS_UNAVAILABLE)
        self.assertEqual(too_large["reason"], rd.DETAILS_TOO_LARGE)

    def test_quote_and_backslash_doubling_is_accounted_exactly(self) -> None:
        # Quotes escape to two bytes in the worst mode: the preflight
        # must not undercount them.
        original = "\"" * 10000 + "a" * 1000000
        ok = self._no_edit_result(original)
        self.assertEqual(ok["status"], rd.STATUS_COMPLETE)
        original = "\"" * 100000 + "a" * 900000
        too_large = self._no_edit_result(original)
        self.assertEqual(too_large["reason"], rd.DETAILS_TOO_LARGE)

    def test_large_replacement_is_bounded(self) -> None:
        # ASCII replacement boundary by actual serialization: 1,048,300
        # replacement code points serialize to 1,048,531B worst mode
        # (fits); 1,048,400 does not.
        ok = rd.build_complete("ab", [(0, 2, "x" * 1048300)], "x" * 1048300, 0, 0)
        self.assertEqual(ok["status"], rd.STATUS_COMPLETE)
        self.assertEqual(len(rd._worst_case_encoding(ok)), 1048531)
        too_large = rd.build_complete(
            "ab", [(0, 2, "x" * 1048400)], "x" * 1048400, 0, 0
        )
        self.assertEqual(too_large["reason"], rd.DETAILS_TOO_LARGE)


class ReservedFieldTests(unittest.TestCase):
    ENVELOPE = {
        "id": "resp_1",
        "object": "response",
        "status": "completed",
        "output": [],
        "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
    }

    def test_attach_adds_extension_to_clean_root(self) -> None:
        envelope = dict(self.ENVELOPE)
        extension = rd.build_complete("a", [], "a", 0, 0)
        rd.attach(envelope, extension)
        self.assertEqual(envelope[rd.FIELD_NAME], extension)
        self.assertEqual(envelope["usage"], self.ENVELOPE["usage"])

    def test_upstream_root_field_is_replaced_by_collision(self) -> None:
        envelope = dict(self.ENVELOPE)
        envelope[rd.FIELD_NAME] = {"version": 99, "status": "sneaky"}
        rd.attach(envelope, rd.build_complete("a", [], "a", 0, 0))
        self.assertEqual(
            envelope[rd.FIELD_NAME],
            {"version": 1, "status": "unavailable", "reason": rd.UPSTREAM_FIELD_COLLISION},
        )

    def test_same_named_nested_user_data_is_never_touched(self) -> None:
        nested = {"user": {"llm_language_repair": "user-data"}}
        envelope = dict(self.ENVELOPE)
        envelope["output"] = [{"type": "message", "metadata": nested}]
        rd.attach(envelope, rd.build_complete("a", [], "a", 0, 0))
        self.assertEqual(nested, {"user": {"llm_language_repair": "user-data"}})
        self.assertEqual(envelope["output"][0]["metadata"], nested)


class WireContractTests(unittest.TestCase):
    """Selective standard-field parse (wire contract).

    Emulates a pinned ResponsesStreamEvent-style selective
    deserialization: known completed-response fields are read, unknown
    JSON keys are ignored.  This proves the standard view is unchanged
    by the extension; it is NOT installed-binary Codex evidence
    (order 041-a, requirement 7).
    """

    KNOWN = {
        "id",
        "object",
        "status",
        "output",
        "usage",
        "usage_metadata",
        "end_turn",
        "error",
        "created_at",
        "model",
    }

    def _selective(self, response: dict) -> dict:
        return {key: response[key] for key in self.KNOWN if key in response}

    def _envelope(self, repaired_text: str) -> dict:
        return {
            "id": "resp_42",
            "object": "response",
            "status": "completed",
            "model": "synthetic-main",
            "output": [
                {
                    "type": "reasoning",
                    "id": "rs_1",
                    "status": "completed",
                    "summary": [{"type": "summary_text", "text": "think"}],
                },
                {
                    "type": "message",
                    "id": "msg_1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {"type": "output_text", "text": repaired_text}
                    ],
                },
            ],
            "usage": {"input_tokens": 10, "output_tokens": 20, "total_tokens": 30},
            "end_turn": True,
        }

    def test_json_standard_view_unchanged_by_extension(self) -> None:
        repaired = "abc xyz"
        baseline = self._envelope(repaired)
        extended = self._envelope(repaired)
        extension = rd.build_complete("abc def", [(4, 7, "xyz")], repaired, 1, 0)
        rd.attach(extended, extension)
        self.assertEqual(self._selective(baseline), self._selective(extended))
        # The raw envelope keeps every standard field and the answer.
        self.assertEqual(extended["output"], baseline["output"])
        self.assertEqual(extended["usage"], baseline["usage"])
        self.assertEqual(extended[rd.FIELD_NAME], extension)
        # Unknown-key tolerance: the selective parser never reads it.
        self.assertNotIn(rd.FIELD_NAME, self.KNOWN)

    def test_sse_terminal_standard_view_unchanged(self) -> None:
        repaired = "line1 line2"
        event = {
            "type": "response.completed",
            "response": self._envelope(repaired),
        }
        baseline_view = self._selective(event["response"])
        extension = rd.build_complete(
            "line1\r\nline2", [(5, 7, " ")], repaired, 1, 0
        )
        rd.attach(event["response"], extension)
        self.assertEqual(baseline_view, self._selective(event["response"]))
        # No new event kinds: the terminal event type is unchanged.
        self.assertEqual(event["type"], "response.completed")


class TransformFlagTests(unittest.TestCase):
    """Metadata-only presentation must not masquerade as an answer-text
    change: the transform's repaired flag reports the ACTUAL text edit
    only (order 041-a R5 review), while the delivered bytes still carry
    the extension and the cache/length decisions key off actual bytes.
    Pure transform units: fake repair/hook, no engine, no HTTP."""

    @staticmethod
    def _hook(original, delivered, oi, ci):
        # The two pure-transform fixtures below, with their proved
        # final edits (no edit when the answer is unchanged).
        if original == "abc def" and delivered == "abc xyz":
            edits = [(4, 7, "xyz")]
        elif original == "line1\r\nline2" and delivered == "line1 line2":
            edits = [(5, 7, " ")]
        else:
            edits = []
        return rd.build_complete(original, edits, delivered, oi, ci)

    @staticmethod
    def _repair(replacement=None, span=(4, 7)):
        def _repair(t):
            if replacement is None:
                return t
            return t[: span[0]] + replacement + t[span[1]:]
        return _repair

    def _sse_raw(self, answer: str) -> bytes:
        def ev(name: str, payload: dict) -> str:
            return f"event: {name}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
        msg = {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": answer}],
        }
        # A reasoning item precedes the answer: the extension must
        # locate the actual answer part (output_index 1), not the
        # reasoning item.
        reasoning = {
            "type": "reasoning",
            "id": "rs_1",
            "status": "completed",
            "summary": [{"type": "summary_text", "text": "think"}],
        }
        return (
            ev("response.created", {"type": "response.created",
                                    "response": {"id": "resp_1", "status": "created"}})
            + ev("response.output_text.delta",
                 {"type": "response.output_text.delta", "item_id": "msg_1",
                  "output_index": 1, "content_index": 0, "delta": answer})
            + ev("response.output_text.done",
                 {"type": "response.output_text.done", "item_id": "msg_1",
                  "output_index": 1, "content_index": 0, "text": answer})
            + ev("response.completed",
                 {"type": "response.completed", "response": {
                     "id": "resp_1", "object": "response", "status": "completed",
                     "output": [reasoning, msg],
                     "usage": {"input_tokens": 1, "output_tokens": 2,
                               "total_tokens": 3}}})
        ).encode()

    def test_sse_metadata_only_flag_false_extension_present(self) -> None:
        raw = self._sse_raw("abc def")
        out, changed = proxy.transform_sse(raw, self._repair(), self._hook)
        self.assertFalse(changed)
        text = out.decode()
        self.assertEqual(text.count("llm_language_repair"), 1)
        terminal = text.split("event: response.completed", 1)[1]
        ext = json.loads(terminal.split("data: ", 1)[1])["response"]["llm_language_repair"]
        self.assertEqual(ext["status"], "complete")
        self.assertEqual(ext["edits"], [])
        self.assertEqual(ext["original_text"], "abc def")
        # The wire bytes DID change (extension added) even though the
        # flag is False: byte comparison, not the flag, decides the
        # cache-validator/length behavior.
        self.assertNotEqual(out, raw)

    def test_sse_text_edit_flag_true(self) -> None:
        raw = self._sse_raw("abc def")
        out, changed = proxy.transform_sse(
            raw, self._repair(replacement="xyz"), self._hook
        )
        self.assertTrue(changed)
        text = out.decode()
        ext = json.loads(text.split("event: response.completed", 1)[1].split("data: ", 1)[1])
        self.assertEqual(ext["response"]["output"][1]["content"][0]["text"], "abc xyz")
        self.assertEqual(len(ext["response"]["llm_language_repair"]["edits"]), 1)

    def test_json_metadata_only_flag_false_extension_present(self) -> None:
        envelope = {
            "id": "resp_1", "object": "response", "status": "completed",
            "output": [{"type": "message", "id": "msg_1", "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "abc def"}]}],
        }
        raw = json.dumps(envelope).encode()
        out, changed = proxy.transform_json(
            raw, self._repair(), self._hook
        )
        self.assertFalse(changed)
        body = json.loads(out)
        self.assertEqual(body["output"][0]["content"][0]["text"], "abc def")
        self.assertEqual(body["llm_language_repair"]["status"], "complete")
        self.assertEqual(body["llm_language_repair"]["edits"], [])
        self.assertNotEqual(out, raw)

    def test_json_off_flag_false_bytes_identical(self) -> None:
        envelope = {
            "id": "resp_1", "object": "response", "status": "completed",
            "output": [{"type": "message", "id": "msg_1", "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "abc def"}]}],
        }
        raw = json.dumps(envelope).encode()
        out, changed = proxy.transform_json(raw, self._repair())
        self.assertFalse(changed)
        self.assertEqual(out, raw)


if __name__ == "__main__":
    unittest.main()
