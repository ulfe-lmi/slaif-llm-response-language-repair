"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE. Proxy."""

from __future__ import annotations

import contextlib
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import sys
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Protocol, TypedDict, cast
from urllib.parse import urlparse

import analysis_policy
import capture_transport
import client_transport
import composed_validation
import delivery_transport
import header_transport
import passthrough
import repair_details
import request_admission
import request_lifecycle
import sse_streaming
import upstream_setup
from batch_review import (
    BOUND_REVIEW_REASONS,
    REASON_FAILED,
    REASON_PREP_INVALID,
    REASON_PROMPT_OVERSIZE,
    REASON_REQUEST_OVERSIZE,
    BatchProposal,
    BatchResult,
    BatchTarget,
    BatchUsage,
    prepare_targets,
)
from config import (
    CLIENT_DELIVERY_TIMEOUT_SECONDS,
    CLIENT_REQUEST_TIMEOUT_SECONDS,
    MAIN_BODY_MAX_BYTES,
    MAIN_HEADER_TIMEOUT_SECONDS,
    MAIN_SETUP_TIMEOUT_SECONDS,
    canonical_bytes,
)
from detector import Candidate, tokenize
from evidence_budget import validate_default_evidence_policy
from pandoc_protection import (
    PandocSpec,
    PandocStartupError,
    check_pandoc,
    compose_protection,
    protect_with_pandoc,
    resolve_pandoc_spec,
)
from qwen_client import Proposal, ResponsesReviewer, responses_path
from repair import apply_edits
from responses_contract import (
    RequestContractError,
    aligned_metadata_clean,
    remove_defined_logprobs,
    validate_completed_envelope,
    validate_request,
)
from review_policy import DEFAULT_POLICY, ReviewPolicy
from review_policy import (
    ReviewPolicyError,
    load_review_policy,
    validate_reviewer_timeout,
)
from runtime_acceptance import accept
from runtime_corpus import RuntimeCorpus, RuntimeCorpusError
from runtime_detection import detect

# Native accepted-object class captured at import time.  BaseServer
# types the accepted request as the TCP/UDP union; this TCP
# HTTPServer accept boundary always yields the socket member, and
# the captured reference keeps that narrowing sound for the whole
# process even when a frozen native supervised-test child script
# legitimately reassigns the mutable socket.socket module attribute
# while the accept loop is running (order 026-a, requirements 2
# and 3).  It is not a new protocol behavior and adds no UDP
# support or claim.
_SOCKET_CLASS = socket.socket


class ProxyError(ValueError):
    pass


class Reviewer(Protocol):
    """Structural reviewer interface: the engine only calls ``review_batch``.

    The runtime RepairEngine never falls back to single-target calls; the
    historical single-target prompt/API stays in ``qwen_client`` and the
    frozen evaluator for research use only (order 014-a, requirement 1).
    """

    def review_batch(
        self, sentence: str, targets: tuple[BatchTarget, ...]
    ) -> BatchResult: ...


def _split_sse(raw: bytes) -> list[tuple[str, str]]:
    text = raw.decode("utf-8")
    result: list[tuple[str, str]] = []
    cursor = 0
    for match in re.finditer(r"\r\n\r\n|\n\n", text):
        result.append((text[cursor : match.start()], match.group(0)))
        cursor = match.end()
    if cursor < len(text):
        result.append((text[cursor:], ""))
    return result


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            # Ambiguous duplicate keys never reach repair or rendering.
            raise ProxyError("upstream event contains duplicate keys")
        value[key] = item
    return value


def _strict_constant(_name: str) -> object:
    raise ProxyError("upstream event contains a nonfinite number")


def _strict_float(text: str) -> float:
    number = float(text)
    if not math.isfinite(number):
        raise ProxyError("upstream event contains a nonfinite number")
    return number


def _strict_json_object(raw: str) -> dict[str, object]:
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_strict_pairs,
            parse_constant=_strict_constant,
            parse_float=_strict_float,
        )
    except ProxyError:
        raise
    except json.JSONDecodeError as exc:
        raise ProxyError("upstream event is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ProxyError("upstream event is not an object")
    return value


def _event(block: str) -> dict[str, object] | None:
    data_lines = [line[5:] for line in block.splitlines() if line.startswith("data:")]
    if not data_lines or "".join(data_lines).strip() == "[DONE]":
        return None
    return _strict_json_object("\n".join(data_lines))


# Byte-level SSE frame delimiters (the byte counterpart of the strict
# runtime parser's \r\n\r\n|\n\n frame split, order 038-a R6): a frame
# is complete only at a full delimiter, so the bypass filter can frame
# original bytes before any decoding and never partially decode UTF-8.
_SSE_FRAME_DELIMITERS = re.compile(rb"\r\n\r\n|\n\n")


def _events(raw: bytes) -> list[tuple[str, dict[str, object]]]:
    result: list[tuple[str, dict[str, object]]] = []
    for block, _delimiter in _split_sse(raw):
        value = _event(block)
        if value is not None:
            result.append((block, value))
    return result


def _bounded_read(
    response: http.client.HTTPResponse,
    maximum: int,
    timeout: float,
    *,
    require_complete_sse: bool = False,
    cancelled: Callable[[], bool] | None = None,
) -> bytes:
    """Read within byte/time limits, stopping at a complete SSE event.

    One finite positive monotonic body deadline is anchored at
    collection entry and governs declared Content-Length, close-delimited
    and chunked delivery (order 017-a): byte arrival never resets it and
    every blocking wait is bounded by the remaining time.  A wire chunk
    may end inside a UTF-8 code point: the live tail is carried until
    the body is known complete, where a truncated or invalid encoding is
    a bounded main failure.  Readiness spans two layers: header parsing
    (``getresponse``) may already have over-read body bytes into the
    buffered reader, which the collector drains first (never entering
    the stdlib chunk decoder or its connection-closing read paths), so a
    fully delimited terminal event already in the buffer terminates the
    read without EOF even while the HTTP/1.1 connection stays open.
    """
    try:
        return capture_transport.read_bounded_body(
            response,
            maximum,
            timeout,
            require_complete_sse=require_complete_sse,
            event=_event,
            cancelled=cancelled,
        )
    except capture_transport.CaptureError as exc:
        raise ProxyError(str(exc)) from exc


def _render_event(block: str, value: dict[str, object]) -> str:
    lines = block.splitlines()
    data_index = next((index for index, line in enumerate(lines) if line.startswith("data:")), None)
    if data_index is None:
        return block
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    try:
        payload.encode("utf-8")
    except UnicodeEncodeError:
        # Lone surrogate in a value: fall back to ASCII-safe encoding
        # (order 035-c R6).  Parsed value is identical.
        payload = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    lines[data_index] = "data: " + payload
    lines = [
        line
        for index, line in enumerate(lines)
        if index == data_index or not line.startswith("data:")
    ]
    return "\n".join(lines)


# Sentinel for a stream-event ownership position that is absent (distinct
# from a declared null, which is inconsistent and abstains).
_UNSET: object = object()


def transform_sse_blocks(
    raw: bytes,
    repair: Callable[[str], str],
    details: Callable[[str, str, int | None, int | None], dict[str, object]]
    | None = None,
) -> tuple[list[str], bool]:
    """Repair one unambiguously identified assistant text answer only.

    Ownership is prefetched across the whole completed upstream SSE before
    any repair call: the delta layer (stream item ids and the positions the
    stream events themselves declare) and every terminal representation
    that carries the answer (``output_text.done`` events, ``content_part.done``
    output_text parts, message-typed ``output_item.done`` items, and the
    ``output_text`` part in the completed ``response.output``).  Several
    assistant text answers/items/content parts, a completed envelope that
    does not exactly confirm the single delta answer, stream-declared
    indices that contradict the completed list positions, a terminal copy
    that cannot be proved consistent (different text, role, status,
    identity, index, content cardinality/kinds, a duplicated terminal
    representation, or a missing/null/non-string id), or otherwise
    ambiguous target ownership bypass repair unchanged with zero review
    calls (order 014-a requirement 8; order 014-b requirements 2-4).

    The editable fields are collected as explicit event references during
    the preflight, and the renderer mutates only those: it never iterates
    the output_text nodes of a matching-id object after repair, never
    recurses into metadata, and never matches substrings or identical
    values (no ID coercion, no value-based recovery).  Every
    ``output_text.delta`` payload must be a string (an empty string is a
    valid no-op): a delta event whose payload is missing or non-string is
    a malformed answer event and abstains raw unchanged with zero calls.
    Initial ``added`` events are supported only as empty placeholders:
    their ownership (typed confirmed id) and, for message items, the
    assistant role are validated, their protected content is never
    rewritten, and any non-empty initial content - which cannot be proved
    safe - abstains raw unchanged with zero calls.  The confirming
    completed event must be an explicitly completed, error-free Responses
    object and the final message must carry exactly
    ``status == "completed"``; non-empty (or non-list) aligned
    annotations/logprobs on any answer-bearing representation make the
    whole stream ineligible before dispatch (order 015-a, requirements 4
    and 5).  When the review accepts no text edit the raw bytes are
    returned unchanged (order 014-a requirement 7).

    Returns the per-block rendered strings (each block plus its
    delimiter, in stream order) and whether the answer was repaired;
    the public transform_sse joins and encodes them, so the two never
    diverge and the early-reasoning streaming prefix can validate the
    blocks it already sent against this exact per-block output
    (order 038-a R1).
    """
    blocks = _split_sse(raw)
    parsed: list[dict[str, object] | None] = [_event(block) for block, _ in blocks]

    # Order 035-c: always remove defined logprobs from all SSE event
    # locations before any eligibility/repair logic (by key presence,
    # regardless of value/type; no recursive user-data scan).  The
    # shared sse_streaming helper applies exactly the defined
    # locations, so the whole-body transform and the streaming prefix
    # path strip identically frame by frame (order 038-a R1).
    logprobs_modified: set[int] = {
        _idx
        for _idx, _val in enumerate(parsed)
        if _val is not None and sse_streaming.strip_event_logprobs(_val)
    }

    def _bypass_parts() -> list[str]:
        """Per-block raw, or re-render only logprobs-removed events."""
        if not logprobs_modified:
            return [_blk + _delim for _blk, _delim in blocks]
        _parts: list[str] = []
        for (_blk, _delim), _v, _i in zip(blocks, parsed, range(len(blocks)), strict=True):
            if _v is None:
                _parts.append(_blk + _delim)
                continue
            if _i in logprobs_modified:
                _blk = _render_event(_blk, _v)
            _parts.append(_blk + _delim)
        return _parts

    streams: dict[str, list[str]] = {}
    # Positions declared by the stream events themselves: text delta/done
    # events and content-part added/done events carry ``item_id`` plus
    # ``output_index``/``content_index`` in the documented Responses
    # stream.  Those declared positions are authoritative for ownership;
    # optional indices on the final message/part are only an additional
    # consistency constraint, never a substitute.
    declared: dict[str, list[tuple[object, object]]] = {}
    # A delta/done event whose ``item_id`` is missing, non-string or empty
    # does not identify an answer, and an answer-shaped content part or
    # message item without a typed nonempty id is an unowned terminal copy.
    # Such a stream carries ambiguous ownership and abstains instead of
    # being coerced to an invented id.
    ownership_invalid = False
    # Every answer-related stream event (text delta/done, output-text
    # content-part added/done, message-typed output-item added/done) must
    # belong to the single confirmed answer; any other id abstains.
    answer_event_ids: set[str] = set()
    for value in parsed:
        if value is None:
            continue
        event_type = value.get("type")
        if event_type in (
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.added",
            "response.content_part.done",
        ):
            stream_id = value.get("item_id")
            if type(stream_id) is not str or not stream_id:
                if event_type in ("response.output_text.delta", "response.output_text.done"):
                    ownership_invalid = True
                elif (
                    isinstance((stream_part := value.get("part")), dict)
                    and stream_part.get("type") == "output_text"
                ):
                    # An answer-shaped content part that cannot identify
                    # its owner is an unowned terminal copy.
                    ownership_invalid = True
                continue
            part = value.get("part")
            answer_part = (
                event_type.startswith("response.content_part.")
                and isinstance(part, dict)
                and part.get("type") == "output_text"
            )
            if event_type.startswith("response.output_text.") or answer_part:
                answer_event_ids.add(stream_id)
            if "output_index" in value or "content_index" in value:
                declared.setdefault(stream_id, []).append(
                    (value.get("output_index", _UNSET), value.get("content_index", _UNSET))
                )
        elif event_type in ("response.output_item.added", "response.output_item.done"):
            item_obj = value.get("item")
            if isinstance(item_obj, dict):
                obj_id = item_obj.get("id")
                if item_obj.get("type") == "message":
                    if type(obj_id) is not str or not obj_id:
                        # An answer-shaped message item without a typed id
                        # is an unowned terminal representation.
                        ownership_invalid = True
                        continue
                    answer_event_ids.add(obj_id)
                if type(obj_id) is str and obj_id and "output_index" in value:
                    declared.setdefault(obj_id, []).append((value["output_index"], _UNSET))
        if event_type == "response.output_text.delta":
            delta = value.get("delta")
            if type(delta) is not str:
                # A delta event whose payload is missing or non-string is a
                # malformed answer event: the stream carries ambiguous
                # content and abstains instead of being partially collected.
                ownership_invalid = True
                continue
            stream_id = value.get("item_id")
            if type(stream_id) is str and stream_id:
                streams.setdefault(stream_id, []).append(delta)
    if ownership_invalid:
        # Answer-related events with missing, non-string or empty ids (or
        # unowned answer-shaped terminal copies) carry ambiguous ownership:
        # bypass unchanged with zero review calls.
        return _bypass_parts(), False
    # Completed-envelope answer units: every output_text part of any message
    # item in any response.output, with list ownership positions.
    # Output messages are identified by ``id`` (delta/done events carry
    # ``item_id``); ``output_index``/``content_index`` are optional but must
    # be consistent when present.
    units: list[tuple[int, int, dict[str, object], int, dict[str, object]]] = []
    # The completed event that owns a confirming unit must itself be an
    # explicitly completed, error-free Responses object; a missing/null
    # status or a non-null error never confirms an answer (order 015-a,
    # requirements 4 and 5).
    completed_eligible: dict[int, bool] = {}
    for index, value in enumerate(parsed):
        if value is None or value.get("type") != "response.completed":
            continue
        response = value.get("response")
        if not isinstance(response, dict):
            completed_eligible[index] = False
            continue
        # The completed layer must match the shared JSON profile exactly
        # (order 015-a, requirements 4 and 5): the full envelope
        # validation - completed object, absent/null error, stateless
        # declarations, at most one bounded reasoning item preceding
        # exactly one completed assistant message with a typed nonempty
        # id and exactly one plain output_text part - decides both
        # eligibility and the single confirming unit.  Earlier stream
        # bookkeeping, terminal copy checks and rendering are unchanged.
        found = validate_completed_envelope(response)
        completed_eligible[index] = found is not None
        if found is None:
            continue
        _answer_text, part = found
        final_output = response.get("output")
        if not isinstance(final_output, list):
            continue
        for output_pos, item in enumerate(final_output):
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for content_pos, candidate in enumerate(content):
                # The validator returns a reference into this same
                # response; identity locates the exact unit.
                if candidate is part:
                    units.append((index, output_pos, item, content_pos, part))

    def ownership_consistent(
        output_pos: int, item: dict[str, object], content_pos: int, part: dict[str, object]
    ) -> bool:
        declared_output = item.get("output_index")
        output_ok = declared_output is None or (
            type(declared_output) is int and declared_output == output_pos
        )
        declared_content = part.get("content_index")
        content_ok = declared_content is None or (
            type(declared_content) is int and declared_content == content_pos
        )
        return output_ok and content_ok

    def stream_positions_consistent(
        item_id: str, output_pos: int, content_pos: int
    ) -> bool:
        # Every stream event for this item that declares an ownership
        # position must declare it as an integer matching the actual final
        # list position; a null or contradictory declaration abstains.
        for declared_output, declared_content in declared.get(item_id, ()):
            if declared_output is not _UNSET and (
                type(declared_output) is not int or declared_output != output_pos
            ):
                return False
            if declared_content is not _UNSET and (
                type(declared_content) is not int or declared_content != content_pos
            ):
                return False
        return True

    non_empty = [item_id for item_id, parts in streams.items() if "".join(parts)]
    unit_index: int
    unit_item: dict[str, object]
    unit_part: dict[str, object]
    confirmed_id: str
    original_text: str
    if non_empty:
        if len(non_empty) != 1:
            # Several assistant text answers: ambiguous ownership.
            return _bypass_parts(), False
        item_id = non_empty[0]
        if not answer_event_ids <= {item_id}:
            # An answer event for another id: ambiguous ownership.
            return _bypass_parts(), False
        joined = "".join(streams[item_id])
        # Exactly one confirming final assistant output_text with consistent
        # ownership is required before any repair call: the final message id
        # equals the stream item id, the text matches, and every position
        # declared by the stream events equals the actual final list
        # positions.  An unconfirmed completed answer (absent or several
        # units, wrong id, inconsistent indices, or mismatched text)
        # bypasses unchanged with zero calls.
        if len(units) != 1:
            return _bypass_parts(), False
        unit_index, output_pos, unit_item, content_pos, unit_part = units[0]
        message_id = unit_item.get("id")
        confirmed = (
            ownership_consistent(output_pos, unit_item, content_pos, unit_part)
            and stream_positions_consistent(item_id, output_pos, content_pos)
            and unit_item.get("role") == "assistant"
            and unit_item.get("status") == "completed"
            and type(message_id) is str
            and message_id == item_id
            and unit_part["text"] == joined
        )
        if not confirmed:
            return _bypass_parts(), False
        confirmed_id = item_id
        original_text = joined
    else:
        assistant_units = [
            unit for unit in units if unit[2].get("role") == "assistant"
        ]
        if len(assistant_units) != 1 or len(units) != 1:
            # No unambiguous single assistant answer (several answers,
            # several content parts, or non-answer content): bypass.
            return _bypass_parts(), False
        unit_index, output_pos, unit_item, content_pos, unit_part = assistant_units[0]
        message_id = unit_item.get("id")
        if (
            type(message_id) is not str
            or not message_id
            or unit_item.get("status") != "completed"
            or not ownership_consistent(output_pos, unit_item, content_pos, unit_part)
            or not stream_positions_consistent(message_id, output_pos, content_pos)
            or not answer_event_ids <= {message_id}
        ):
            # Absent or inconsistent ownership: bypass unchanged.
            return _bypass_parts(), False
        confirmed_id = message_id
        original_text = str(unit_part["text"])

    if not completed_eligible.get(unit_index, False):
        # The completed envelope confirming the single answer is not an
        # explicitly completed, error-free Responses object: bypass
        # unchanged with zero review calls.
        return _bypass_parts(), False
    if not aligned_metadata_clean(unit_part):
        # Non-empty (or non-list) aligned metadata on the final answer
        # part cannot be kept valid beside a changed answer: bypass
        # unchanged with zero review calls.
        return _bypass_parts(), False

    # Order 014-b requirement 2/3: before any reviewer side effect, every
    # answer-bearing terminal representation must be proved consistent with
    # the one final assistant answer (same exact typed id, same final
    # positions, expected part kind / item role / status / content
    # cardinality and kinds, and the exact original answer text).  Any copy
    # that cannot be proved consistent makes the whole SSE ineligible:
    # return the raw original with zero calls, never keep one edited
    # representation beside an incompatible original copy.  The validated
    # editable fields are collected here, as explicit event references, and
    # are the only ones the renderer below may mutate.
    final_content = unit_item["content"]
    if not isinstance(final_content, list):
        # Units are only collected from a list content above; this keeps the
        # preflight's typed boundary explicit.
        return _bypass_parts(), False
    final_kinds = [
        part.get("type") if isinstance(part, dict) else None for part in final_content
    ]
    # event index -> ("part" | "item" | "done", content position)
    editable: dict[int, tuple[str, int | None]] = {}
    delta_indices: set[int] = set()
    copy_counts = {"output_text.done": 0, "content_part.done": 0, "output_item.done": 0}
    for index, value in enumerate(parsed):
        if value is None or index == unit_index:
            continue
        event_type = value.get("type")
        stream_id = value.get("item_id")
        if event_type == "response.output_text.delta":
            if type(stream_id) is str and stream_id == confirmed_id:
                if not aligned_metadata_clean(value):
                    # A token/offset aligned list on a delta of the
                    # confirmed answer becomes stale beside a changed
                    # answer: the whole stream abstains.
                    return _bypass_parts(), False
                delta_indices.add(index)
        elif event_type == "response.content_part.added":
            part = value.get("part")
            if not (isinstance(part, dict) and part.get("type") == "output_text"):
                # Non-answer part: recognized bookkeeping, never touched.
                continue
            # An initial added part is a supported empty placeholder only:
            # its ownership is validated here, its protected content is
            # never rewritten, and non-empty initial content cannot be
            # proved safe, so it abstains raw unchanged with zero calls.
            if type(stream_id) is not str or stream_id != confirmed_id:
                return _bypass_parts(), False
            if "text" in part and part["text"] != "":
                return _bypass_parts(), False
            if not aligned_metadata_clean(part):
                return _bypass_parts(), False
        elif event_type == "response.output_item.added":
            item_obj = value.get("item")
            if not (isinstance(item_obj, dict) and item_obj.get("type") == "message"):
                # Non-message item: recognized non-answer bookkeeping.
                continue
            obj_id = item_obj.get("id")
            if type(obj_id) is not str or obj_id != confirmed_id:
                return _bypass_parts(), False
            # The initial representation of the answer item must agree on
            # identity and role and carry supported empty placeholders only.
            if item_obj.get("role") != "assistant":
                return _bypass_parts(), False
            content = item_obj.get("content")
            if content is not None:
                if not isinstance(content, list):
                    return _bypass_parts(), False
                for part in content:
                    if not isinstance(part, dict) or (
                        "text" in part and part["text"] != ""
                    ):
                        return _bypass_parts(), False
                    if not aligned_metadata_clean(part):
                        return _bypass_parts(), False
        elif event_type == "response.output_text.done":
            if type(stream_id) is not str or stream_id != confirmed_id:
                continue
            copy_counts["output_text.done"] += 1
            if value.get("text") != original_text:
                # A terminal done event must carry the exact original
                # answer text; a missing or different text cannot be
                # proved consistent.
                return _bypass_parts(), False
            if not aligned_metadata_clean(value):
                # Stale token/offset alignment on a terminal copy of the
                # confirmed answer makes the whole stream ineligible.
                return _bypass_parts(), False
            editable[index] = ("done", None)
        elif event_type == "response.content_part.done":
            part = value.get("part")
            if not (isinstance(part, dict) and part.get("type") == "output_text"):
                # Non-answer part: recognized bookkeeping, never touched.
                continue
            # A different valid id already failed the membership check
            # above; a missing or non-string id returned earlier.  Kept as
            # an explicit defense of the ownership rule.
            if type(stream_id) is not str or stream_id != confirmed_id:
                return _bypass_parts(), False
            copy_counts["content_part.done"] += 1
            if part.get("text") != original_text:
                return _bypass_parts(), False
            if not aligned_metadata_clean(part):
                return _bypass_parts(), False
            editable[index] = ("part", None)
        elif event_type == "response.output_item.done":
            item_obj = value.get("item")
            if not (isinstance(item_obj, dict) and item_obj.get("type") == "message"):
                # Non-message item (e.g. reasoning or tool bookkeeping):
                # recognized non-answer bookkeeping, never touched.
                continue
            obj_id = item_obj.get("id")
            if type(obj_id) is not str or obj_id != confirmed_id:
                return _bypass_parts(), False
            copy_counts["output_item.done"] += 1
            if item_obj.get("role") != "assistant":
                return _bypass_parts(), False
            if "status" in item_obj and item_obj.get("status") != "completed":
                return _bypass_parts(), False
            content = item_obj.get("content")
            if not isinstance(content, list) or len(content) != len(final_content):
                return _bypass_parts(), False
            for part_pos, part in enumerate(content):
                if not isinstance(part, dict) or part.get("type") != final_kinds[part_pos]:
                    return _bypass_parts(), False
            if content[content_pos].get("text") != original_text:
                return _bypass_parts(), False
            if not aligned_metadata_clean(content[content_pos]):
                return _bypass_parts(), False
            editable[index] = ("item", content_pos)
    if any(count > 1 for count in copy_counts.values()):
        # Duplicated terminal representations of the one answer are
        # ambiguous: bypass unchanged with zero calls.
        return _bypass_parts(), False

    new_text = repair(original_text)
    if details is not None:
        # Order 041-a, R5: the extension exists only in the actual
        # terminal completion, once: the response object of the
        # terminal response.completed event.  Early prefix frames are
        # untouched (they are ordinal-identical and already delivered
        # or validated before this point).  The hook consumes the
        # request-local final outcome exactly once, with or without
        # an accepted edit (no correction means exact original_text
        # plus empty edits, never a missing object).
        unit_event = parsed[unit_index]
        if type(unit_event) is dict:
            completed_response = unit_event.get("response")
            if type(completed_response) is dict:
                repair_details.attach(
                    completed_response,
                    details(
                        original_text, new_text, output_pos, content_pos
                    ),
                )
        # A non-dict completed response is unreachable (units are only
        # collected from validated completed envelopes); the handler
        # drains the unconsumed outcome on every path.
    if new_text == original_text and details is None:
        # No accepted text edit: the raw bytes are returned unchanged.
        return _bypass_parts(), False

    # The validated final unit (a shared reference into the parsed events)
    # carries the repaired text; the render pass re-emits exactly the
    # collected editable references plus the confirmed delta events.
    unit_part["text"] = new_text
    output: list[str] = []
    delta_done = False
    for (block, delimiter), value, index in zip(blocks, parsed, range(len(blocks)), strict=True):
        if value is None:
            output.append(block + delimiter)
            continue
        re_render = index == unit_index or index in logprobs_modified
        kind = editable.get(index)
        if kind is not None:
            kind_name, copy_pos = kind
            if kind_name == "done":
                value["text"] = new_text
                re_render = True
            elif kind_name == "part":
                part = value.get("part")
                if isinstance(part, dict):
                    part["text"] = new_text
                    re_render = True
            else:
                item_obj = value.get("item")
                if isinstance(item_obj, dict) and type(copy_pos) is int:
                    content = item_obj.get("content")
                    if isinstance(content, list):
                        part = content[copy_pos]
                        if isinstance(part, dict):
                            part["text"] = new_text
                            re_render = True
        elif index in delta_indices and new_text != original_text:
            # The first non-empty delta of the confirmed answer carries the
            # full repaired text; every later delta is emptied so the answer
            # is not duplicated.  Only the explicitly validated confirmed id
            # is ever mutated; only genuine string payloads are touched and
            # empty text events stay untouched.
            delta = value.get("delta")
            if type(delta) is str and delta:
                value["delta"] = new_text if not delta_done else ""
                delta_done = True
                re_render = True
        if re_render:
            block = _render_event(block, value)
        output.append(block + delimiter)
    # The flag reports the ACTUAL text edit only (order 041-a R5): a
    # metadata-only extension (details opted in, no accepted edit)
    # changes wire bytes but is NOT an answer-text change; handler,
    # cache-validator and Content-Length decisions key off the actual
    # transformed bytes, not this flag.
    return output, new_text != original_text


def transform_sse(
    raw: bytes,
    repair: Callable[[str], str],
    details: Callable[[str, str, int | None, int | None], dict[str, object]]
    | None = None,
) -> tuple[bytes, bool]:
    """Public whole-body SSE transform: per-block output, joined and
    UTF-8 encoded (byte-identical to the historical implementation
    when the optional details hook is absent).
    """
    parts, repaired = transform_sse_blocks(raw, repair, details)
    return "".join(parts).encode("utf-8"), repaired


def _sse_completed_ok(raw: bytes) -> bool:
    """True iff the completed SSE carries at least one valid
    response.completed envelope (the transform precheck).  Malformed
    framing or JSON is not an eligible completed stream."""
    try:
        return any(
            value.get("type") == "response.completed"
            and isinstance(value.get("response"), dict)
            and validate_completed_envelope(value["response"]) is not None
            for _block, value in _events(raw)
        )
    except (ProxyError, ValueError, OverflowError, RecursionError):
        return False


def _strict_json_body(raw: bytes) -> dict[str, object]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProxyError("upstream JSON body is not valid UTF-8") from exc
    return _strict_json_object(text)


def _safe_json_dumps(obj: dict[str, object]) -> bytes:
    r"""Serialize to UTF-8 JSON, escaping lone surrogates to keep output valid.

    Normal non-ASCII text is preserved (ensure_ascii=False); a lone unpaired
    surrogate cannot be UTF-8-encoded, so we fall back to ensure_ascii=True
    which represents it as a JSON unicode escape in the text - the parsed
    value is identical (order 035-c R6).
    """
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except UnicodeEncodeError:
        return json.dumps(obj, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def normalize_json_body(raw: bytes) -> bytes:
    """Remove defined logprobs from a well-formed JSON response (any status).

    If the body is not valid strict JSON (including decoder overflow,
    duplicate keys, nonfinite numbers), it is returned unchanged
    (malformed error-body semantics preserved, order 035-c R3).
    No repair, no review calls.
    """
    try:
        envelope = _strict_json_object(raw.decode("utf-8"))
    except (ProxyError, UnicodeDecodeError, ValueError, OverflowError):
        return raw
    if not isinstance(envelope, dict):
        return raw
    if not remove_defined_logprobs(envelope):
        return raw
    try:
        return _safe_json_dumps(envelope)
    except (UnicodeEncodeError, ValueError):
        return raw


def normalize_sse_body(raw: bytes) -> bytes:
    """Remove defined logprobs from a well-formed SSE stream (any status).

    If the body is not valid SSE, it is returned unchanged.  No repair,
    no review calls.  Only events with recognized logprobs locations are
    re-rendered.
    """
    try:
        blocks = _split_sse(raw)
        parsed: list[dict[str, object] | None] = [_event(block) for block, _ in blocks]
    except (ProxyError, ValueError, OverflowError):
        return raw
    modified: set[int] = set()
    for idx, val in enumerate(parsed):
        if val is None:
            continue
        et = val.get("type")
        if et in ("response.output_text.delta", "response.output_text.done"):
            if "logprobs" in val:
                del val["logprobs"]
                modified.add(idx)
        elif et in ("response.content_part.added", "response.content_part.done"):
            p = val.get("part")
            if isinstance(p, dict) and p.get("type") == "output_text" and "logprobs" in p:
                del p["logprobs"]
                modified.add(idx)
        elif et in ("response.output_item.added", "response.output_item.done"):
            it = val.get("item")
            if isinstance(it, dict) and it.get("type") == "message":
                ct = it.get("content")
                if isinstance(ct, list):
                    for pp in ct:
                        if (isinstance(pp, dict) and pp.get("type") == "output_text"
                                and "logprobs" in pp):
                            del pp["logprobs"]
                            modified.add(idx)
        elif et in ("response.created", "response.in_progress", "response.completed",
                    "response.incomplete", "response.failed", "response.queued"):
            resp = val.get("response")
            if isinstance(resp, dict) and remove_defined_logprobs(resp):
                modified.add(idx)
    if not modified:
        return raw
    parts: list[str] = []
    for (block, delim), value, idx in zip(blocks, parsed, range(len(blocks)), strict=True):
        if value is None:
            parts.append(block + delim)
            continue
        if idx in modified:
            block = _render_event(block, value)
        parts.append(block + delim)
    return "".join(parts).encode("utf-8")


def transform_json(
    raw: bytes,
    repair: Callable[[str], str],
    details: Callable[[str, str, int | None, int | None], dict[str, object]]
    | None = None,
) -> tuple[bytes, bool]:
    """Repair one completed JSON text answer with the same eligibility.

    Nonstreaming JSON is the primary supported path (order 015-a,
    requirement 4).  Malformed main JSON (invalid UTF-8, duplicate keys,
    nonfinite numbers, non-object) is a main error: it raises
    ``ProxyError`` and never reaches repair.  Defined API logprobs are
    always removed by presence from recognized output_text parts before
    eligibility or repair (order 035-c): without an approved edit, answer
    text stays identical and only probability-field removal may change
    wire data.  A well-formed envelope that is unsupported, or explicitly
    incomplete or error-bearing, still has defined logprobs removed while
    retaining its original status/finish/error semantics.  Only the exact
    validated answer part is mutated for text; unrelated values, IDs,
    usage, citations and protected reasoning are preserved.
    """
    envelope = _strict_json_body(raw)
    # Order 035-c: always remove defined logprobs before eligibility/repair.
    metadata_removed = remove_defined_logprobs(envelope)
    found = validate_completed_envelope(envelope)
    if found is None:
        if metadata_removed:
            return (
                _safe_json_dumps(envelope),
                False,
            )
        return raw, False
    original_text, part = found
    new_text = repair(original_text)
    if details is not None:
        # Order 041-a, R2: the actual answer part is located by proven
        # object identity inside the validated envelope - a reasoning
        # item preceding the answer or repeated identical text never
        # shifts it to a substring/equality match.  The hook consumes
        # the request-local final outcome exactly once, with or
        # without an accepted edit; the reserved root field rule
        # (collision) is applied on the envelope root only.
        output_index: int | None = None
        content_index: int | None = None
        output = envelope.get("output")
        if isinstance(output, list):
            for output_pos, item in enumerate(output):
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for content_pos, candidate in enumerate(content):
                    if candidate is part:
                        output_index = output_pos
                        content_index = content_pos
        part["text"] = new_text
        repair_details.attach(
            envelope,
            details(original_text, new_text, output_index, content_index),
        )
        return _safe_json_dumps(envelope), new_text != original_text
    if new_text == original_text:
        if metadata_removed:
            return (
                _safe_json_dumps(envelope),
                False,
            )
        return raw, False
    part["text"] = new_text
    return (
        _safe_json_dumps(envelope),
        True,
    )


@dataclass(frozen=True)
class Forwarded:
    status: int
    body: bytes
    content_type: str


@dataclass(frozen=True)
class _PreparedCandidate:
    """A detector candidate with its validated neighbor context, in the
    deterministic original order used for batch target alignment."""

    candidate: Candidate
    left: str | None
    right: str | None
    left_bounds: tuple[int, int] | None
    right_bounds: tuple[int, int] | None


@dataclass
class RepairEngine:
    index: Path
    reviewer: Reviewer
    mode: str = "local-context"
    # Resolved qualified-Pandoc spec; None resolves lazily at repair time
    # (the proxy main path resolves and checks it at startup).
    pandoc: PandocSpec | None = None
    # Configured review policy (order 040-a): the same typed immutable
    # instance the normal entry point loaded once and also passed to
    # the reviewer; direct core use keeps the shipped default file
    # values.  It drives the length-scaled selection allowance, the
    # batch size/context/prompt/output/request/response bounds and the
    # whole-exchange timeout (already env-overridden at the entry
    # point); it never touches main-request settings or the analysis
    # gate.
    policy: ReviewPolicy = DEFAULT_POLICY
    _details: list[dict[str, object]] | None = None
    # Actual reviewer usage for the in-flight request only; reset on every
    # request so no earlier usage is ever carried over (order 015-a,
    # requirement 7).  Order 040-a, R6: the value is the aggregate
    # over ALL attempted batches and stays known only while every
    # attempted batch supplied validated usage - any missing/invalid
    # usage or any failed batch keeps it explicitly unknown (None),
    # never partial-as-total and never zero.
    _review_usage: BatchUsage | None = None
    # Order 040-a, R6: sticky unknown - once ANY dispatched batch
    # lacks validated usage (missing/invalid envelope usage, or a
    # dispatched batch that failed), the aggregate is unknown for the
    # whole request and never reverts to known, even if a later batch
    # reports validated usage.  A pre-dispatch failure (no attempt, no
    # token cost) does NOT set it.
    _review_usage_sticky_unknown: bool = False
    # Order 040-a, R6: batches that COMPLETED with a validated result
    # (usage present or absent).  Compared against the actual
    # attempted-dispatch ledger at observation time: a completed-known
    # result count below the actual attempts (for example a
    # mid-second-call cancellation) is unknown, never reported as the
    # total.
    _review_completed_calls: int = 0
    # The in-flight request's request-local cancellation scope
    # (order 030-a, requirement 1); None for direct core use, where
    # context-local access is a no-op.
    _scope: request_lifecycle.CancellationScope | None = None
    # Whether THIS request's operator usage record was already
    # emitted (order 030-a, requirement 7): the success-path
    # observation and the abort observation are the only two
    # emitters, and exactly one of them may emit per request.
    _usage_observed: bool = False
    # The request-local FINAL repair outcome (order 041-a, R3): the
    # immutable (original, final_edits, repaired) triple set by
    # repair() - the no-edit default on every fail-closed early path,
    # the authoritative final surviving edits on the success return -
    # and drained exactly once by the presentation hook or by the
    # handler on normal completion.  It never crosses requests: reset
    # on begin/abort and drained on success, so no original/repaired
    # text is retained as a cross-request cache.
    _repair_outcome: tuple[str, tuple[tuple[int, int, str], ...], str] | None = None

    def begin_request(
        self, scope: request_lifecycle.CancellationScope | None = None
    ) -> None:
        self._details = []
        self._review_usage = None
        self._review_usage_sticky_unknown = False
        self._review_completed_calls = 0
        self._scope = scope
        self._usage_observed = False
        self._repair_outcome = None

    def _raise_if_cancelled(self) -> None:
        # Phase-boundary check (order 030-a, requirement 4): a no-op
        # outside an active request scope.
        if self._scope is not None:
            self._scope.raise_if_cancelled()

    def _known_review_usage(self, actual_attempts: int) -> BatchUsage | None:
        """The authoritative known aggregate reviewer usage (order
        040-a, R6), or explicit unknown.

        Known only while ALL of: (a) no dispatched batch ever lacked
        validated usage (the sticky unknown flag is clear - missing
        envelope usage and dispatched-batch failure both set it and it
        never reverts); (b) the completed-result count EQUALS the
        actual attempted-dispatch count - a mid-second-call
        cancellation or transport failure leaves a dispatched attempt
        without a completed result, so an earlier completed known
        result is never reported as the total, while a between-call
        or pre-dispatch failure leaves the completed known aggregate
        intact; and (c) the aggregate exists at all (zero attempts is
        unknown, never zero).
        """
        if self._review_usage_sticky_unknown:
            return None
        if self._review_completed_calls != actual_attempts:
            return None
        return self._review_usage

    def abort_request(
        self, observer: Callable[[list[dict[str, object]]], None]
    ) -> None:
        """One content-free abort observation plus the full
        request-local state reset (order 030-a, requirements 6 and
        7).

        The observation derives ONE authoritative record from the
        engine's own request-local state: the actual
        attempted-dispatch count at the real owned dispatch
        boundary (zero for a pre-dispatch cancellation; one per
        owned exchange actually attempted, including a mid-second-
        call cancellation or transport failure - order 040-a, R6)
        and the validated known-or-unknown aggregate usage (a
        completed known result remains known through a later CPU
        cancellation; any missing/invalid usage or failed batch
        keeps it unknown, never partial-as-total/zero).  The
        text-bearing details are drained and discarded - never
        re-aggregated (their already-counted batches would
        double-count the actual review work) and never observed, so
        the emitted record is content-free.  An already-emitted
        observation is never re-observed: exactly one operator
        record per request, whichever boundary emits it first.
        The request-local state is always cleared, even if the
        operator callback faults (the drain precedes the observer
        call).
        """
        calls = (
            self._scope.review_dispatch_attempts()
            if self._scope is not None
            else self._review_completed_calls
        )
        known = self._known_review_usage(calls)
        if known is not None:
            usage: object = {
                "input_tokens": known.input_tokens,
                "output_tokens": known.output_tokens,
                "total_tokens": known.total_tokens,
            }
        else:
            usage = "unknown"
        self.take_details()
        self._repair_outcome = None
        self._review_usage = None
        self._scope = None
        if self._usage_observed:
            return
        self._usage_observed = True
        observer(
            [
                {
                    "review_batch": {
                        "calls": calls,
                        "targets": 0,
                        "outcome": "cancelled",
                        "reason": None,
                        "usage": usage,
                    },
                }
            ]
        )

    def mark_usage_observed(self) -> None:
        """Record that THIS request's operator usage record was
        already emitted on the success path (order 030-a,
        requirement 7): a later abort must then only clear state
        and never re-observe the already-emitted record."""
        self._usage_observed = True

    def take_details(self) -> list[dict[str, object]]:
        details = self._details or []
        self._details = None
        return details

    def take_repair_outcome(
        self,
    ) -> tuple[str, tuple[tuple[int, int, str], ...], str] | None:
        """Drain THIS request's final repair outcome (order 041-a,
        R3): (original, final edits, repaired).  Exactly one consumer
        per request - the presentation hook, or the handler on normal
        completion - and None when repair() never ran for this
        request.  The drain clears the request-local slot so no
        original/repaired text is retained across requests."""
        outcome = self._repair_outcome
        self._repair_outcome = None
        return outcome

    def take_repair_details(
        self,
        original: object,
        delivered: object,
        output_index: object,
        content_index: object,
    ) -> dict[str, object]:
        """The request-local llm_language_repair object (order
        041-a, R2/R4): consume the final outcome ONCE, require exact
        identity with the transformed answer, and build strictly.
        Any mismatch, proof failure, cap overflow or missing index
        yields the fixed unavailable object - never a fabricated or
        partial mapping.  Metadata extraction precedes the shared
        trace/usage drains (the hook runs inside the transform)."""
        outcome = self.take_repair_outcome()
        if (
            outcome is None
            or outcome[0] is not original
            or outcome[2] is not delivered
        ):
            return repair_details.unavailable(
                repair_details.EDIT_MAPPING_UNAVAILABLE
            )
        # The untrusted index values pass through unmodified: the
        # builder's strict type check turns any non-int (including
        # None/bool) into the fixed unavailable object.
        return repair_details.build_complete(
            outcome[0],
            outcome[1],
            outcome[2],
            output_index,  # type: ignore[arg-type]
            content_index,  # type: ignore[arg-type]
        )

    def _fail_closed_original(
        self, text: str, raw_started: float, reason: str | None, detail: str
    ) -> str:
        """Record a minimal trace and return the exact completed original.

        A global parse/mapping failure must short-circuit before Corpus
        access, detection and any reviewer call (order 013-a, requirement
        12).  No corpus is opened and no reviewer is invoked.
        """
        if self._details is not None:
            self._details.append(
                {
                    "original": text,
                    "repaired": text,
                    "candidates": [],
                    "corpus_summaries": [],
                    "reviewer_decisions": [],
                    "acceptance": [],
                    "replacements": [],
                    "timings": {
                        "raw_seconds": 0.0,
                        "detector_seconds": 0.0,
                        "reviewer_seconds": 0.0,
                        "repair_seconds": round(time.monotonic() - raw_started, 6),
                    },
                    "protected_difference_count": 0,
                    "review_batch": {
                        "calls": 0,
                        "targets": 0,
                        "outcome": "skipped",
                        "reason": None,
                        # No reviewer response exists on this path: usage
                        # is explicitly unknown, never reused from an
                        # earlier request.
                        "usage": "unknown",
                    },
                    "protection_failure": {
                        "reason": reason or "PANDOC_PROTECTION",
                        "detail": detail[:256],
                    },
                }
            )
        return text

    def _fail_closed_evidence(
        self, text: str, raw_started: float, reason: str, stage: str
    ) -> str:
        """Return the exact completed original after a technical evidence failure.

        An early failure (index open or detection) short-circuits before any
        reviewer call: zero edits and zero review calls are recorded.  A
        late failure is recorded on the normal detail by the caller (stage
        ``late``) with the actual review attempt and known/unknown usage
        preserved.  The fixed content-free label reflects no SQL, path,
        value, target or credential text, and an evidence failure is never
        reported as a Pandoc failure (order 016-a, requirement 4).
        """
        if self._details is not None:
            self._details.append(
                {
                    "original": text,
                    "repaired": text,
                    "candidates": [],
                    "corpus_summaries": [],
                    "reviewer_decisions": [],
                    "acceptance": [],
                    "replacements": [],
                    "timings": {
                        "raw_seconds": 0.0,
                        "detector_seconds": 0.0,
                        "reviewer_seconds": 0.0,
                        "repair_seconds": round(time.monotonic() - raw_started, 6),
                    },
                    "protected_difference_count": 0,
                    "review_batch": {
                        "calls": 0,
                        "targets": 0,
                        "outcome": "skipped",
                        "reason": None,
                        # No reviewer response exists on this path: usage
                        # is explicitly unknown, never reused from an
                        # earlier request.
                        "usage": "unknown",
                    },
                    "evidence_failure": {"reason": reason, "stage": stage},
                }
            )
        return text

    def _fail_closed_analysis(
        self, text: str, raw_started: float, reason: str
    ) -> str:
        """Record a minimal trace and return the exact completed original.

        An ineligible completed answer bypasses the OPTIONAL analysis
        pass BEFORE Pandoc, Corpus, detection and any reviewer call
        (order 025-a, requirement 3): zero analysis, evidence,
        protection and review work is performed, and nothing is
        truncated, split, sampled or partially repaired.  The fixed
        content-free ``analysis_bypass`` reason is separate from the
        protection and evidence failure labels, which this path never
        sets, and no client-visible diagnostic text or logging is
        added.
        """
        if self._details is not None:
            self._details.append(
                {
                    "original": text,
                    "repaired": text,
                    "candidates": [],
                    "corpus_summaries": [],
                    "reviewer_decisions": [],
                    "acceptance": [],
                    "replacements": [],
                    "timings": {
                        "raw_seconds": 0.0,
                        "detector_seconds": 0.0,
                        "reviewer_seconds": 0.0,
                        "repair_seconds": round(time.monotonic() - raw_started, 6),
                    },
                    "protected_difference_count": 0,
                    "review_batch": {
                        "calls": 0,
                        "targets": 0,
                        "outcome": "skipped",
                        "reason": None,
                        # No reviewer response exists on this path:
                        # usage is explicitly unknown, never reused
                        # from an earlier request.
                        "usage": "unknown",
                    },
                    "analysis_bypass": {"reason": reason},
                }
            )
        return text

    def repair(self, text: str) -> str:
        raw_started = time.monotonic()
        # Order 041-a, R3: the request-local final outcome defaults to
        # the no-edit result (exact completed original) and is
        # overwritten with the authoritative final surviving edits
        # before the success return; every fail-closed early path
        # keeps the no-edit default, which is exactly its outcome.
        self._repair_outcome = (text, (), text)
        # Versioned finite analysis eligibility (order 025-a,
        # requirement 3): the ORIGINAL completed answer is checked on
        # its exact codepoint count first, then on its strict
        # original UTF-8 byte count (no NFC, stripping or other
        # transformation), BEFORE resolve/protect/map/residual/
        # tokenize/index/reviewer work.  Ineligibility - an invalid
        # policy, either size axis exceeded, or an unencodable
        # original - returns the exact completed original with a
        # fixed content-free analysis reason and zero optional work;
        # every admitted answer still runs the mandatory Pandoc
        # pipeline below unchanged.  Only the declared policy
        # (ValueError) and encoding (UnicodeEncodeError) conditions
        # are translated; caller control and MemoryError propagate
        # untouched.
        try:
            codepoint_limit, byte_limit = analysis_policy.current_limits()
        except ValueError:
            return self._fail_closed_analysis(
                text, raw_started, analysis_policy.ANALYSIS_POLICY_INVALID
            )
        reason = analysis_policy.classify_original(
            text, codepoint_limit, byte_limit
        )
        if reason is not None:
            return self._fail_closed_analysis(text, raw_started, reason)
        self._raise_if_cancelled()
        try:
            spec = self.pandoc if self.pandoc is not None else resolve_pandoc_spec()
            outcome = protect_with_pandoc(spec, text)
            if not outcome.ok:
                return self._fail_closed_original(
                    text, raw_started, outcome.reason, outcome.detail
                )
            intervals = compose_protection(text, outcome)
        except PandocStartupError as exc:
            return self._fail_closed_original(text, raw_started, exc.reason, exc.detail)
        except request_lifecycle.RequestCancelled:
            # A dedicated cancellation at an owned phase boundary
            # escapes to the handler (order 030-a, requirements 4
            # and 6); it is never a fail-closed original.
            raise
        except Exception as exc:  # noqa: BLE001 - fail closed before Corpus
            # Any unexpected mapping, composition or depth failure must be
            # translated to the fail-closed original, never escape to the
            # handler or widen around the upstream capture path.
            return self._fail_closed_original(
                text, raw_started, "PANDOC_UNEXPECTED", type(exc).__name__
            )
        self._raise_if_cancelled()
        # Technical evidence access opens only after complete-main
        # eligibility: an invalid or ineligible main response never opens
        # or creates the index (order 016-a, requirements 4 and 6).  The
        # adapter is strictly read-only over the existing index.
        cancelled = self._scope.cancelled if self._scope is not None else None
        corpus: RuntimeCorpus | None = None
        # The explicit first control of the corpus-owned region
        # (order 030-a, requirement 5): the close below must never
        # replace the exact first, and clean-flow close controls
        # still propagate.
        first_control: BaseException | None = None
        try:
            corpus = RuntimeCorpus(self.index)
            if cancelled is not None:
                corpus.adopt_cancelled(cancelled)
            # Every step after the successful acquisition - the
            # adoption and the phase-boundary cancellation checks
            # included - sits inside the owned region, so the
            # read-only handle is released on every path after
            # acquisition (order 030-a, requirement 5).
            self._raise_if_cancelled()
            try:
                detector_started = time.monotonic()
                # Length-scaled selection maximum (order 040-a, R1/R2):
                # B * targets_per_call with B = ceil(N / characters_
                # per_call) on the ORIGINAL completed eligible answer
                # (N code points).  The detector keeps its frozen
                # deterministic scoring/ranking/tie-breaking/non-
                # overlap selection; the obsolete hidden fixed cap is
                # gone, and N=0 (no eligible suspicion possible)
                # admits zero targets.
                candidates = detect(
                    text,
                    corpus,
                    intervals,
                    mode=self.mode,
                    maximum=self.policy.selection_maximum(len(text)),
                )
            except RuntimeCorpusError as exc:
                # Detection queried the index: same early boundary - exact
                # completed original with zero edits and zero review calls.
                return self._fail_closed_evidence(
                    text, raw_started, exc.reason, "early"
                )
            detector_seconds = time.monotonic() - detector_started
            tokens = tokenize(text, intervals)
            prepared: list[_PreparedCandidate] = []
            selected_invalid = False
            for candidate in candidates:
                token_index = next(
                    (i for i, token in enumerate(tokens) if token.start == candidate.start),
                    None,
                )
                if token_index is None:
                    # A detected occurrence whose start matches no token is
                    # invalid/stale selection: the whole answer returns the
                    # completed original with zero calls, never a valid
                    # subset (order 014-a, requirement 2).
                    selected_invalid = True
                    break
                # The right neighbor is the first token at or after the
                # WHOLE selected span's end (order 027-a, requirement 3):
                # for a single-token span this is the immediately
                # following token, and for a multiword span the span's
                # internal tokens are never its external neighbors.
                right_index = next(
                    (
                        i
                        for i in range(token_index + 1, len(tokens))
                        if tokens[i].start >= candidate.end
                    ),
                    None,
                )
                prepared.append(
                    _PreparedCandidate(
                        candidate,
                        left=tokens[token_index - 1].key if token_index else None,
                        right=tokens[right_index].key
                        if right_index is not None
                        else None,
                        left_bounds=(
                            (tokens[token_index - 1].start, tokens[token_index - 1].end)
                            if token_index
                            else None
                        ),
                        right_bounds=(
                            (tokens[right_index].start, tokens[right_index].end)
                            if right_index is not None
                            else None
                        ),
                    )
                )
            # Length-scaled bounded review budget (order 040-a,
            # R1/R2): at most B = ceil(N / characters_per_call) fresh
            # batch requests per completed eligible answer (N the
            # ORIGINAL completed answer length in code points),
            # covering at most B * targets_per_call selected
            # occurrences.  These are maxima: no eligible suspicion
            # means zero calls and an empty batch is never sent.  The
            # analysis gate above already bounds the eligible
            # original, so the default B stays at most 32; no hidden
            # fixed target cap remains on the runnable path.
            review_batch: dict[str, object] = {
                "calls": 0,
                "targets": len(candidates) if selected_invalid else len(prepared),
                "outcome": "skipped",
                "reason": REASON_PREP_INVALID if selected_invalid else None,
                # Request-local default: every path without a reviewer
                # response (Pandoc failure, no suspicion, unsupported
                # response, failed batch) reports explicit unknown usage,
                # never a zero and never an earlier request's usage.
                "usage": "unknown",
            }
            # Per-batch ledger records (order 040-a, R6): one record
            # per batch actually prepared/dispatched, in original
            # order; the aggregate ``review_batch`` above summarizes
            # them for the existing single-record interfaces.
            review_batches: list[dict[str, object]] = []
            # The response-wide proposal map: populated batch by batch
            # and discarded (cleared) on any batch failure - uncommitted
            # proposals are never applied or retained (order 040-a, R5).
            proposals: dict[str, BatchProposal] = {}
            aligned: list[str] | None = None
            reviewer_seconds = 0.0
            if prepared and not selected_invalid:
                policy = self.policy
                occurrences = tuple(
                    (item.candidate.start, item.candidate.end, item.candidate.text)
                    for item in prepared
                )
                protected = tuple((item.start, item.end) for item in intervals)
                # Select/prepare ALL occurrences against the same
                # immutable original BEFORE any dispatch: consecutive
                # groups of policy.targets_per_call with response-local
                # occurrence ids unique across batches and an exact
                # batch-to-original mapping (order 040-a, R2); each
                # occurrence is batched at most once, never re-detected
                # on edited text.
                # Response-wide selection validity gate (order 040-a,
                # R2; established defensive preparation behavior): the
                # COMPLETE occurrence list must stay within the length-
                # scaled bound and be in deterministic original order,
                # strictly non-overlapping, duplicate-free and exact
                # against the immutable original ACROSS chunk
                # boundaries - a stale/overlapping/out-of-order
                # selection must not become valid merely because it
                # straddles a chunk (the trusted detector returns
                # globally ordered nonoverlapping results; this gate
                # retains the old single-batch preparation validity at
                # response scope).  Any violation returns the completed
                # original with zero calls.
                response_wide_valid = (
                    len(occurrences) <= policy.selection_maximum(len(text))
                )
                previous_end = -1
                seen_spans: set[tuple[int, int]] = set()
                for start, end, target_text in occurrences:
                    if (
                        not isinstance(start, int)
                        or isinstance(start, bool)
                        or not isinstance(end, int)
                        or isinstance(end, bool)
                        or not isinstance(target_text, str)
                        or start < 0
                        or start >= end
                        or end > len(text)
                        or start < previous_end
                        or (start, end) in seen_spans
                        or text[start:end] != target_text
                    ):
                        response_wide_valid = False
                        break
                    previous_end = end
                    seen_spans.add((start, end))
                batches: list[tuple[BatchTarget, ...]] = []
                prep_invalid = not response_wide_valid
                if not prep_invalid:
                    for offset in range(
                        0, len(occurrences), policy.targets_per_call
                    ):
                        chunk = occurrences[
                            offset : offset + policy.targets_per_call
                        ]
                        targets = prepare_targets(
                            text, chunk, protected, policy, id_offset=offset
                        )
                        if targets is None:
                            # Invalid/stale/protected/oversized preparation
                            # of ANY occurrence: completed original with
                            # zero calls, never a valid subset (order
                            # 014-a, requirement 2).
                            prep_invalid = True
                            break
                        batches.append(targets)
                if prep_invalid:
                    # No batch is dispatched; zero calls recorded.
                    review_batch["reason"] = REASON_PREP_INVALID
                else:
                    aligned = [
                        target.id for batch in batches for target in batch
                    ]
                    review_batch["targets"] = len(aligned)

                    def _dispatch_calls(
                        before: int, reason: str | None
                    ) -> int:
                        """The actual attempted-dispatch ledger for one
                        batch (order 040-a, R6): with an active scope
                        the count is read at the real owned dispatch
                        boundary (zero for a pre-dispatch failure, one
                        when the owned exchange was actually attempted,
                        including mid-call cancellation/transport
                        failure); without a scope (direct core use) the
                        closed reason labels keep the historical
                        mapping."""
                        if self._scope is not None:
                            return self._scope.review_dispatch_attempts() - before
                        if reason is None:
                            return 1
                        return (
                            0
                            if reason in (
                                REASON_PROMPT_OVERSIZE,
                                REASON_REQUEST_OVERSIZE,
                            )
                            else 1
                        )

                    batch_started = time.monotonic()
                    try:
                        for batch in batches:
                            # Phase-boundary cancellation check before
                            # each dispatch (order 030-a, R4): a
                            # cancelled request starts no new reviewer
                            # work.
                            self._raise_if_cancelled()
                            attempts_before = (
                                self._scope.review_dispatch_attempts()
                                if self._scope is not None
                                else 0
                            )
                            record: dict[str, object] = {
                                "calls": 0,
                                "targets": len(batch),
                                "outcome": "skipped",
                                "reason": None,
                                # Request-local default: explicit unknown
                                # until this batch produces validated
                                # usage; never a zero, never carried over.
                                "usage": "unknown",
                            }
                            try:
                                batch_result = self.reviewer.review_batch(
                                    text, batch
                                )
                            except request_lifecycle.RequestCancelled:
                                # A dedicated cancellation during the
                                # owned exchange escapes to the handler
                                # (order 030-a, requirements 4 and 6);
                                # it is never a trusted failure label.
                                raise
                            except Exception as exc:
                                # Only the closed vocabulary of fixed
                                # labels is trusted; anything else maps
                                # to REVIEW_FAILED so untrusted exception
                                # content never reaches traces. (type
                                # guard first: an unhashable reason must
                                # not raise inside this fallback path.)
                                reason = getattr(exc, "reason", None)
                                if (
                                    type(reason) is not str
                                    or reason not in BOUND_REVIEW_REASONS
                                ):
                                    reason = REASON_FAILED
                                record["outcome"] = "failed"
                                record["reason"] = reason
                                record["calls"] = _dispatch_calls(
                                    attempts_before, reason
                                )
                                review_batches.append(record)
                                # Any batch failure stops later batches
                                # and discards uncommitted proposals:
                                # the completed original is delivered
                                # (order 040-a, R5).  The aggregate usage
                                # is explicitly unknown from here on -
                                # never partial-as-total and never zero,
                                # and a failure before another dispatch
                                # adds no fake attempt/token cost.
                                aligned = None
                                proposals.clear()
                                # A DISPATCHED failure (an actual attempt
                                # without a completed result) makes the
                                # aggregate sticky unknown - never
                                # partial-as-total and never zero; a
                                # PRE-DISPATCH failure is not an attempt
                                # (no fake attempt or token cost) and
                                # leaves any completed known aggregate
                                # for the earlier attempted call intact
                                # (order 040-a, R6).
                                if record["calls"]:
                                    self._review_usage_sticky_unknown = True
                                    self._review_usage = None
                                review_batch["outcome"] = "failed"
                                review_batch["reason"] = reason
                                break
                            else:
                                record["outcome"] = "ok"
                                record["calls"] = _dispatch_calls(
                                    attempts_before, None
                                )
                                # One completed result (order 040-a, R6):
                                # counted whether or not its envelope
                                # usage is validated.
                                self._review_completed_calls += 1
                                # Actual reviewer usage, separately from
                                # main usage.  A dispatched batch without
                                # validated usage makes the aggregate
                                # sticky unknown - never partial-as-total,
                                # never reverted to known by a later
                                # batch.
                                if batch_result.usage is not None:
                                    record["usage"] = {
                                        "input_tokens": (
                                            batch_result.usage.input_tokens
                                        ),
                                        "output_tokens": (
                                            batch_result.usage.output_tokens
                                        ),
                                        "total_tokens": (
                                            batch_result.usage.total_tokens
                                        ),
                                    }
                                    aggregate = self._review_usage
                                    if aggregate is not None:
                                        self._review_usage = BatchUsage(
                                            aggregate.input_tokens
                                            + batch_result.usage.input_tokens,
                                            aggregate.output_tokens
                                            + batch_result.usage.output_tokens,
                                            aggregate.total_tokens
                                            + batch_result.usage.total_tokens,
                                        )
                                    else:
                                        self._review_usage = batch_result.usage
                                else:
                                    self._review_usage_sticky_unknown = True
                                    self._review_usage = None
                                proposals.update(batch_result.proposals)
                                review_batches.append(record)
                    finally:
                        reviewer_seconds = time.monotonic() - batch_started
                    # The aggregate call count is the actual
                    # attempted-dispatch ledger across all batches
                    # (order 040-a, R6).
                    review_batch["calls"] = sum(
                        record["calls"] for record in review_batches
                    )
                    # The authoritative known aggregate is serialized
                    # INDEPENDENTLY OF THE REPAIR OUTCOME (order 040-a,
                    # R6): known only when the completed-result count
                    # equals the actual dispatch ledger and no
                    # dispatched batch lacked validated usage - so a
                    # failed or pre-dispatch-stopped review whose
                    # actual attempted calls all supplied validated
                    # usage still reports that known cost (a late
                    # pre-dispatch failure adds no fake attempt and
                    # does not erase earlier known usage), while any
                    # dispatched loss stays unknown, never partial-
                    # as-total and never zero.
                    attempts = (
                        self._scope.review_dispatch_attempts()
                        if self._scope is not None
                        else review_batch["calls"]
                    )
                    known = self._known_review_usage(attempts)
                    if known is not None:
                        review_batch["usage"] = {
                            "input_tokens": known.input_tokens,
                            "output_tokens": known.output_tokens,
                            "total_tokens": known.total_tokens,
                        }
                    if aligned is not None:
                        review_batch["outcome"] = "ok"
            decisions: list[dict[str, object]] = []
            edits: list[tuple[int, int, str]] = []
            composed_records: list[composed_validation.ComposedRecord] = []
            # Late technical evidence failure state: a known
            # RuntimeCorpusError during per-proposal acceptance aborts the
            # entire optional pass from the immutable original.
            late_aborted = False
            evidence_reason: str | None = None
            # R6 mapping failure state: a validation mapping that cannot
            # be proved aborts the whole optional pass to the completed
            # original (order 027-a, requirement 6).
            mapping_unprovable = False
            for index, item in enumerate(prepared):
                self._raise_if_cancelled()
                candidate = item.candidate
                review_started = time.monotonic()
                proposal_record: dict[str, object] | None = None
                acceptance: dict[str, object] = {
                    "accepted": False,
                    "reason": "reviewer-error",
                    "replacement": None,
                }
                failure: str | None = None
                if late_aborted:
                    # The optional pass already aborted: no earlier accepted
                    # edit survives and no further proposal is applied; the
                    # reviewer proposal is retained separately as untrusted
                    # data, never as an acceptance.
                    if aligned is not None and proposals is not None:
                        batch_proposal = proposals[aligned[index]]
                        proposal_record = {
                            "keep": batch_proposal.keep,
                            "replacement": batch_proposal.replacement,
                            "needs_wider_edit": batch_proposal.needs_wider_edit,
                        }
                    acceptance = {
                        "accepted": False,
                        "reason": "evidence-abort",
                        "replacement": None,
                    }
                    failure = evidence_reason
                elif aligned is not None and proposals is not None:
                    batch_proposal = proposals[aligned[index]]
                    try:
                        value = Proposal(
                            batch_proposal.keep,
                            batch_proposal.replacement,
                            batch_proposal.needs_wider_edit,
                        )
                        proposal_record = {
                            "keep": value.keep,
                            "replacement": value.replacement,
                            "needs_wider_edit": value.needs_wider_edit,
                        }
                        decision = accept(
                            text,
                            candidate,
                            value,
                            corpus,
                            intervals,
                            left=item.left,
                            right=item.right,
                            left_bounds=item.left_bounds,
                            right_bounds=item.right_bounds,
                        )
                        acceptance = {
                            "accepted": decision.accepted,
                            "reason": decision.reason,
                            "replacement": decision.replacement,
                        }
                    except RuntimeCorpusError as exc:
                        # A known technical evidence error escapes the
                        # per-proposal generic catch: it aborts the entire
                        # optional pass from the immutable original, with the
                        # actual review attempt and known/unknown usage
                        # preserved (order 016-a, requirement 4).
                        late_aborted = True
                        evidence_reason = exc.reason
                        acceptance = {
                            "accepted": False,
                            "reason": "evidence-abort",
                            "replacement": None,
                        }
                        failure = exc.reason
                    except request_lifecycle.RequestCancelled:
                        # A dedicated cancellation is not individual
                        # isolation: it aborts the whole optional
                        # pass to the handler (order 030-a,
                        # requirement 6).
                        raise
                    except Exception:
                        # Individual isolation: a failed acceptance never
                        # invalidates the other independent proposals.
                        failure = "reviewer-error"
                else:
                    # Whole-batch failure: byte-exact original, no edit.
                    failure = "reviewer-error"
                decisions.append(
                    {
                        "candidate": candidate.as_dict(),
                        "evidence": candidate.evidence,
                        "neighbors": {"left": item.left, "right": item.right},
                        "proposal": proposal_record,
                        "acceptance": acceptance,
                        "failure": failure,
                        "timing_seconds": round(time.monotonic() - review_started, 6),
                    }
                )
                if acceptance["accepted"] and isinstance(acceptance["replacement"], str):
                    edits.append((candidate.start, candidate.end, acceptance["replacement"]))
                    composed_records.append(
                        composed_validation.ComposedRecord(
                            decision_index=len(decisions) - 1,
                            start=candidate.start,
                            end=candidate.end,
                            text=candidate.text,
                            replacement=acceptance["replacement"],
                            left=item.left,
                            right=item.right,
                            left_bounds=item.left_bounds,
                            right_bounds=item.right_bounds,
                        )
                    )
            if not late_aborted and edits:
                self._raise_if_cancelled()
                # Composed-result validation (order 027-a, requirements
                # 3-5): every individually accepted edit is re-validated
                # against the original with all other surviving edits
                # composed, coordinates rebased arithmetically by the
                # exact original edits.  An edit whose positive local
                # support depended on a neighbor token that another
                # surviving edit replaces is rejected with the group and
                # rebuilt from the original; independently supported
                # compatible edits survive.  A known technical/budget
                # failure here is the same late boundary as during
                # per-proposal acceptance: abort the whole optional pass
                # from the immutable original (requirement 8).
                try:
                    surviving = composed_validation.validate_composed(
                        text, composed_records, corpus, intervals
                    )
                except RuntimeCorpusError as exc:
                    late_aborted = True
                    evidence_reason = exc.reason
                except composed_validation.ComposedMappingError:
                    # An unprovable validation mapping abstains the WHOLE
                    # optional pass to the conservatively completed
                    # original: no edit - not even an independently
                    # supported one - survives, while the actual review
                    # call and known/unknown usage stay recorded (order
                    # 027-a, requirement 6).  Caller control and MemoryError
                    # are not caught here and propagate untouched.
                    mapping_unprovable = True
                    edits = []
                    for recorded in decisions:
                        recorded["acceptance"] = {
                            "accepted": False,
                            "reason": (
                                composed_validation.COMPOSED_MAPPING_UNPROVABLE
                            ),
                            "replacement": None,
                        }
                        recorded["failure"] = (
                            composed_validation.COMPOSED_MAPPING_UNPROVABLE
                        )
                else:
                    for index, record in enumerate(composed_records):
                        if index not in surviving:
                            recorded = decisions[record.decision_index]
                            recorded["acceptance"] = {
                                "accepted": False,
                                "reason": composed_validation.COMPOSED_REJECTED,
                                "replacement": None,
                            }
                            recorded["failure"] = (
                                composed_validation.COMPOSED_REJECTED
                            )
                    edits = [
                        (record.start, record.end, record.replacement)
                        for index, record in enumerate(composed_records)
                        if index in surviving
                    ]
            if late_aborted:
                # Whole optional pass aborted from a technical evidence
                # failure: the immutable original is delivered and no
                # earlier accepted edit survives.  Every persisted decision
                # reflects that final disposition (not accepted); the
                # reviewer proposals are retained separately as untrusted
                # data, and the actual review call and known/unknown usage
                # stay recorded.
                edits = []
                for recorded in decisions:
                    recorded["acceptance"] = {
                        "accepted": False,
                        "reason": "evidence-abort",
                        "replacement": None,
                    }
                    recorded["failure"] = evidence_reason
            repaired = apply_edits(text, edits, intervals) if edits else text
            protected_changes = 0
            for interval in intervals:
                shift = sum(
                    len(replacement) - (end - start)
                    for start, end, replacement in edits
                    if end <= interval.start
                )
                start = interval.start + shift
                if (
                    repaired[start : start + interval.end - interval.start]
                    != text[interval.start : interval.end]
                ):
                    protected_changes += 1
        except RuntimeCorpusError as exc:
            return self._fail_closed_evidence(text, raw_started, exc.reason, "early")
        except BaseException as exc:
            first_control = exc
            raise
        finally:
            if corpus is not None:
                try:
                    corpus.close()
                except BaseException as close_exc:
                    if first_control is None:
                        # Clean flow: close controls still propagate.
                        raise close_exc
                    if not request_lifecycle.is_nonordinary_control(
                        first_control
                    ) and request_lifecycle.is_nonordinary_control(
                        close_exc
                    ):
                        # An ordinary pending (or cancellation) never
                        # acquires authority to discard the first
                        # nonordinary caller control: the later
                        # nonordinary cleanup control propagates
                        # (order 030-a, requirement 5).
                        raise close_exc
                    # The pending first control keeps its exact
                    # identity: a nonordinary first over any close
                    # fault, and an ordinary pending over an ordinary
                    # cleanup fault (never replaced, never converted
                    # into a generated failure).
        if self._details is not None:
            detail: dict[str, object] = {
                    "original": text,
                    "repaired": repaired,
                    "candidates": [candidate.as_dict() for candidate in candidates],
                    "corpus_summaries": [candidate.evidence for candidate in candidates],
                    "reviewer_decisions": decisions,
                    "acceptance": [item["acceptance"] for item in decisions],
                    "replacements": [item[2] for item in edits],
                    "timings": {
                        "raw_seconds": 0.0,
                        "detector_seconds": round(detector_seconds, 6),
                        "reviewer_seconds": round(reviewer_seconds, 6),
                        "repair_seconds": round(time.monotonic() - raw_started, 6),
                    },
                    "protected_difference_count": protected_changes,
                    "review_batch": review_batch,
                    "review_batches": review_batches,
                }
            if late_aborted:
                # Recorded with the actual review attempt preserved (calls
                # and known/unknown usage); never a Pandoc failure and never
                # zeroed (order 016-a, requirement 4).
                detail["evidence_failure"] = {
                    "reason": evidence_reason,
                    "stage": "late",
                }
            if mapping_unprovable:
                # The actual review attempt stays recorded (calls and
                # known/unknown usage); the whole optional pass abstained
                # to the completed original because a validation mapping
                # could not be proved (order 027-a, requirement 6).
                detail["evidence_failure"] = {
                    "reason": composed_validation.COMPOSED_MAPPING_UNPROVABLE,
                    "stage": "late",
                }
            self._details.append(detail)
        self._repair_outcome = (text, tuple(edits), repaired)
        return repaired


# Order 041-a, R2/R3/R4: the final-outcome ledger on the engine is an
# OPTIONAL interface.  Legacy engines (including the existing minimal
# concept tests) implement only repair(): they hold no request-local
# outcome and prove no mapping.  The handler tolerates their absence
# with fixed content-free decisions - never a broad catch or a
# swallowed control - and an engine that implements the interface
# (the actual RepairEngine) behaves exactly as before, including the
# once-per-request drain.
def _unavailable_details_hook(
    original: object,
    delivered: object,
    output_index: object,
    content_index: object,
) -> dict[str, object]:
    # The fixed content-free unavailable object for an opted-in
    # request whose engine cannot prove the final edit mapping: the
    # answer text, the review calls and every other wire decision are
    # unchanged (order 041-a, R2/R4).
    return repair_details.unavailable(
        repair_details.EDIT_MAPPING_UNAVAILABLE
    )


def _details_hook_for(
    engine: object, details_on: int
) -> Callable[[str, str, int | None, int | None], dict[str, object]] | None:
    # Off (0): no hook, byte-identical legacy transform.  On (1): the
    # engine's own once-per-request hook when it implements the
    # optional ledger, else the fixed unavailable-object hook.
    if not details_on:
        return None
    take_details = getattr(engine, "take_repair_details", None)
    return take_details if callable(take_details) else _unavailable_details_hook


def _drain_repair_outcome(engine: object) -> None:
    # Drain the request-local final outcome only from engines that
    # implement the optional ledger: a legacy engine holds nothing, so
    # its prior 200 path and bytes stand unchanged (order 041-a, R3).
    drain = getattr(engine, "take_repair_outcome", None)
    if callable(drain):
        drain()


class _Handler(BaseHTTPRequestHandler):
    server_version = "concept-verification/1"
    _intake: client_transport.IntakeGuard | None = None
    _delivery: delivery_transport.DeliveryGuard | None = None
    _emission_begun: bool = False

    def handle_one_request(self) -> None:
        # One anchored incoming-intake deadline BEFORE any
        # request-line/header/body read and before the guard starts
        # (order 020-a, requirement 1); OS accept-backlog waiting is
        # outside this scope.  The stdlib finite line/header-count
        # guards remain.
        owner = cast(ConceptProxy, self.server)
        self._intake = client_transport.IntakeGuard(
            self.connection, owner.client_timeout
        )
        try:
            super().handle_one_request()
        finally:
            # Release and actually join the owned intake guard on
            # every path: success, EOF, timeout, unsupported
            # method/path, framing rejection, parse failure,
            # exception or startup failure.  No intake worker
            # survives the request (order 020-a, requirement 2).
            if self._intake is not None:
                self._intake.release()
                self._intake = None

    def _deliver(self, emit: Callable[[], None]) -> None:
        """Emit one post-intake response under one delivery deadline.

        One absolute monotonic deadline anchors immediately before
        this emission's status/headers/body/flush and before the
        owned guard starts; response-ready work (upstream capture,
        optional repair) is outside the phase (order 021-a,
        requirement 2).  Byte progress, short writes and internal
        retries never reset it.  On expiry or a write/flush/
        early-disconnect failure the connection closes quietly: no
        body is logged, no second response/status/fallback is
        attempted on this socket, and a partially written body is
        never labelled completed (order 021-a, requirement 4).  The
        guard is cancelled and actually joined on every path before
        this handler returns to the next connection (order 021-a,
        requirement 3).
        """
        owner = cast(ConceptProxy, self.server)
        # No-retry ownership marker BEFORE any fallible delivery
        # setup (order 021-a, requirement 4): a delivery attempt
        # that fails before bytes - including a guard startup
        # failure - closes safely and never triggers another
        # response or a second delivery attempt on this socket.
        self._emission_begun = True
        try:
            guard = delivery_transport.DeliveryGuard(
                self.connection, owner.delivery_timeout
            )
        except Exception:
            # Guard startup failure (partial startup already
            # cancelled and actually joined inside the guard):
            # quiet close, no second response, no retry.
            self.close_connection = True
            return
        self._delivery = guard
        try:
            if guard.is_expired():
                # A successful guard startup that already consumed the
                # whole budget expires the anchored absolute clock
                # BEFORE emission can begin: the response must not be
                # started (late or racing completion loses to expiry,
                # order 021-a, requirement 2).  Close quietly without
                # any status/headers/body; the owned guard is still
                # cancelled and actually joined in the finally below.
                self.close_connection = True
                return
            emit()
            # The common explicit flush belongs inside the common
            # delivery lifecycle so every post-intake response -
            # normal emission, contract 400, protocol/generated 502 -
            # classifies its actual flush against the one delivery
            # clock (order 021-a, requirements 2 and 4).  On the
            # native unbuffered _SocketWriter this flush is a no-op
            # (the bytes were already sent by write), and later
            # stdlib unbuffered flushes are likewise no-ops; it is
            # the phase-ownership point, not a network barrier.
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
            # write/flush failure or early client disconnect: quiet
            # close, no duplicate response, no body logging.
            self.close_connection = True
        finally:
            if guard.release():
                # The absolute deadline passed before the emission
                # completed (or a racing completion lost to it): the
                # partial (if any) body is never completed and no
                # second response is attempted on this socket.
                self.close_connection = True

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/responses":
            self.send_error(404)
            return
        owner = cast(ConceptProxy, self.server)
        # Strict unambiguous framing before any body read or
        # forwarding (order 020-a, requirement 3): repeated
        # Content-Length fields (even equal), comma lists, any
        # Transfer-Encoding and any unsupported Expect are fixed
        # content-free 400 rejections with connection close; a
        # missing length, a nondecimal value (including signed) or
        # an overlarge length keeps the existing bounded 413 size
        # policy.
        framing = client_transport.check_request_framing(self.headers)
        if isinstance(framing, client_transport.FramingRejection):
            if framing.status == 413:
                self.send_error(413)
            else:
                self._send_intake_error(400, framing.code)
            self.close_connection = True
            return
        _zero, length, _empty = framing
        # Read exactly the advertised length under the one anchored
        # intake deadline; the owned guard unblocks a waiting read by
        # shutting down only this accepted socket object (order
        # 020-a, requirements 2 and 4).
        body = self.rfile.read(length)
        # Release and actually join the intake guard BEFORE request
        # validation, forwarding or engine work (order 020-a,
        # requirement 5): the timer cannot fire later during
        # legitimate slow main/review work or interrupt another
        # connection.  A late or racing completion loses to the
        # expired clock and can never reach main.
        if self._intake is not None and self._intake.release():
            # Bounded best-effort 408 on an interrupted connection;
            # never blocks indefinitely, then closes with zero main,
            # analysis, evidence or review calls.
            self._send_intake_error(408, "REQUEST_INTAKE_TIMEOUT")
            self.close_connection = True
            return
        if len(body) != length:
            # EOF (or the guard's interruption) delivered fewer bytes
            # than advertised: a complete valid JSON prefix is NOT a
            # complete advertised body.  Fixed content-free 400 where
            # delivery is possible; always closes (order 020-a,
            # requirement 4).  Bytes beyond the one advertised
            # request can never become a second upstream request
            # (HTTP/1.0 close, no pipelining).
            self._send_intake_error(400, "REQUEST_BODY_INCOMPLETE")
            self.close_connection = True
            return
        request_id = self.headers.get("X-Concept-Request-ID", "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", request_id):
            request_id = uuid.uuid4().hex
        scope: request_lifecycle.CancellationScope | None = None
        started = time.monotonic()
        # The actual pending control of THIS handler operation,
        # tracked explicitly (order 030-a, requirement 5): only a
        # control actually propagating out of this operation may
        # authorize suppression of a new cleanup control in the
        # owned finisher.  An unrelated already-handled ambient
        # exception is never borrowed - sys.exc_info can return
        # one from an enclosing handled context, and passing it
        # would wrongly suppress this operation's own cleanup
        # control.
        first_control: BaseException | None = None
        try:
            try:
                try:
                    contract = validate_request(body)
                except RequestContractError as exc:
                    # Bounded content-free rejection before any upstream or
                    # reviewer call: unsupported capabilities and invalid
                    # requests never reach main generation (order 015-a,
                    # requirement 2).  This post-intake emission runs
                    # under the one delivery deadline (order 021-a,
                    # requirement 4).  The fixed validated reason is
                    # bound to an ordinary local before the callback:
                    # the except-as variable is deleted at block end and
                    # must not be closed over.
                    reason = exc.reason
                    self._deliver(
                        lambda: self._send_error_json(
                            400, "invalid_request", reason
                        )
                    )
                    return
                # Order 041-a, R1: the per-request opt-in control
                # header is decided after the existing
                # admission/intake/contract precedence and before any
                # main/review work: duplicates or any other value are
                # a fixed content-free 400, never reflected.  The
                # header itself is stripped from the upstream forward
                # below, and absent/off behavior is byte-identical.
                try:
                    details_on = repair_details.parse_details_values(
                        repair_details.details_header_values(
                            list(self.headers.items())
                        )
                    )
                except repair_details.DetailsHeaderError:
                    self._deliver(
                        lambda: self._send_error_json(
                            400,
                            "invalid_request",
                            repair_details.REPAIR_DETAILS_HEADER_INVALID,
                        )
                    )
                    return
                # One request-local cancellation scope begins AFTER the
                # fully captured VALID ingress and BEFORE any main work
                # (order 030-a, requirement 1): the exact accepted
                # socket is watched by one bounded disconnect-only poll
                # watcher, the owned scope is finished on every path
                # before the handler returns to the next connection, and
                # a cancellation racing any completed transfer wins
                # before the transfer (requirement 4).
                scope = request_lifecycle.CancellationScope(self.connection)
                scope.start()
                if hasattr(owner.engine, "begin_request"):
                    owner.engine.begin_request(scope)
                scope.raise_if_cancelled()
                forwarded = owner.forward(
                    body,
                    dict(
                        repair_details.remove_details_header(
                            list(self.headers.items())
                        )
                    ),
                )
                scope.raise_if_cancelled()
                raw_seconds = time.monotonic() - started
                repaired = forwarded.body
                if forwarded.status != 200:
                    # Order 035-c R3: remove defined logprobs from recognized
                    # non-200 responses by ACTUAL content type (independent
                    # of request.stream).  Malformed error bodies unchanged.
                    _mt = forwarded.content_type.split(";", 1)[0].strip().lower()
                    if _mt == "text/event-stream":
                        repaired = normalize_sse_body(forwarded.body)
                    elif _mt == "application/json":
                        repaired = normalize_json_body(forwarded.body)
                if forwarded.status == 200:
                    # The declared client mode must agree with the observed
                    # upstream content type; a mismatch (including an unknown
                    # type) is a bounded upstream protocol failure, never a
                    # silent mode switch (order 015-a, requirement 6).
                    media_type = forwarded.content_type.split(";", 1)[0].strip().lower()
                    if contract.stream and media_type != "text/event-stream":
                        self._deliver(
                            lambda: self._send_error_json(
                                502,
                                "upstream_protocol_error",
                                "RESPONSE_MODE_MISMATCH",
                            )
                        )
                        return
                    if not contract.stream and media_type != "application/json":
                        self._deliver(
                            lambda: self._send_error_json(
                                502,
                                "upstream_protocol_error",
                                "RESPONSE_MODE_MISMATCH",
                            )
                        )
                        return
                    details_hook = _details_hook_for(owner.engine, details_on)
                    if contract.stream:
                        repaired, _ = transform_sse(
                            forwarded.body, owner.engine.repair, details_hook
                        )
                    else:
                        repaired, _ = transform_json(
                            forwarded.body, owner.engine.repair, details_hook
                        )
                    # Drain the request-local final outcome (already
                    # consumed by the hook when opted in) so no
                    # original/repaired text is retained across
                    # requests; metadata extraction above precedes the
                    # shared trace/usage drains below.
                    _drain_repair_outcome(owner.engine)
                    scope.raise_if_cancelled()
                    if hasattr(owner.engine, "take_details") and hasattr(owner, "write_traces"):
                        details = owner.engine.take_details()
                        owner.write_traces(
                            request_id,
                            details,
                            time.monotonic() - started,
                            raw_seconds,
                        )
                        owner.observe_usage(details)
                        if hasattr(owner.engine, "mark_usage_observed"):
                            owner.engine.mark_usage_observed()
                def _emit() -> None:
                    # The delivery deadline is anchored inside _deliver
                    # immediately before this emission and before the
                    # owned guard starts (order 021-a, requirement 2);
                    # the common explicit flush in _deliver keeps the
                    # whole status/headers/body/flush phase inside the
                    # one clock.
                    content_type = (
                        forwarded.content_type or "application/octet-stream"
                    )
                    self.send_response(forwarded.status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(repaired)))
                    self.end_headers()
                    self.wfile.write(repaired)

                scope.raise_if_cancelled()
                self._deliver(_emit)
            except request_lifecycle.RequestCancelled:
                # A dedicated cancellation is caught INSIDE the owning
                # Handler before any optional-original/502 fallback and
                # before the admission worker boundary (order 030-a,
                # requirement 6): one content-free abort observation,
                # then a quiet close - no retry, no second status, no
                # cancellation-time trace on a closed client.
                if hasattr(owner.engine, "abort_request"):
                    owner.engine.abort_request(owner.observe_usage)
                self.close_connection = True
                return
            except BrokenPipeError:
                return
            except MemoryError as exc:
                # The first MemoryError of THIS operation is a
                # nonordinary control by law: it is tracked for the
                # owned finisher and propagates after complete
                # cleanup - never caught into a generated 502 and
                # never lost (order 030-a, requirement 5).
                first_control = exc
                raise
            except Exception:
                # Post-intake generated error (order 021-a, requirement
                # 4): only while no response emission has begun.  Once
                # emission began, a failed partial send is closed quietly
                # - never a second response/status/502/fallback on the
                # same socket.
                if not self._emission_begun:
                    try:
                        self._deliver(lambda: self.send_error(502))
                    except Exception:
                        self.close_connection = True
            except BaseException as exc:
                # A nonordinary control (or a MemoryError) of
                # THIS operation escaping the handling suites
                # above is the first own control: it is tracked
                # for the owned finisher and propagates after
                # complete cleanup (order 030-a, requirement 5).
                first_control = exc
                raise
        except BaseException as exc:
            # A control escaping from INSIDE a handling suite is
            # this operation's first own control as well (order
            # 030-a, requirement 5); the exact first identity is
            # kept and cleanup never replaces it.
            if first_control is None:
                first_control = exc
            raise
        finally:
            # Actually finish the owned scope on every path before
            # the handler returns to the next connection (order
            # 030-a, requirements 1 and 5): the exact owned watcher
            # is stopped and joined and the accepted-socket
            # reference is released, so no canceled-request resource
            # or text is retained idle.
            if scope is not None:
                request_lifecycle.finish_owned_scope(
                    scope, first_control
                )

    def _send_intake_error(self, status: int, code: str) -> None:
        """Bounded best-effort fixed content-free intake error.

        A short socket timeout bounds the send so an error on an
        interrupted connection never blocks indefinitely; any failure
        is suppressed and the caller closes the connection (order
        020-a, requirement 4).  No raw exception, request, credential,
        header or text is emitted.
        """
        error_type = "request_timeout" if status == 408 else "invalid_request"
        payload = json.dumps(
            {"error": {"type": error_type, "code": code}},
            separators=(",", ":"),
        ).encode()
        sock = self.connection
        previous: float | None = None
        try:
            previous = sock.gettimeout()
            sock.settimeout(1.0)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
            return
        finally:
            with contextlib.suppress(Exception):
                if previous is not None:
                    sock.settimeout(previous)

    def _send_error_json(self, status: int, error_type: str, code: str) -> None:
        payload = json.dumps(
            {"error": {"type": error_type, "code": code}}, separators=(",", ":")
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            return

    def log_message(self, *_: object) -> None:
        return


class _PassthroughHandler(_Handler):
    """Forward every route; inspect only potentially repairable Responses."""

    def _repair_candidate(self, body: bytes) -> bool:
        if self.command != "POST" or self.path.split("?", 1)[0] != "/v1/responses":
            return False
        if self.headers.get("Content-Encoding", "identity").lower() != "identity":
            return False
        try:
            request = _strict_json_body(body)
        except (ProxyError, ValueError, OverflowError, RecursionError):
            return False
        # Tools and multimodal/history input remain upstream's concern. The
        # *answer* must independently satisfy the existing plain-text contract.
        if request.get("store") is not False or request.get("background", False) is not False:
            return False
        if any(request.get(key) is not None for key in ("previous_response_id", "conversation")):
            return False
        if type(request.get("stream", False)) is not bool:
            return False
        text = request.get("text", {})
        if not isinstance(text, dict):
            return False
        for formatting in (text.get("format", {"type": "text"}),
                           request.get("response_format", {"type": "text"})):
            if not isinstance(formatting, dict) or formatting.get("type") != "text":
                return False
        return True

    def _transform(
        self,
        raw: bytes,
        media_type: str,
        scope: request_lifecycle.CancellationScope,
        started: float,
        details_on: int,
    ) -> bytes:
        owner = cast(ConceptProxy, self.server)
        # Malformed envelopes are opaque byte-identical passthrough.  A
        # well-formed non-completed or unsupported envelope never enters
        # repair, but the owner's unconditional defined-logprobs removal
        # still applies to its recognized defined locations (order 038-a
        # R6: inherited PR43 gap closed); bodies without defined logprobs
        # pass through unchanged.
        try:
            if media_type == "application/json":
                if validate_completed_envelope(_strict_json_body(raw)) is None:
                    return normalize_json_body(raw)
            elif not _sse_completed_ok(raw):
                return normalize_sse_body(raw)
        except (ProxyError, ValueError, OverflowError, RecursionError):
            return raw
        owner.engine.begin_request(scope)
        details_hook = _details_hook_for(owner.engine, details_on)
        if media_type == "application/json":
            repaired, _changed = transform_json(
                raw, owner.engine.repair, details_hook
            )
        else:
            repaired, _changed = transform_sse(
                raw, owner.engine.repair, details_hook
            )
        # Drain the request-local final outcome (already consumed by
        # the hook when opted in) before the shared trace/usage
        # drains; nothing text-bearing is retained across requests.
        _drain_repair_outcome(owner.engine)
        details = owner.engine.take_details()
        owner.write_traces(uuid.uuid4().hex, details, time.monotonic() - started)
        owner.observe_usage(details)
        owner.engine.mark_usage_observed()
        return repaired

    def _relay(self) -> None:
        owner = cast(ConceptProxy, self.server)
        started = time.monotonic()
        connection: http.client.HTTPConnection | None = None
        response: http.client.HTTPResponse | None = None
        guard: upstream_setup.SetupGuard | None = None
        scope: request_lifecycle.CancellationScope | None = None
        first_control: BaseException | None = None
        # Cumulative ACTIVE client-write time of the prefix emission
        # phase (order 038-a R5, G2 review): only real write/flush
        # time counts - upstream and review waits are excluded - so a
        # healthy slow client and a slow upstream are both allowed
        # while a stalled client cannot hold the stream past the
        # delivery budget.  Initialized here (before any fallible
        # setup) so every cleanup path can read it.
        prefix_write_spent = 0.0
        try:
            try:
                path = passthrough.request_path(owner.upstream.path, self.path)
                body = passthrough.read_request(self)
            except OverflowError:
                self._send_intake_error(413, "REQUEST_TOO_LARGE")
                return
            except ValueError:
                self._send_intake_error(400, "REQUEST_FRAMING_INVALID")
                return
            if self._intake is not None and self._intake.release():
                self._send_intake_error(408, "REQUEST_INTAKE_TIMEOUT")
                return
            # Order 041-a, R1: on the Responses route the opt-in
            # control header is decided after the existing
            # admission/intake precedence and before any main/review
            # work (duplicates or any other value: fixed content-free
            # 400, never reflected, zero main); on every other route it
            # is only stripped from the relay below.
            details_on = 0
            if (
                self.command == "POST"
                and self.path.split("?", 1)[0] == "/v1/responses"
            ):
                try:
                    details_on = repair_details.parse_details_values(
                        repair_details.details_header_values(
                            list(self.headers.items())
                        )
                    )
                except repair_details.DetailsHeaderError:
                    self._deliver(
                        lambda: self._send_error_json(
                            400,
                            "invalid_request",
                            repair_details.REPAIR_DETAILS_HEADER_INVALID,
                        )
                    )
                    return
            scope = request_lifecycle.CancellationScope(self.connection)
            scope.start()
            cancelled = scope.cancelled
            # End-to-end headers (including authorization and vendor headers)
            # survive. Hop-by-hop headers, Host, and framing are reconstructed.
            excluded = passthrough.hop_headers(self.headers) | {"host", "content-length"}
            headers = repair_details.remove_details_header(
                [(key, value) for key, value in self.headers.items()
                 if key.lower() not in excluded]
            )
            if (body or self.command in {"POST", "PUT", "PATCH"}
                    or self.headers.get("Content-Length")):
                headers.append(("Content-Length", str(len(body))))
            connection = upstream_setup.open_upstream_connection(
                scheme=owner.upstream.scheme, host=cast(str, owner.upstream.hostname),
                port=owner.upstream.port or (443 if owner.upstream.scheme == "https" else 80),
                request_path=path, body=body, headers=headers,
                deadline=time.monotonic() + owner.setup_timeout,
                capture_timeout=owner.capture_timeout, cancelled=cancelled,
                method=self.command,
            )
            sock = connection.sock
            response = header_transport.acquire_response(
                connection, owner.header_timeout, cancelled=cancelled,
            )
            guard = upstream_setup.SetupGuard(time.monotonic() + owner.capture_timeout)
            guard.adopt_cancelled(cancelled)
            if sock is not None and not guard.adopt(sock):
                raise ProxyError("upstream body deadline expired")
            media_type = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
            inspect_answer = (
                self._repair_candidate(body) and response.status == 200
                and media_type in {"application/json", "text/event-stream"}
                and response.getheader("Content-Encoding", "identity").lower() == "identity"
            )
            prefix = b""
            buffered = False
            transformed = False
            streamed = False
            if inspect_answer:
                chunks: list[bytes] = []
                size = 0
                sse = media_type == "text/event-stream"
                # Early-reasoning streaming state (order 038-a R1):
                # while the body is still being captured, each newly
                # complete proven prefix frame is emitted to the
                # client; the first unproven frame is the one-way
                # buffer boundary.  The PR43 bounded read1 loop, its
                # oversize raw fallback and the guard primitives are
                # preserved (order 038-a R3, decision D0).
                decoded = ""
                pending = b""
                sweep_dead = False
                emitted_chars = 0
                emitted_bytes = 0
                boundary: int | None = None
                boundary_ordinal = 0
                sent_blocks: list[str] = []

                def guard_client_phase(run: Callable[[], None]) -> None:
                    """Run one client-emission phase (status/headers or
                    one prefix frame) under the cumulative ACTIVE
                    delivery budget (order 038-a R5, G2 review): each
                    phase arms one owned guard with the REMAINING
                    budget, so only real client write/flush time
                    counts - upstream and review waits are excluded
                    and never arm or extend the clock.  On expiry the
                    guard shuts down only the captured client socket
                    and a late or racing completion loses to
                    expiration; the guard is released and actually
                    joined on every path."""
                    nonlocal prefix_write_spent
                    remaining = owner.delivery_timeout - prefix_write_spent
                    if remaining <= 0:
                        raise ProxyError(
                            "client prefix delivery budget exceeded"
                        )
                    guard = delivery_transport.DeliveryGuard(
                        self.connection, remaining
                    )
                    started = time.monotonic()
                    try:
                        run()
                        expired = guard.release()
                    except BaseException:
                        guard.release()
                        raise
                    finally:
                        prefix_write_spent += time.monotonic() - started
                    if expired:
                        self.close_connection = True
                        raise ProxyError(
                            "client prefix delivery budget exceeded"
                        )

                def begin_streaming() -> None:
                    """Send status and headers exactly once, before
                    any prefix frame.  Validator headers are always
                    excluded: the tail may be re-rendered."""
                    nonlocal streamed
                    assert response is not None
                    self._emission_begun = True
                    self.close_connection = True

                    def send_headers() -> None:
                        assert response is not None
                        self.send_response_only(response.status, response.reason)
                        excluded = (
                            passthrough.hop_headers(response.headers)
                            | {"content-length", "etag", "content-md5",
                               "digest", "content-digest"}
                        )
                        for key, value in response.getheaders():
                            if key.lower() not in excluded:
                                self.send_header(key, value)
                        self.send_header("Connection", "close")
                        self.end_headers()

                    guard_client_phase(send_headers)
                    streamed = True

                def sweep_frames(frame_chunk: bytes) -> None:
                    """Emit each newly complete proven prefix frame;
                    record the first unproven frame as the boundary.
                    Emission stops permanently on the boundary, on a
                    mid-stream decode failure, or once the body is
                    known oversized (raw fallback)."""
                    nonlocal decoded, pending, sweep_dead
                    nonlocal emitted_chars, emitted_bytes
                    nonlocal boundary, boundary_ordinal
                    if sweep_dead:
                        return
                    try:
                        text, pending = capture_transport.utf8_split(
                            pending + frame_chunk
                        )
                    except capture_transport.CaptureError:
                        # Mid-stream encoding failure: stop emitting;
                        # the whole-body outcome path decides below.
                        sweep_dead = True
                        return
                    if text:
                        decoded += text
                    if boundary is not None:
                        sweep_dead = True
                        return

                    def emit_frame(block: str, delim: str) -> None:
                        nonlocal emitted_bytes, boundary, boundary_ordinal
                        if boundary is not None:
                            return
                        decision = sse_streaming.classify_prefix_frame(
                            block, _event
                        )
                        if decision.kind != "prefix":
                            # One-way boundary: this and every later
                            # frame are buffered (no replay, no
                            # reorder, no reclassification).
                            boundary = emitted_bytes
                            boundary_ordinal = len(sent_blocks)
                            return
                        if not streamed:
                            begin_streaming()
                        if decision.value is not None:
                            stripped = sse_streaming.strip_event_logprobs(
                                decision.value
                            )
                            rendered = (
                                _render_event(block, decision.value)
                                if stripped
                                else block
                            ) + delim
                        else:
                            rendered = block + delim
                        sent_blocks.append(rendered)
                        frame_bytes = rendered.encode("utf-8")
                        guard_client_phase(
                            lambda: (self.wfile.write(frame_bytes),
                                     self.wfile.flush())
                        )
                        emitted_bytes += len((block + delim).encode("utf-8"))

                    # Only the not-yet-emitted suffix is scanned, so
                    # frame delivery is once-only and in stream order.
                    suffix = decoded[emitted_chars:]
                    emitted_chars += sse_streaming.emit_new_frames(
                        suffix, 0, emit_frame
                    )
                    if boundary is not None:
                        sweep_dead = True

                while size <= MAIN_BODY_MAX_BYTES:
                    scope.raise_if_cancelled()
                    chunk = response.read1(min(65536, MAIN_BODY_MAX_BYTES + 1 - size))
                    if not chunk:
                        if response.length not in (None, 0):
                            raise ProxyError("incomplete upstream body")
                        buffered = True
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                    if sse:
                        sweep_frames(chunk)
                        if size > MAIN_BODY_MAX_BYTES:
                            # The PR43 oversize fallback decides
                            # below; no further frame may be emitted.
                            sweep_dead = True
                prefix = b"".join(chunks)
                if buffered:
                    if guard.release():
                        raise ProxyError("upstream body deadline expired")
                    guard = None
                if sse and streamed:
                    # Streaming outcome: status and the proven prefix
                    # frames are already with the client; only the
                    # tail is decided here, then delivered under one
                    # fresh delivery deadline.
                    scope.raise_if_cancelled()
                    live = not buffered
                    tail: bytes | None
                    if not buffered:
                        # PR43 oversize fallback preserved: no
                        # repair, raw continuation from the first
                        # unsent byte (zero review).
                        tail = prefix[emitted_bytes:]
                    elif boundary is None:
                        # Frames but no boundary (no terminal, no
                        # answer frame): nothing more is sent and no
                        # completion is fabricated.
                        tail = None
                    else:
                        scope.raise_if_cancelled()
                        if _sse_completed_ok(prefix):
                            owner.engine.begin_request(scope)
                            details_hook = _details_hook_for(
                                owner.engine, details_on
                            )
                            try:
                                blocks_out, _changed = transform_sse_blocks(
                                    prefix,
                                    owner.engine.repair,
                                    details_hook,
                                )
                                # Ordinal validation: the sent prefix
                                # must equal the transform's own
                                # per-block output frame by frame
                                # (frame identity/ordinal mapping, not
                                # byte slicing; order 038-a R1).
                                if blocks_out[:boundary_ordinal] != sent_blocks:
                                    raise ProxyError(
                                        "streaming prefix ordinal mismatch"
                                    )
                                tail = "".join(
                                    blocks_out[boundary_ordinal:]
                                ).encode("utf-8")
                            except (
                                ProxyError, ValueError, OverflowError, RecursionError
                            ) as exc:
                                owner.engine.abort_request(owner.observe_usage)
                                if isinstance(exc, ProxyError):
                                    tail = normalize_sse_body(prefix[boundary:])
                                else:
                                    tail = None
                            else:
                                _drain_repair_outcome(owner.engine)
                                details = owner.engine.take_details()
                                owner.write_traces(
                                    uuid.uuid4().hex,
                                    details,
                                    time.monotonic() - started,
                                )
                                owner.observe_usage(details)
                                owner.engine.mark_usage_observed()
                                transformed = True
                        else:
                            # No valid completed envelope: the tail
                            # passes through from the boundary with the
                            # owner's unconditional defined-logprobs
                            # removal (order 038-a R6); a malformed tail
                            # stays byte-identical.
                            tail = normalize_sse_body(prefix[boundary:])
                    if tail is None:
                        # Honest post-header failure: quiet close, no
                        # second response, no fabricated completion.
                        self.close_connection = True
                        return
                    scope.raise_if_cancelled()

                    def emit_tail() -> None:
                        assert response is not None
                        self.wfile.write(tail)
                        self.wfile.flush()
                        if live:
                            while True:
                                scope.raise_if_cancelled()
                                chunk = response.read1(65536)
                                if not chunk:
                                    if response.length not in (None, 0):
                                        raise ProxyError(
                                            "incomplete upstream body"
                                        )
                                    break
                                self.wfile.write(chunk)
                                self.wfile.flush()

                    self._deliver(emit_tail)
                    return
                if buffered:
                    scope.raise_if_cancelled()
                    original = prefix
                    prefix = self._transform(
                        prefix, media_type, scope, started, details_on
                    )
                    transformed = prefix != original
            elif (
                self.command == "POST"
                and self.path.split("?", 1)[0] == "/v1/responses"
                and media_type == "application/json"
                and response.getheader(
                    "Content-Encoding", "identity"
                ).lower() == "identity"
                and (
                    response.status != 200
                    or not self._repair_candidate(body)
                )
            ):
                # Owner's unconditional defined-logprobs policy
                # (order 038-a R6): a recognized JSON response on the
                # Responses route that does not take the repair path
                # (a non-200, or a 200 request bypass) is captured
                # under the same bounded capture and normalized;
                # malformed bodies stay byte-identical, an over-limit
                # body falls back to the raw continuation, and a
                # truncated body is an observable main failure.
                # Neither path adds repair or review, and SSE of any
                # status keeps its raw early passthrough.
                chunks: list[bytes] = []
                size = 0
                oversize = False
                while size <= MAIN_BODY_MAX_BYTES:
                    scope.raise_if_cancelled()
                    chunk = response.read1(
                        min(65536, MAIN_BODY_MAX_BYTES + 1 - size)
                    )
                    if not chunk:
                        if response.length not in (None, 0):
                            raise ProxyError("incomplete upstream body")
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > MAIN_BODY_MAX_BYTES:
                        oversize = True
                        break
                if oversize:
                    # PR43-style raw continuation: emit the captured
                    # prefix and relay the remainder unchanged.
                    prefix = b"".join(chunks)
                else:
                    if guard.release():
                        raise ProxyError("upstream body deadline expired")
                    guard = None
                    body = b"".join(chunks)
                    prefix = normalize_json_body(body)
                    transformed = prefix != body
                    buffered = True
            elif (
                self.command == "POST"
                and self.path.split("?", 1)[0] == "/v1/responses"
                and media_type == "text/event-stream"
                and response.getheader(
                    "Content-Encoding", "identity"
                ).lower() == "identity"
            ):
                # Owner's unconditional defined-logprobs policy
                # (order 038-a R6): an SSE response on the Responses
                # route that does not take the repair path keeps its
                # raw early passthrough under a bounded byte-frame
                # filter: frames are split on the original bytes at
                # the strict LF/CRLF delimiters, a complete frame is
                # strictly decoded and parsed, defined logprobs are
                # removed at the shared locations, and the frame is
                # re-rendered only when changed.  Only the original
                # unterminated tail is retained, bounded by
                # MAIN_BODY_MAX_BYTES.  A malformed or undecodable
                # frame, or over-limit tail growth, switches the
                # remainder to verbatim relay (one-way, original byte
                # order, the retained tail flushed exactly once);
                # frames are never reordered, dropped or re-decoded
                # and no terminal event is invented.
                scope.raise_if_cancelled()
                raw_mode = False
                pending: bytes = b""

                def emit_filtered() -> None:
                    assert response is not None
                    nonlocal raw_mode, pending
                    self.send_response_only(response.status, response.reason)
                    excluded = (
                        passthrough.hop_headers(response.headers)
                        | {"content-length", "etag", "content-md5",
                           "digest", "content-digest"}
                    )
                    for key, value in response.getheaders():
                        if key.lower() not in excluded:
                            self.send_header(key, value)
                    self.send_header("Connection", "close")
                    self.end_headers()

                    def on_frame(frame: bytes) -> None:
                        """Filter one complete delimited frame (bytes,
                        delimiter included); unfiltered frames pass
                        through byte-identical."""
                        nonlocal raw_mode
                        if not raw_mode:
                            if frame.endswith(b"\r\n\r\n"):
                                block_bytes, delim = frame[:-4], b"\r\n\r\n"
                            else:
                                block_bytes, delim = frame[:-2], b"\n\n"
                            try:
                                block = block_bytes.decode("utf-8")
                                value = _event(block)
                            except (
                                UnicodeDecodeError, ProxyError, ValueError,
                                OverflowError, RecursionError,
                            ):
                                raw_mode = True
                            else:
                                if (
                                    value is not None
                                    and sse_streaming.strip_event_logprobs(value)
                                ):
                                    frame = (
                                        _render_event(block, value).encode("utf-8")
                                        + delim
                                    )
                        self.wfile.write(frame)
                        self.wfile.flush()

                    def scan(data: bytes) -> None:
                        nonlocal raw_mode, pending
                        if raw_mode:
                            self.wfile.write(data)
                            self.wfile.flush()
                            return
                        buf = pending + data
                        pending = b""
                        last = 0
                        for match in _SSE_FRAME_DELIMITERS.finditer(buf):
                            on_frame(buf[last:match.end()])
                            last = match.end()
                            if raw_mode:
                                # One-way verbatim fallback: the
                                # untouched remainder flushes exactly
                                # once, in original order.
                                self.wfile.write(buf[last:])
                                self.wfile.flush()
                                return
                        pending = buf[last:]
                        if len(pending) > MAIN_BODY_MAX_BYTES:
                            # Delimiter-free growth past the bound:
                            # coherent verbatim over-limit fallback.
                            self.wfile.write(pending)
                            self.wfile.flush()
                            pending = b""
                            raw_mode = True

                    while True:
                        scope.raise_if_cancelled()
                        chunk = response.read1(65536)
                        if not chunk:
                            if response.length not in (None, 0):
                                raise ProxyError("incomplete upstream body")
                            if pending:
                                self.wfile.write(pending)
                                self.wfile.flush()
                                pending = b""
                            break
                        scan(chunk)

                self._deliver(emit_filtered)
                return
            scope.raise_if_cancelled()

            def emit() -> None:
                assert response is not None
                self.send_response_only(response.status, response.reason)
                excluded = passthrough.hop_headers(response.headers) | {"content-length"}
                if transformed:
                    # A transformed entity cannot retain validators for the
                    # original bytes. Unrelated metadata headers are preserved.
                    excluded |= {"etag", "content-md5", "digest", "content-digest"}
                for key, value in response.getheaders():
                    if key.lower() not in excluded:
                        self.send_header(key, value)
                if buffered:
                    self.send_header("Content-Length", str(len(prefix)))
                elif response.getheader("Content-Length") is not None:
                    self.send_header(
                        "Content-Length", cast(str, response.getheader("Content-Length")),
                    )
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command == "HEAD":
                    return
                self.wfile.write(prefix)
                self.wfile.flush()
                if not buffered:
                    while True:
                        scope.raise_if_cancelled()
                        chunk = response.read1(65536)
                        if not chunk:
                            if response.length not in (None, 0):
                                raise ProxyError("incomplete upstream body")
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()

            self.close_connection = True
            self._deliver(emit)
        except request_lifecycle.RequestCancelled:
            owner.engine.abort_request(owner.observe_usage)
            self.close_connection = True
        except MemoryError as exc:
            first_control = exc
            raise
        except Exception:
            if not self._emission_begun:
                self._deliver(
                    lambda: self._send_error_json(502, "upstream_error", "UPSTREAM_FAILED")
                )
            self.close_connection = True
        except BaseException as exc:
            first_control = exc
            raise
        finally:
            with contextlib.ExitStack() as cleanup:
                if scope is not None:
                    cleanup.callback(request_lifecycle.finish_owned_scope, scope, first_control)
                if connection is not None:
                    cleanup.callback(connection.close)
                if response is not None:
                    cleanup.callback(response.close)
                if guard is not None:
                    cleanup.callback(guard.release)

    do_GET = _relay
    do_HEAD = _relay
    do_POST = _relay
    do_PUT = _relay
    do_PATCH = _relay
    do_DELETE = _relay
    do_OPTIONS = _relay


class ConceptProxy(HTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        upstream: str,
        engine: RepairEngine,
        timeout: float = 30.0,
        trace_dir: Path | None = None,
        usage_observer: Callable[[dict[str, object]], None] | None = None,
        *,
        header_timeout: float = MAIN_HEADER_TIMEOUT_SECONDS,
        client_timeout: float = CLIENT_REQUEST_TIMEOUT_SECONDS,
        delivery_timeout: float = CLIENT_DELIVERY_TIMEOUT_SECONDS,
        setup_timeout: float = MAIN_SETUP_TIMEOUT_SECONDS,
        passthrough_mode: bool = False,
    ):
        if address[0] != "127.0.0.1" and not passthrough_mode:
            raise ValueError("strict proxy must bind to loopback")
        parsed = urlparse(upstream)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("upstream must be an explicit HTTP URL")
        if parsed.port == 0:
            # Keep the full 16-bit port meaning: an explicit :0 is
            # rejected, never aliased to the scheme default (order
            # 022-a, requirement 4).
            raise ValueError("upstream port must be an explicit 1-65535")
        # Independent finite positive elapsed header budget (order
        # 018-a, requirement 2): separate from the socket/body capture
        # timeout; rejected before any bind allocation or traffic is
        # accepted, and validated again at guard use.
        self.header_timeout = header_transport.validate_header_budget(
            header_timeout
        )
        # Independent finite positive elapsed incoming-intake budget
        # (order 020-a, requirement 1): separate from the socket/body
        # capture timeout, the header budget and the fixed 15 s
        # reviewer budget; rejected before any bind allocation or
        # traffic is accepted, and validated again at guard use.
        self.client_timeout = client_transport.validate_client_budget(
            client_timeout
        )
        # Independent finite positive elapsed final-delivery budget
        # (order 021-a, requirement 1): bounds the final response
        # emission (status/headers/body/flush) after the response
        # bytes are ready; separate from the socket/body capture
        # timeout, the header budget, the intake budget and the fixed
        # 15 s reviewer budget; rejected before any bind allocation
        # or traffic is accepted, and validated again at guard use.
        self.delivery_timeout = delivery_transport.validate_delivery_budget(
            delivery_timeout
        )
        # Independent finite positive elapsed main-upstream setup
        # budget (order 022-a, requirement 1): bounds owned
        # transport startup, resolver, address attempts, connect,
        # TLS handshake and the entire request write before response
        # acquisition; separate from the socket/body capture timeout,
        # the header budget, the intake budget, the delivery budget
        # and the fixed 15 s reviewer budget; rejected before any
        # bind allocation or traffic is accepted, and validated
        # again at use.
        self.setup_timeout = upstream_setup.validate_setup_budget(
            setup_timeout
        )
        # Finite positive body budget (order 017-a, requirement
        # 2): nonfinite or nonpositive operator values are rejected
        # before any bind allocation or traffic is accepted.
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError(
                "capture timeout must be a finite positive duration"
            )
        # Bounded evidence lookup policy (order 024-a, requirement
        # 2): the CURRENT config default policy must hold before any
        # bind allocation or traffic is accepted.  This pure
        # validation reads no index, file or connection, so a
        # startup-invalid policy fails here, before bind, while
        # at-use invalidity becomes the fixed RuntimeCorpus
        # budget-invalid label.
        validate_default_evidence_policy()
        # Versioned finite analysis-eligibility policy (order 025-a,
        # requirement 2): the CURRENT config default policy must hold
        # before any bind allocation or traffic is accepted.  This
        # pure validation reads no file, model, network or index, so
        # a startup-invalid policy fails here, before bind, with a
        # plain ValueError; a startup success is not a live
        # inference proof, and the mandatory normal-main Pandoc
        # startup/version/capability check remains the separate
        # startup contract.  At-use invalidity later becomes the
        # fixed content-free ANALYSIS_POLICY_INVALID bypass.
        analysis_policy.current_limits()
        # Finite accepted-request admission policy (order 026-a,
        # requirement 1): the CURRENT config admission defaults
        # must hold before any bind allocation or traffic is
        # accepted.  This pure validation reads no file, model,
        # network or index, so a startup-invalid policy fails
        # here, before bind, with a plain ValueError.  The
        # validated policy is immutable per instance: running
        # instances retain it while later config defaults apply
        # only to newly constructed instances.
        self._admission_policy = request_admission.current_admission_policy()
        # Explicit pure native capability validation (order 030-a,
        # requirement 1): the selected-native disconnect-only poll
        # prerequisite must hold before any bind allocation or
        # traffic is accepted; a weaker or missing detector refuses
        # startup explicitly - no silent fallback, no runtime IO
        # self-probe.
        request_lifecycle.validate_native_poll_capability()
        super().__init__(address, _PassthroughHandler if passthrough_mode else _Handler)
        self.upstream = parsed
        self.engine = engine
        self.capture_timeout = timeout
        self.trace_dir = trace_dir
        # Bounded content-free operator observation of separated review
        # usage (order 015-a, requirement 7); None disables observation.
        self.usage_observer = usage_observer
        if trace_dir is not None and trace_dir.exists() and trace_dir.is_symlink():
            raise ValueError("trace directory must not be a symlink")
        if trace_dir is not None:
            trace_dir.mkdir(parents=True, exist_ok=True)
        # One owned accepted-request admission lifecycle (order
        # 026-a): the fixed owned worker/monitor threads start
        # synchronously here, before request traffic, and commit
        # to dispatch only after this constructor has fully
        # completed; partial-startup ownership is contained inside
        # the controller (started threads stopped and actually
        # joined, never-started threads never joined, first
        # current-operation control preserved).  A construction or
        # thread startup failure must not leave the bound listener
        # socket open: the exact first control is preserved and the
        # listener is closed as independent best-effort cleanup
        # (order 026-a, requirement 5).
        try:
            self._admission = request_admission.RequestAdmissionController(
                self, self._admission_policy
            )
            self._admission.start()
        except BaseException:
            with contextlib.suppress(Exception):
                super().server_close()
            raise

    def _handle_request_noblock(self) -> None:
        # The accepting loop (order 026-a, requirements 2 and 3):
        # accept, then one owned admission decision - admit in
        # accepted order, or one fixed bounded 503 and close -
        # with the one absolute queue deadline anchored inside
        # dispatch before admission allocation/queue work.  The
        # accepting loop never runs a request, so it keeps
        # accepting while one handler is held; the admitted entry
        # is later handled inline by the single owned handler
        # worker through the standard process_request boundary,
        # preserving the existing outer HTTP error semantics -
        # and the frozen test fixtures' process_request
        # completion observation - exactly.
        # The narrowing below uses the import-time captured class
        # (see _SOCKET_CLASS above) rather than the mutable module
        # attribute; the impossible non-socket member routes to the
        # existing content-free server error boundary.
        try:
            request, client_address = self.get_request()
        except OSError:
            return
        if not self.verify_request(request, client_address):
            return
        if not isinstance(request, _SOCKET_CLASS):
            self.handle_error(request, client_address)
            return
        try:
            self._admission.dispatch(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
            with contextlib.suppress(Exception):
                self.shutdown_request(request)
        except BaseException:
            with contextlib.suppress(Exception):
                self.shutdown_request(request)
            raise

    def shutdown(self) -> None:
        # Stop the accepting loop first, then the documented
        # owning admission close boundary (order 026-a,
        # requirement 5): stop admission, cancel/close every
        # waiting entry with the fixed closed code, release every
        # owned slot and actually join the exact owned workers
        # before returning; an owned nonordinary worker control is
        # re-raised here at the owning supervisor.  Must run from
        # a different thread than serve_forever (native
        # BaseServer contract).  The admission close may re-raise a
        # preserved owned control, so the listener close is an
        # independent finalization step that runs before the
        # control propagates; the native shutdown threading
        # semantics are unchanged.
        super().shutdown()
        try:
            self._admission.stop()
        except BaseException:
            with contextlib.suppress(Exception):
                super().server_close()
            raise

    def server_close(self) -> None:
        # The accepting-loop socket close boundary (order 026-a,
        # requirement 5): the documented owning admission close
        # runs BEFORE the listener socket is closed, so a close of
        # the accepting loop alone can never leave the fixed owned
        # workers running.  The admission close may re-raise a
        # preserved owned control, and the admission lifecycle may
        # not exist yet (the native constructor failure path
        # virtually invokes this method before _admission is
        # constructed), so the listener close is an independent
        # finalization step that runs in every case and any
        # preserved control is re-raised only after it.  Idempotent
        # after an owning shutdown close.
        control: BaseException | None = None
        admission: (
            request_admission.RequestAdmissionController | None
        ) = getattr(self, "_admission", None)
        if admission is not None:
            try:
                admission.stop()
            except BaseException as exc:
                control = exc
        super().server_close()
        if control is not None:
            raise control

    def handle_error(self, request: object, client_address: object) -> None:
        # Bounded content-free intake diagnostics (order 020-a,
        # requirement 4): handler failures are closed quietly; no
        # raw exception, request, credential, header or text reaches
        # the log.
        return

    def observe_usage(self, details: list[dict[str, object]]) -> None:
        """One bounded content-free request-local review usage record.

        ``review_calls`` counts attempted batch requests (zero versus one
        is recorded separately from whether usage is known);
        ``reviewer_usage`` is the actual reviewer usage object or the
        explicit string ``"unknown"`` (never zero, never carried over
        from an earlier request).  Observation failure must not change a
        safely completed answer or hide a main failure.

        The record is content-free: it carries counts and usage numbers
        only, never a caller-supplied identifier (the legacy opt-in trace
        files keep their own request id separately).
        """
        if self.usage_observer is None:
            return
        calls = 0
        usage: object = "unknown"
        for detail in details:
            review_batch = detail.get("review_batch")
            if not isinstance(review_batch, dict):
                continue
            attempted = review_batch.get("calls")
            if type(attempted) is int:
                calls += attempted
            record = review_batch.get("usage")
            if isinstance(record, dict):
                usage = {
                    "input_tokens": record.get("input_tokens"),
                    "output_tokens": record.get("output_tokens"),
                    "total_tokens": record.get("total_tokens"),
                }
        if calls == 0:
            usage = "unknown"
        record = {"review_calls": calls, "reviewer_usage": usage}
        # Best-effort observation: a failing operator probe must not
        # change the answer or hide a main failure.
        with contextlib.suppress(Exception):
            self.usage_observer(record)
            pass

    def write_traces(
        self,
        request_id: str,
        details: list[dict[str, object]],
        proxy_seconds: float,
        raw_seconds: float = 0.0,
    ) -> list[Path]:
        if self.trace_dir is None:
            return []
        paths: list[Path] = []
        for index, detail in enumerate(details):
            detail_timings = detail.get("timings")
            timings: dict[str, object] = {}
            if isinstance(detail_timings, dict):
                timings.update(detail_timings)
            value: dict[str, object] = {
                "request_id": request_id,
                "message_index": index,
                "original": detail["original"],
                "repaired": detail["repaired"],
                "candidates": detail["candidates"],
                "corpus_summaries": detail["corpus_summaries"],
                "reviewer_decisions": detail["reviewer_decisions"],
                "acceptance": detail["acceptance"],
                "replacements": detail["replacements"],
                "timings": {
                    **timings,
                    "raw_seconds": round(raw_seconds, 6),
                    "proxy_seconds": round(proxy_seconds, 6),
                },
                "protected_difference_count": detail["protected_difference_count"],
                "protocol": {"response_completed": True, "valid": True},
            }
            # Content-free technical evidence failure marker (fixed label
            # plus early/late stage); persisted whenever the optional pass
            # was aborted or short-circuited by a known evidence error.
            evidence_failure = detail.get("evidence_failure")
            if isinstance(evidence_failure, dict):
                value["evidence_failure"] = evidence_failure
            # Content-free analysis-eligibility bypass marker (fixed
            # reason only); persisted whenever the optional analysis
            # pass was skipped by the finite policy gate before any
            # analysis work (order 025-a, requirement 4).
            analysis_bypass = detail.get("analysis_bypass")
            if isinstance(analysis_bypass, dict):
                value["analysis_bypass"] = analysis_bypass
            # Optional trace serialization boundary (order 025-a,
            # requirement 4): the existing shared canonical serializer
            # (unchanged global config.canonical_bytes) is computed
            # BEFORE any tempfile allocation.  A detail whose completed
            # original the serializer cannot represent (an unpaired
            # surrogate) raises UnicodeEncodeError and that ONE
            # optional trace detail/file is skipped - no empty
            # temporary artifact, no raw-text persistence, no answer
            # transformation - so the in-memory analysis_bypass
            # reason, the zero-call shape, the usage observer and the
            # exact original delivery continue unchanged.  Only
            # UnicodeEncodeError is caught: MemoryError, caller
            # control and file I/O errors are not hidden, and no
            # ASCII fallback or re-rendering is added.
            try:
                payload = canonical_bytes(value)
            except UnicodeEncodeError:
                continue
            target = self.trace_dir / f"{request_id}-{index}.json"
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".trace-", suffix=".tmp", dir=self.trace_dir, delete=False
            ) as handle:
                temporary = Path(handle.name)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            paths.append(target)
        return paths

    def forward(self, body: bytes, incoming: dict[str, str]) -> Forwarded:
        # The accepted two-argument call shape is unchanged for the
        # frozen transparent observers (order 030-a): the active
        # request's cancellation check is captured HERE on the
        # owning request thread (the per-request slot the handler
        # establishes before any main work; a no-op None outside an
        # active request), and the captured predicate is passed
        # explicitly into the existing guarded transports - no
        # ContextVar lookup from the guard threads, and no deadline
        # or limit change.
        cancelled = request_lifecycle.active_cancellation_check()
        host = self.upstream.hostname
        if host is None:
            raise ProxyError("upstream hostname is missing")
        # Narrow forwarded header policy (order 015-a, requirement 3):
        # only content-type, accept and an explicitly caller-provided
        # Authorization reach the main upstream.  Hop-by-hop, internal
        # control and linkage headers (host, content-length, cookies,
        # session/request ids, proxy controls) are never forwarded, and
        # the reviewer's API key is never borrowed as a main credential.
        headers = {
            key: value
            for key, value in incoming.items()
            if key.lower() in {"content-type", "accept", "authorization"}
        }
        headers["Content-Length"] = str(len(body))
        # One absolute monotonic setup deadline anchored at entry to
        # the owned main forward setup, BEFORE any worker/guard/
        # process/IPC startup, resolver call, connect/TLS or request
        # emission (order 022-a, requirement 2): it covers owned
        # transport startup, resolver, bounded address attempts,
        # connect, the TLS handshake and the entire request write
        # through the last body byte handed to the local transport,
        # and never resets.  A successful setup releases its clock
        # before response-header acquisition; the existing header/
        # body, intake and delivery clocks stay independent.  Setup
        # failure is a main failure (the handler's existing fixed
        # content-free 502 path) with zero engine, corpus or reviewer
        # work.
        # The setup budget is re-validated at use, before the
        # arithmetic or any downstream work (order 022-a,
        # requirement 1): the constructor validates the configured
        # value before bind, and the setup path rejects a mutated or
        # otherwise invalid instance value now, never accepting it as
        # a duration.
        setup_budget = upstream_setup.validate_setup_budget(
            self.setup_timeout
        )
        deadline = time.monotonic() + setup_budget
        port = self.upstream.port
        if port is None:
            port = 443 if self.upstream.scheme == "https" else 80
        response: http.client.HTTPResponse | None = None
        connection: http.client.HTTPConnection | None = None
        try:
            connection = upstream_setup.open_upstream_connection(
                scheme=self.upstream.scheme,
                host=host,
                port=port,
                request_path=responses_path(self.upstream.path),
                body=body,
                headers=headers,
                deadline=deadline,
                capture_timeout=self.capture_timeout,
                cancelled=cancelled,
            )
            # One elapsed header budget anchored immediately before
            # acquisition (order 018-a, requirement 2): the stdlib
            # getresponse parser is retained unchanged; the owned guard
            # interrupts only this connection's owned socket on
            # expiration and joins before any body capture, so the 017
            # body deadline below begins later and stays independent.
            response = header_transport.acquire_response(
                connection, self.header_timeout, cancelled=cancelled
            )
            content_type = response.getheader("Content-Type", "")
            raw = _bounded_read(
                response,
                MAIN_BODY_MAX_BYTES,
                self.capture_timeout,
                require_complete_sse=response.status == 200
                and content_type.lower().startswith("text/event-stream"),
                cancelled=cancelled,
            )
            return Forwarded(response.status, raw, content_type)
        finally:
            # Close the response body (and any over-read buffer) and the
            # owned transport handle: no transport outlives the request.
            if response is not None:
                response.close()
            if connection is not None:
                connection.close()


def _stderr_usage_observer(record: dict[str, object]) -> None:
    """Bounded content-free stderr record for operator observation.

    Enabled with ``CONCEPT_USAGE_OBSERVER=stderr`` (order 015-a,
    requirement 7): no new HTTP route, metrics service or raw-text log;
    the record carries counts and usage numbers only.
    """
    usage = record.get("reviewer_usage")
    if isinstance(usage, dict):
        line = (
            "concept-review-usage "
            f"calls={record.get('review_calls')} "
            f"usage=input={usage.get('input_tokens')},"
            f"output={usage.get('output_tokens')},"
            f"total={usage.get('total_tokens')}"
        )
    else:
        line = f"concept-review-usage calls={record.get('review_calls')} usage=unknown"
    print(line, file=sys.stderr, flush=True)


class _IntakeKwargs(TypedDict, total=False):
    """Optional operator-forwarded budget keywords for the real
    validated constructor (orders 020-a, 021-a and 022-a,
    requirement 1)."""

    client_timeout: float
    delivery_timeout: float
    setup_timeout: float


def main() -> int:
    upstream = os.environ.get("CONCEPT_UPSTREAM_URL", "http://127.0.0.1:8001")
    host = str(ipaddress.IPv4Address(os.environ.get("CONCEPT_HOST", "127.0.0.1")))
    port = int(os.environ.get("CONCEPT_PORT", "18024"))
    if not 1 <= port <= 65535:
        raise ValueError("CONCEPT_PORT must be between 1 and 65535")
    passthrough_setting = os.environ.get("CONCEPT_PASSTHROUGH", "1")
    if passthrough_setting not in {"0", "1"}:
        raise ValueError("CONCEPT_PASSTHROUGH must be 0 or 1")
    index = os.environ.get("CONCEPT_INDEX")
    if not index:
        raise SystemExit("CONCEPT_INDEX is required")
    # Configurable length-scaled review policy (order 040-a, R3):
    # canonical operator selection CONCEPT_CONFIG (explicit supplied
    # path) or ONLY the shipped adjacent file - never historical or
    # directory searching, no hot reload.  The file is loaded and
    # strictly validated BEFORE any listener/model/corpus work: a
    # missing/unreadable/malformed file, unknown sections/keys, wrong
    # types (including bool-as-int), zero/negative/nonfinite numeric
    # bounds or an unknown effort name refuses startup with a fixed
    # content-free reason, without echoing any value or credential.
    try:
        policy = load_review_policy()
    except ReviewPolicyError as exc:
        raise SystemExit(str(exc)) from None
    # Mandatory qualified-Pandoc startup check (order 013-a, requirements 2
    # and 12): executable presence, exact version and reader capability,
    # verified before the proxy accepts any traffic (before the HTTP bind).
    # There is no historical pulldown discovery or regex fallback path.
    try:
        pandoc_spec = resolve_pandoc_spec()
        check_pandoc(pandoc_spec)
    except PandocStartupError as exc:
        raise SystemExit(f"pandoc startup failure: {exc.reason}: {exc.detail}") from None
    # The batch reviewer is bounded by the ordered finite 15-second
    # default, now the whole runtime exchange deadline: one monotonic
    # deadline anchored at review_batch entry covers process/IPC
    # startup and the owned child's actual resolver/connect/TLS/
    # upload/status/header/body/result work, a late success being a
    # failure (order 019-a).  The upstream capture timeout
    # (CONCEPT_TIMEOUT, socket/body axis) and the independent elapsed
    # response-header budget (CONCEPT_HEADER_TIMEOUT, order 018-a) are
    # separate axes and must not extend reviewer calls (order 014-a,
    # requirement 4).
    # One explicit operator setting for the reviewer ONLY (order
    # 031-b, requirement 4; order 040-a, R3 precedence): the legacy
    # CONCEPT_REVIEWER_TIMEOUT remains the SOLE legacy timeout
    # override, environment override > selected/shipped file, and is
    # validated EQUALLY before any bind - a nonnumeric, nonpositive,
    # NaN, infinite or overflowing duration refuses startup with a
    # fixed content-free reason, never a raw decode crash.  Absent,
    # the selected file's timeout_seconds (shipped default 15 s)
    # governs.  The selected value keeps the one absolute
    # whole-exchange clock semantics per batch and is not coupled to
    # the other independent budgets (no retry, extra pass, changed
    # token cap, global HTTP clock or request/header override).
    raw_reviewer_timeout = os.environ.get("CONCEPT_REVIEWER_TIMEOUT")
    if raw_reviewer_timeout is not None:
        try:
            reviewer_timeout = validate_reviewer_timeout(
                float(raw_reviewer_timeout)
            )
        except (ValueError, OverflowError, ReviewPolicyError):
            raise SystemExit(
                "CONCEPT_REVIEWER_TIMEOUT must be a finite positive duration"
            ) from None
    else:
        reviewer_timeout = policy.timeout_seconds
    # The SAME immutable policy instance (with the effective timeout)
    # is shared by the reviewer and the engine below (order 040-a
    # interface checkpoint): the file values flow to the real
    # runtime, not just parsed/echoed.
    policy = replace(policy, timeout_seconds=reviewer_timeout)
    reviewer = ResponsesReviewer(
        os.environ.get("CONCEPT_REVIEWER_URL", "http://127.0.0.1:8001/v1"),
        os.environ.get("CONCEPT_REVIEWER_MODEL", "Qwen3.8-27B"),
        timeout=reviewer_timeout,
        profile=os.environ.get("CONCEPT_REVIEWER_PROFILE"),
        policy=policy,
    )
    trace_value = os.environ.get("CONCEPT_TRACE_DIR")
    # Operator-observable separated review usage (order 015-a,
    # requirement 7): wired at the normal runnable entry point, enabled
    # without editing source; observation stays a bounded stderr record.
    usage_observer: Callable[[dict[str, object]], None] | None = None
    if os.environ.get("CONCEPT_USAGE_OBSERVER") == "stderr":
        usage_observer = _stderr_usage_observer
    # One elapsed incoming-intake budget (order 020-a, requirement
    # 1): independent of CONCEPT_TIMEOUT (socket/body capture),
    # CONCEPT_HEADER_TIMEOUT (response-header axis) and the fixed 15 s
    # reviewer exchange.  Forwarded only when the operator explicitly
    # configures CONCEPT_REQUEST_TIMEOUT; otherwise the real
    # constructor's named CLIENT_REQUEST_TIMEOUT_SECONDS default (30 s)
    # governs, keeping pre-020 constructor adapters compatible at
    # their defaults while the real constructor always validates and
    # sets the bounded 30 s default when unconfigured.
    intake_kwargs: _IntakeKwargs = {}
    if os.environ.get("CONCEPT_REQUEST_TIMEOUT") is not None:
        intake_kwargs["client_timeout"] = float(
            os.environ["CONCEPT_REQUEST_TIMEOUT"]
        )
    # One elapsed final-delivery budget (order 021-a, requirement
    # 1): independent of CONCEPT_TIMEOUT (socket/body capture),
    # CONCEPT_HEADER_TIMEOUT (response-header axis),
    # CONCEPT_REQUEST_TIMEOUT (incoming-intake axis) and the fixed 15 s
    # reviewer exchange.  Forwarded only when the operator explicitly
    # configures CONCEPT_DELIVERY_TIMEOUT; otherwise the real
    # constructor's named CLIENT_DELIVERY_TIMEOUT_SECONDS default
    # (30 s) governs, keeping pre-021 constructor adapters compatible
    # at their defaults while the real constructor always validates
    # and sets the bounded 30 s default when unconfigured.
    if os.environ.get("CONCEPT_DELIVERY_TIMEOUT") is not None:
        intake_kwargs["delivery_timeout"] = float(
            os.environ["CONCEPT_DELIVERY_TIMEOUT"]
        )
    # One elapsed main-upstream setup budget (order 022-a,
    # requirement 1): independent of CONCEPT_TIMEOUT (socket/body
    # capture), CONCEPT_HEADER_TIMEOUT (response-header axis),
    # CONCEPT_REQUEST_TIMEOUT (incoming-intake axis),
    # CONCEPT_DELIVERY_TIMEOUT (outgoing-delivery axis) and the fixed
    # 15 s reviewer exchange.  Forwarded only when the operator
    # explicitly configures CONCEPT_SETUP_TIMEOUT; otherwise the real
    # constructor's named MAIN_SETUP_TIMEOUT_SECONDS default (30 s)
    # governs, keeping pre-022 constructor adapters compatible at
    # their defaults while the real constructor always validates and
    # sets the bounded 30 s default when unconfigured.
    if os.environ.get("CONCEPT_SETUP_TIMEOUT") is not None:
        intake_kwargs["setup_timeout"] = float(
            os.environ["CONCEPT_SETUP_TIMEOUT"]
        )
    proxy = ConceptProxy(
        (host, port),
        upstream,
        # The SAME immutable policy instance loaded once above reaches
        # BOTH the reviewer (constructed above) and the engine (order
        # 040-a interface checkpoint); no second independent policy
        # exists at the server.
        RepairEngine(
            Path(index), reviewer, pandoc=pandoc_spec, policy=policy
        ),
        header_timeout=float(
            os.environ.get(
                "CONCEPT_HEADER_TIMEOUT", str(MAIN_HEADER_TIMEOUT_SECONDS)
            )
        ),
        timeout=float(os.environ.get("CONCEPT_TIMEOUT", "30")),
        trace_dir=Path(trace_value) if trace_value else None,
        usage_observer=usage_observer,
        passthrough_mode=passthrough_setting == "1",
        **intake_kwargs,
    )
    # One owning close on normal serve return and every unwind
    # (order 031-b, requirement 2): the documented owning
    # server_close() boundary stops admission, actually joins the
    # exact owned workers and closes the accepting-loop listener, so
    # no owned non-daemon thread or bound listener outlives the real
    # entry point.  No shutdown() from the serving thread, no
    # daemonized workers, no dropped join, no optional-repair
    # fallback.
    # Control selection (order 031-b, requirement 3): the exact
    # first pending nonordinary caller control (including
    # MemoryError) keeps priority over later cleanup failures; an
    # ordinary pending error cannot suppress a later nonordinary
    # cleanup control; with no pending error the cleanup failure
    # propagates.  The catch settles cleanup and selects the right
    # control only, then re-raises the applicable original object;
    # no error conversion and no product-wide control refactor.
    pending_control: BaseException | None = None
    try:
        proxy.serve_forever()
    except BaseException as exc:
        pending_control = exc
    cleanup_control: BaseException | None = None
    try:
        proxy.server_close()
    except BaseException as exc:
        cleanup_control = exc
    if cleanup_control is not None:
        if pending_control is None:
            raise cleanup_control
        if (
            request_lifecycle.is_nonordinary_control(pending_control)
            or not request_lifecycle.is_nonordinary_control(cleanup_control)
        ):
            raise pending_control
        raise cleanup_control
    if pending_control is not None:
        raise pending_control
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
