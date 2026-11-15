"""Incremental SSE early-reasoning prefix streaming (order 038-a).

One-way buffer boundary: complete SSE frames are classified as soon as
their delimiter is known.  Only an explicit allowlist of recognized
reasoning events and validated no-answer-payload response-start
lifecycle events may be forwarded before the buffered tail; the first
non-allowlisted frame (and every later frame) is buffered
irrevocably.  Frames are never reordered, reclassified or replayed,
and answer text is never classified by substring or content
heuristic - only by the event's own structural fields.  The client
may dispatch on either the SSE ``event:`` label or the JSON ``type``,
so a label that is absent from the JSON, mismatches the JSON type, or
is duplicated cannot classify a frame as safe; it buffers instead.

This module is pure (no socket, no proxy import): frame splitting,
classification and the shared defined-logprobs removal used by both
the streaming prefix path and the whole-body transform.

SSE field semantics (order 038-a R1, helper review 1): the event/id/
retry field value is normalized per the SSE spec, which consumes
exactly one optional ASCII space after the field colon, so the spaced
and unspaced spellings of a label identify the same event while a
double space (or any other deviation) leaves a mismatching label and
therefore buffers.  ``data`` values keep their post-colon bytes
exactly as the strict runtime parser reads them.  A frame is early
forwardable only when its structure is explicitly proven: reasoning
content must be a proven reasoning item, proven reasoning part or
proven summary part, and a lifecycle snapshot is forwardable only
when its output list is proven to hold only reasoning items.  Message,
tool, unknown and malformed structures are buffered, never guessed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

from responses_contract import remove_defined_logprobs

# Same delimiter semantics as proxy._split_sse and
# capture_transport._SSE_DELIMITERS: a frame is complete only at its
# full blank-line delimiter, CRLF or LF.
SSE_DELIMITERS = re.compile(r"\r\n\r\n|\n\n")

# Terminal stream events must never be forwarded early (order 038-a R1).
TERMINAL_EVENT_TYPES = frozenset(
    {"response.completed", "response.incomplete", "response.failed"}
)

# Response-start / lifecycle nonterminal events: forwardable only with
# no answer payload in the carried response snapshot.
LIFECYCLE_NONTERMINAL_EVENT_TYPES = frozenset(
    {"response.created", "response.queued", "response.in_progress"}
)

# Reasoning event spellings evidenced by the documented Responses
# stream, the frozen synthetic Qwen fixtures, and the observed
# qwen3.8-27b-rtx3090 traces: reasoning arrives as output items of
# type "reasoning", as dedicated reasoning part/text events, and as
# content parts.  v7 observed the event names; the field-type-only v8
# instrumentation proved the native part type is "reasoning_text"
# while the documented Responses summary family uses "summary_text"
# - both are proven structural types, every other type is
# unrecognized and buffers.
REASONING_ITEM_EVENT_TYPES = frozenset(
    {"response.output_item.added", "response.output_item.done"}
)
REASONING_PART_EVENT_TYPES = frozenset(
    {"response.reasoning_part.added", "response.reasoning_part.done"}
)
REASONING_TEXT_EVENT_TYPES = frozenset(
    {"response.reasoning_text.delta", "response.reasoning_text.done"}
)
REASONING_SUMMARY_PART_EVENT_TYPES = frozenset(
    {"response.content_part.added", "response.content_part.done"}
)
# Documented Responses summary event families (helper review 4): the
# summary-side counterparts of the native reasoning_text family, with
# the same proven structural guards.  Any other reasoning spelling is
# unrecognized and buffers.
SUMMARY_PART_EVENT_TYPES = frozenset(
    {"response.reasoning_summary_part.added",
     "response.reasoning_summary_part.done"}
)
SUMMARY_TEXT_EVENT_TYPES = frozenset(
    {"response.reasoning_summary_text.delta",
     "response.reasoning_summary_text.done"}
)


@dataclass(frozen=True)
class FrameDecision:
    """Classification of one complete SSE frame for early forwarding.

    ``kind`` is "prefix" (safe to forward now) or "boundary" (buffer
    this frame and every later frame).  ``value`` is the strict-parsed
    data object for a prefix frame carrying data (None otherwise); it
    is already the exact object the whole-body transform would parse
    from the same bytes, so stripping it here and re-rendering it with
    the shared renderer keeps the forwarded bytes identical to the
    transform's own output for the same frame ordinal.
    """

    kind: str
    reason: str
    value: dict[str, object] | None = None


def split_complete_frames(text: str) -> list[tuple[str, str]]:
    """Split fully decodable text into complete (block, delimiter) frames.

    Only frames whose delimiter is fully present are returned; a
    trailing unterminated tail is dropped (still in flight).  This is
    the incremental counterpart of proxy._split_sse, which also keeps
    the final unterminated block.
    """
    frames: list[tuple[str, str]] = []
    cursor = 0
    for match in SSE_DELIMITERS.finditer(text):
        frames.append((text[cursor : match.start()], match.group(0)))
        cursor = match.end()
    return frames


def emit_new_frames(
    text: str, emitted_chars: int, on_frame: Callable[[str, str], None]
) -> int:
    """Invoke ``on_frame(block, delimiter)`` for each complete frame in
    ``text`` that ends after ``emitted_chars``; return the new emitted
    offset.  Frames arrive in stream order and each exactly once.
    """
    cursor = 0
    for match in SSE_DELIMITERS.finditer(text):
        end = match.end()
        if end > emitted_chars:
            on_frame(text[cursor : match.start()], match.group(0))
        cursor = end
    return cursor


def _optional_space(value: str) -> str:
    # SSE consumes exactly one optional U+0020 after the field colon.
    return value[1:] if value.startswith(" ") else value


def _field_lines(block: str) -> dict[str, list[str]]:
    # Line splitting matches proxy._event (str.splitlines).  data
    # values keep their post-colon bytes exactly as the strict runtime
    # parser reads them (its [DONE] test strips; JSON tolerates
    # leading whitespace).  event/id/retry labels are normalized per
    # the SSE spec above so both label spellings classify identically.
    fields: dict[str, list[str]] = {
        "data": [],
        "event": [],
        "id": [],
        "retry": [],
        "comment": [],
        "unknown": [],
    }
    for line in block.splitlines():
        if line.startswith("data:"):
            fields["data"].append(line[5:])
        elif line.startswith("event:"):
            fields["event"].append(_optional_space(line[6:]))
        elif line.startswith("id:"):
            fields["id"].append(_optional_space(line[3:]))
        elif line.startswith("retry:"):
            fields["retry"].append(_optional_space(line[6:]))
        elif line.startswith(":"):
            fields["comment"].append(line[1:])
        elif line.strip() == "":
            continue
        else:
            fields["unknown"].append(line)
    return fields


# Reasoning content part types proven for the two supported families:
# the native qwen3.8-27b-rtx3090 reasoning content type
# "reasoning_text" (characterization v8, field-type-only
# instrumentation) and the documented Responses summary content type
# "summary_text".  Any other part type is unrecognized and buffers.
REASONING_PART_TYPES = frozenset({"summary_text", "reasoning_text"})


def _valid_reasoning_part(part: object) -> bool:
    """A well-formed reasoning content part: a proven type and shape.

    An unknown part type or a non-string text field is a wellformed
    but unproven structure and denies early forwarding.
    """
    if not isinstance(part, dict):
        return False
    if part.get("type") not in REASONING_PART_TYPES:
        return False
    text = part.get("text")
    return text is None or type(text) is str


def _valid_reasoning_item(item: object) -> bool:
    """An explicitly proven reasoning-only output item: item type
    "reasoning" and every content container it carries (summary or
    content lists) holds only well-formed reasoning parts.  Message,
    tool and unknown item shapes are not proven reasoning.
    """
    if not isinstance(item, dict):
        return False
    if item.get("type") != "reasoning":
        return False
    for key in ("summary", "content"):
        container = item.get(key)
        if container is None:
            continue
        if not isinstance(container, list):
            return False
        for part in container:
            if not _valid_reasoning_part(part):
                return False
    return True


def _lifecycle_denial_reason(response: object) -> str | None:
    """Denial reason for a lifecycle snapshot, or None when the
    snapshot is explicitly proven reasoning-only (its output list
    holds only proven reasoning items; an empty list proves nothing
    answer-bearing is carried).  A missing response object or output
    list is not proven empty and therefore buffers."""
    if not isinstance(response, dict):
        return "lifecycle snapshot unproven"
    output = response.get("output")
    if not isinstance(output, list):
        return "lifecycle snapshot unproven"
    for item in output:
        if not isinstance(item, dict):
            return "lifecycle unproven output item"
        item_type = item.get("type")
        if item_type == "message":
            return "lifecycle message item"
        if item_type == "reasoning":
            if not _valid_reasoning_item(item):
                return "lifecycle reasoning item unproven"
            continue
        return "lifecycle non-reasoning item"
    return None


def _carrier_denial(value: dict[str, object], *ignore: str) -> str | None:
    """Denial reason for unexpected structural carriers riding in a
    reasoning wrapper (order 038-a R6, helper review 4): a reasoning
    event may not carry an answer payload.  Any response/item/part/
    output carrier present in the payload must be proven reasoning-only
    (the same validators the dedicated frames use); an unproven or
    malformed carrier forces the boundary.  Structure only - no
    recursive metadata rewriting.  Keys named in ``ignore`` are the
    frame's primary, already-validated payload."""
    for key in ("response", "item", "part", "output"):
        if key in ignore or key not in value:
            continue
        carrier = value[key]
        if key == "response":
            proven = _lifecycle_denial_reason(carrier) is None
        elif key == "item":
            proven = _valid_reasoning_item(carrier)
        elif key == "part":
            proven = _valid_reasoning_part(carrier)
        else:  # output
            proven = isinstance(carrier, list) and all(
                _valid_reasoning_item(entry) for entry in carrier
            )
        if not proven:
            return f"embedded {key} unproven"
    return None


def classify_prefix_frame(
    block: str, parse_event: Callable[[str], dict[str, object] | None]
) -> FrameDecision:
    """Classify one complete SSE frame for the early prefix (order 038-a R1).

    ``parse_event`` must be the strict frame parser (proxy._event
    semantics: JSON object, duplicate keys and nonfinite numbers
    rejected) and raise ValueError on malformed data (the strict
    parser's ProxyError is a ValueError).  A malformed frame is a
    boundary, never an approved early prefix; only the expected parse
    family is absorbed.  MemoryError and unexpected control or
    programming errors propagate through the owned handler cleanup
    unchanged and are never classified (order 038-a R4).  Both the SSE
    event label and the JSON type are considered; only their agreement
    identifies the event family (clients may dispatch on the label).
    """
    fields = _field_lines(block)
    if fields["unknown"]:
        return FrameDecision("boundary", "unknown field line")
    if len(fields["event"]) > 1:
        return FrameDecision("boundary", "duplicate event label")
    if not fields["data"]:
        if fields["event"]:
            return FrameDecision("boundary", "event label without data")
        return FrameDecision("prefix", "framing only")
    data_text = "\n".join(fields["data"])
    if data_text.strip() == "[DONE]":
        return FrameDecision("boundary", "terminal marker")
    try:
        value = parse_event(block)
    except ValueError:
        return FrameDecision("boundary", "malformed data")
    if not isinstance(value, dict):
        return FrameDecision("boundary", "data not an object")
    event_type = value.get("type")
    if type(event_type) is not str:
        return FrameDecision("boundary", "missing event type")
    if fields["event"]:
        # The client may dispatch by label: a label that differs from
        # the JSON type (or a typed JSON it cannot match) is ambiguous.
        if fields["event"][0] != event_type:
            return FrameDecision("boundary", "event label/type mismatch")
    if event_type in TERMINAL_EVENT_TYPES:
        return FrameDecision("boundary", "terminal event")
    if event_type in LIFECYCLE_NONTERMINAL_EVENT_TYPES:
        denial = _lifecycle_denial_reason(value.get("response"))
        if denial is not None:
            return FrameDecision("boundary", denial)
        return FrameDecision("prefix", "lifecycle no-answer-payload", value)
    if event_type in REASONING_ITEM_EVENT_TYPES:
        item = value.get("item")
        if isinstance(item, dict) and item.get("type") == "message":
            return FrameDecision("boundary", "message item")
        if not _valid_reasoning_item(item):
            if isinstance(item, dict) and item.get("type") != "reasoning":
                return FrameDecision("boundary", "non-reasoning item")
            return FrameDecision("boundary", "reasoning item unproven")
        denial = _carrier_denial(value, "item")
        if denial is not None:
            return FrameDecision("boundary", denial)
        return FrameDecision("prefix", "reasoning item", value)
    if event_type in REASONING_PART_EVENT_TYPES or event_type in SUMMARY_PART_EVENT_TYPES:
        if not _valid_reasoning_part(value.get("part")):
            return FrameDecision("boundary", "reasoning part unproven")
        denial = _carrier_denial(value, "part")
        if denial is not None:
            return FrameDecision("boundary", denial)
        return FrameDecision("prefix", "reasoning part", value)
    if event_type in REASONING_TEXT_EVENT_TYPES or event_type in SUMMARY_TEXT_EVENT_TYPES:
        delta = value.get("delta")
        if delta is not None and type(delta) is not str:
            return FrameDecision("boundary", "reasoning delta unproven")
        denial = _carrier_denial(value)
        if denial is not None:
            return FrameDecision("boundary", denial)
        return FrameDecision("prefix", "reasoning text", value)
    if event_type in REASONING_SUMMARY_PART_EVENT_TYPES:
        if not _valid_reasoning_part(value.get("part")):
            return FrameDecision("boundary", "non-reasoning part")
        denial = _carrier_denial(value, "part")
        if denial is not None:
            return FrameDecision("boundary", denial)
        return FrameDecision("prefix", "reasoning summary part", value)
    return FrameDecision("boundary", "unrecognized event type")


def strip_event_logprobs(value: dict[str, object]) -> bool:
    """Remove defined Responses logprobs by key presence from one
    parsed event, at exactly the locations the whole-body transform
    applies (order 035-c; A-HISTORY-01): output_text delta/done,
    output_text content parts, message output_text parts, and the
    response object of lifecycle events.  Returns True if changed.
    No recursive user-data scan; same-named user metadata untouched.
    """
    event_type = value.get("type")
    if event_type in ("response.output_text.delta", "response.output_text.done"):
        if "logprobs" in value:
            del value["logprobs"]
            return True
    elif event_type in ("response.content_part.added", "response.content_part.done"):
        part = value.get("part")
        if isinstance(part, dict) and part.get("type") == "output_text" and "logprobs" in part:
            del part["logprobs"]
            return True
    elif event_type in ("response.output_item.added", "response.output_item.done"):
        item = value.get("item")
        if isinstance(item, dict) and item.get("type") == "message":
            content = item.get("content")
            if isinstance(content, list):
                changed = False
                for part in content:
                    if (
                        isinstance(part, dict)
                        and part.get("type") == "output_text"
                        and "logprobs" in part
                    ):
                        del part["logprobs"]
                        changed = True
                return changed
    elif event_type in (
        "response.created",
        "response.in_progress",
        "response.completed",
        "response.incomplete",
        "response.failed",
        "response.queued",
    ):
        response = value.get("response")
        if isinstance(response, dict) and remove_defined_logprobs(response):
            return True
    return False
