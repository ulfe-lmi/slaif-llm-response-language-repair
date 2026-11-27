"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE. Repair details.

Pure, content-free support for the caller opt-in
``X-LLM-Language-Repair-Details`` header (order 041-a, requirements
1-4):

* header collection/parsing - absent or one trimmed ``0`` means off,
  exactly one trimmed ``1`` means on, duplicates or any other value
  reject with the fixed content-free label;
* strict construction of the ``llm_language_repair`` final-applied-edit
  metadata object from the engine's authoritative request-local final
  outcome (immutable original, final surviving edits, delivered
  answer), proving sorted disjoint original spans, exact original
  slices, non-identity replacements, exact final composition equal to
  the delivered answer, arithmetically derived repaired offsets and
  the delivered slices themselves;
* bounded size handling - the serialized extension is capped at
  1,048,576 bytes in the worst serialization mode, first by a cheap
  definite-overflow lower bound (no copying), then by the bounded
  actual serialization of the built object;
  overflow, any mapping-proof failure and a reserved root field
  already carried by the upstream response produce the same small
  fixed unavailable object, never a partial original or edit list.

No transport, no corpus access, no model call and no mutation of
request state.  Ordinary validation failures only: caller control and
MemoryError propagate untouched.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

#: Private per-request control header (order 041-a, requirement 1).
DETAILS_HEADER = "X-LLM-Language-Repair-Details"
_DETAILS_HEADER_LOWER = DETAILS_HEADER.lower()

#: Namespaced root response field selected by the owner (never
#: ``slaif_repair``).
FIELD_NAME = "llm_language_repair"

#: Fixed closed-set reasons (order 041-a, requirements 1 and 4).
REPAIR_DETAILS_HEADER_INVALID = "REPAIR_DETAILS_HEADER_INVALID"
EDIT_MAPPING_UNAVAILABLE = "EDIT_MAPPING_UNAVAILABLE"
DETAILS_TOO_LARGE = "DETAILS_TOO_LARGE"
UPSTREAM_FIELD_COLLISION = "UPSTREAM_FIELD_COLLISION"

#: Fixed schema constants.
DETAILS_VERSION = 1
OFFSET_UNIT = "unicode_codepoints"
STATUS_COMPLETE = "complete"
STATUS_UNAVAILABLE = "unavailable"

#: Serialized-extension cap in UTF-8 bytes (order 041-a, requirement 4).
MAX_DETAILS_BYTES = 1_048_576

#: One (original_start, original_end, replacement) applied edit in
#: ORIGINAL code-point coordinates (inclusive start, exclusive end).
Edit = tuple[int, int, str]

#: Bounded size handling (order 041-a R4 review): the cap is decided
# by the BOUNDED ACTUAL serialization of the built object, never by a
# cost estimate.  The preconstruction check is only a CHEAP DEFINITE
# OVERFLOW LOWER BOUND: every code point serializes to at least one
# byte in both serializer modes, and the JSON structure adds a
# positive constant, so content code points plus a small true lower
# bound on the structure that exceeds the cap overflows in EVERY
# serialization mode and is rejected before any copying.  Inputs that
# survive the lower bound have capped lengths, so the subsequent
# actual worst-mode serialization is itself bounded (ensure_ascii=True
# expansion is at most twelve bytes per code point).
_MIN_STRUCTURE_BYTES = 64
_MIN_STRUCTURE_PER_EDIT_BYTES = 64


class DetailsHeaderError(ValueError):
    """Fixed content-free rejection of the opt-in control header."""

    def __init__(self) -> None:
        super().__init__(REPAIR_DETAILS_HEADER_INVALID)
        self.reason = REPAIR_DETAILS_HEADER_INVALID


def details_header_values(pairs: Sequence[tuple[str, object]]) -> list[object]:
    """Collect this header's values case-insensitively, preserving
    duplicates (the duplicate itself is the rejection condition) and
    returning them as untrusted values, unmodified."""
    return [
        value
        for key, value in pairs
        if type(key) is str and key.lower() == _DETAILS_HEADER_LOWER
    ]


def parse_details_values(values: Sequence[object]) -> int:
    """Map the collected header values to 0 (off) or 1 (on).

    Values are untrusted and pass through unmodified: only a str value
    of exactly one trimmed ``0`` or ``1`` is recognized.  Absent or
    exactly one trimmed ``0`` means off; exactly one trimmed ``1``
    means on.  Two or more values, a non-string value, or a single
    value that is not exactly ``0``/``1`` after trimming, raise the
    fixed rejection; the untrusted value is never reflected.
    """
    if not values:
        return 0
    if len(values) != 1:
        raise DetailsHeaderError()
    value = values[0]
    if type(value) is not str:
        raise DetailsHeaderError()
    trimmed = value.strip()
    if trimmed == "1":
        return 1
    if trimmed == "0":
        return 0
    raise DetailsHeaderError()


def remove_details_header(
    pairs: Sequence[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Return the forwarding pairs with every case-insensitive control
    header removed; all other pairs are preserved untouched."""
    return [
        (key, value)
        for key, value in pairs
        if not (type(key) is str and key.lower() == _DETAILS_HEADER_LOWER)
    ]


def unavailable(reason: str) -> dict[str, object]:
    """The small fixed unavailable object (order 041-a, R4)."""
    return {
        "version": DETAILS_VERSION,
        "status": STATUS_UNAVAILABLE,
        "reason": reason,
    }


def collision() -> dict[str, object]:
    """The fixed reserved-field collision object."""
    return unavailable(UPSTREAM_FIELD_COLLISION)


def _worst_case_encoding(extension: dict[str, object]) -> bytes:
    """Exact serialization in the outer serializer's fallback mode;
    ensure_ascii=True is the maximum and cannot fail on lone
    surrogates (they escape, matching the body serializer's own
    fallback semantics)."""
    return json.dumps(
        extension, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")


def build_complete(
    original: str,
    edits: Sequence[Edit],
    delivered: str,
    output_index: int,
    content_index: int,
) -> dict[str, object]:
    """Build the complete ``llm_language_repair`` object or the fixed
    unavailable object - never a partial one.

    ``edits`` are the AUTHORITATIVE final applied edits (after
    individual acceptance, composed conflict rollback and final patch
    validation) in original code-point coordinates; ``delivered`` is
    the answer actually delivered.  The proof requires sorted disjoint
    valid original spans, replacement != original slice, exact final
    composition equal to ``delivered``, repaired offsets derived
    arithmetically from the preceding actual length changes, and the
    delivered slices re-validated against the replacements
    (order 041-a, R2).  Any proof failure yields
    EDIT_MAPPING_UNAVAILABLE; a serialized extension over the byte cap
    yields DETAILS_TOO_LARGE (checked exactly in the worst serialization
    mode before any duplication, and re-verified on the built object).
    """
    if type(original) is not str or type(delivered) is not str:
        return unavailable(EDIT_MAPPING_UNAVAILABLE)
    if (
        type(output_index) is not int
        or type(content_index) is not int
        or isinstance(output_index, bool)
        or isinstance(content_index, bool)
        or output_index < 0
        or content_index < 0
    ):
        return unavailable(EDIT_MAPPING_UNAVAILABLE)
    if not isinstance(edits, (list, tuple)):
        return unavailable(EDIT_MAPPING_UNAVAILABLE)
    original_length = len(original)
    # Cheap definite-overflow lower bound BEFORE any span slicing or
    # checked-list copying: one byte per content code point in any
    # serialization mode plus a true structure lower bound.  Malformed
    # edit items are not credited here (the full checks below reject
    # them; they are never treated as valid for this bound).
    content_length = original_length
    for item in edits:
        if type(item) is tuple and len(item) == 3 and type(item[2]) is str:
            content_length += len(item[2])
    if (
        content_length
        + _MIN_STRUCTURE_BYTES
        + _MIN_STRUCTURE_PER_EDIT_BYTES * len(edits)
        > MAX_DETAILS_BYTES
    ):
        return unavailable(DETAILS_TOO_LARGE)
    checked: list[Edit] = []
    for item in edits:
        if type(item) is not tuple or len(item) != 3:
            return unavailable(EDIT_MAPPING_UNAVAILABLE)
        start, end, replacement = item
        if (
            type(start) is not int
            or type(end) is not int
            or isinstance(start, bool)
            or isinstance(end, bool)
            or type(replacement) is not str
            or not 0 <= start < end <= original_length
            or replacement == original[start:end]
        ):
            return unavailable(EDIT_MAPPING_UNAVAILABLE)
        checked.append((start, end, replacement))
    # Adjacent pairs; the second sequence is one shorter (B905 noqa).
    for previous, current in zip(checked, checked[1:]):  # noqa: B905
        if current[0] < previous[1]:
            # Unsorted or overlapping original spans.
            return unavailable(EDIT_MAPPING_UNAVAILABLE)
    rendered: list[dict[str, object]] = []
    composed: list[str] = []
    shift = 0
    cursor = 0
    for start, end, replacement in checked:
        composed.append(original[cursor:start])
        repaired_start = start + shift
        repaired_end = repaired_start + len(replacement)
        # Delivered-slice re-validation in the delivered space.
        if delivered[repaired_start:repaired_end] != replacement:
            return unavailable(EDIT_MAPPING_UNAVAILABLE)
        rendered.append(
            {
                "original_start": start,
                "original_end": end,
                "repaired_start": repaired_start,
                "repaired_end": repaired_end,
                "replacement": replacement,
            }
        )
        composed.append(replacement)
        shift += len(replacement) - (end - start)
        cursor = end
    composed.append(original[cursor:])
    if "".join(composed) != delivered:
        # The exact final composition must equal the delivered answer.
        return unavailable(EDIT_MAPPING_UNAVAILABLE)
    extension: dict[str, object] = {
        "version": DETAILS_VERSION,
        "status": STATUS_COMPLETE,
        "offset_unit": OFFSET_UNIT,
        "output_index": output_index,
        "content_index": content_index,
        "original_text": original,
        "edits": rendered,
    }
    # The exact cap decision: the bounded ACTUAL worst-mode
    # serialization of the built object (the outer body serializer's
    # ensure_ascii=True fallback is the maximum of its two modes, so a
    # passing size holds for the wire bytes in BOTH modes).
    if len(_worst_case_encoding(extension)) > MAX_DETAILS_BYTES:
        return unavailable(DETAILS_TOO_LARGE)
    return extension


def attach(envelope: dict[str, object], extension: dict[str, object]) -> None:
    """Reserve the response-root field on one completed response
    object (JSON root; SSE terminal ``response.completed`` response).

    An opted-in response that ALREADY carries a root
    ``llm_language_repair`` value carries untrusted upstream data: the
    field is replaced by the fixed collision object.  Otherwise the
    proxy-authored extension is added.  Only the response root is ever
    touched - same-named user data anywhere else is never removed or
    rewritten (order 041-a, R4).
    """
    if FIELD_NAME in envelope:
        envelope[FIELD_NAME] = collision()
    else:
        envelope[FIELD_NAME] = extension
