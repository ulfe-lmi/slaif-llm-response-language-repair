"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE. Review policy.

Order 040-a: one focused, typed, immutable per-instance review policy for
the runnable proxy, loaded from the canonical operator-selected TOML file
before any listener, model or corpus work.

Discovery is deterministic and closed: an explicitly supplied path wins;
otherwise the environment variable ``CONCEPT_CONFIG`` (when set) is the
selected path; otherwise ONLY the shipped adjacent
``concept-verification/repair-proxy.toml`` is used.  There is no historical
or directory search and no hot reload.

The policy affects reviewer requests ONLY (selection allowance, batch
size, bounded context, prompt/output/request/response bounds, whole-exchange
timeout and the wire ``reasoning.effort``).  It never touches main-request
reasoning settings, the main capture/intake/admission/delivery budgets, the
fixed exact-span/proposal/CPU-acceptance safety constraints or the analysis
eligibility gate.

Failures are :class:`ReviewPolicyError` with fixed content-free diagnostic
labels: they name the offending section/key and the expected constraint,
never the offending value or any credential.
"""

from __future__ import annotations

import math
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Canonical operator selection: explicit path > environment > shipped file.
REVIEW_CONFIG_ENV = "CONCEPT_CONFIG"
SHIPPED_CONFIG_NAME = "repair-proxy.toml"
SHIPPED_CONFIG_PATH = Path(__file__).resolve().parent / SHIPPED_CONFIG_NAME

REVIEW_SECTION = "review"

#: Closed vocabulary of the exact ``[review]`` fields (order 040-a, R4).
INT_FIELDS: tuple[str, ...] = (
    "characters_per_call",
    "targets_per_call",
    "context_codepoints",
    "max_prompt_codepoints",
    "max_output_tokens",
    "max_request_bytes",
    "max_response_bytes",
)
TIMEOUT_FIELD = "timeout_seconds"
EFFORT_FIELD = "reasoning_effort"
KNOWN_FIELDS: frozenset[str] = frozenset(INT_FIELDS) | {TIMEOUT_FIELD, EFFORT_FIELD}

#: Complete standard reasoning-effort names (backend interpretation is
#: provider-dependent; the wire field is sent exactly as configured and is
#: never silently downgraded or omitted after rejection).
REASONING_EFFORTS: tuple[str, ...] = (
    "none",
    "minimal",
    "low",
    "medium",
    "high",
    "xhigh",
    "max",
)

#: Finite 32-bit unsigned bound for the byte caps carried in the owned
#: transport frame (order 040-a, R3/R4 clarification: the configured caps
#: reach the owned child/IPC framing and read limits).
MAX_TRANSPORT_CAP = 0xFFFF_FFFF

#: Documented defaults - the shipped adjacent file carries exactly these
#: values; absent keys in the selected file take them.
DEFAULT_CHARACTERS_PER_CALL = 1024
DEFAULT_TARGETS_PER_CALL = 4
DEFAULT_CONTEXT_CODEPOINTS = 240
DEFAULT_MAX_PROMPT_CODEPOINTS = 8192
DEFAULT_MAX_OUTPUT_TOKENS = 1536
DEFAULT_MAX_REQUEST_BYTES = 32_768
DEFAULT_MAX_RESPONSE_BYTES = 32_768
DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_REASONING_EFFORT = "low"


class ReviewPolicyError(ValueError):
    """Clear startup failure of the selected review policy file.

    ``label`` is a fixed content-free diagnostic; ``detail`` names the
    offending section/key and the expected constraint - never the
    offending value and never any credential.
    """

    def __init__(self, label: str, detail: str) -> None:
        super().__init__(f"review config: {label} ({detail})")
        self.label = label
        self.detail = detail


@dataclass(frozen=True)
class ReviewPolicy:
    """One immutable per-instance review policy (order 040-a, R4).

    All numeric fields are finite strictly positive; the integer fields are
    exact ints (bool rejected); ``timeout_seconds`` is a finite positive
    int/float stored as float.  ``reasoning_effort`` is one of the explicit
    standard names and is the reviewer-only wire default.
    """

    characters_per_call: int
    targets_per_call: int
    context_codepoints: int
    max_prompt_codepoints: int
    max_output_tokens: int
    max_request_bytes: int
    max_response_bytes: int
    timeout_seconds: float
    reasoning_effort: str

    def review_call_allowance(self, codepoints: int) -> int:
        """B = ceil(N / characters_per_call) for the ORIGINAL completed
        eligible answer length N in Unicode code points (not UTF-8 bytes,
        tokens, envelope/metadata/reasoning or edited length).  N=0 permits
        0; 1..1024 permits 1; 1025..2048 permits 2 (default file)."""
        if codepoints <= 0:
            return 0
        return -(-codepoints // self.characters_per_call)

    def selection_maximum(self, codepoints: int) -> int:
        """The detector selection maximum: B * targets_per_call (order
        040-a, R2).  These are maxima: no eligible suspicion means zero
        selected occurrences and zero review calls."""
        return self.review_call_allowance(codepoints) * self.targets_per_call


DEFAULT_POLICY = ReviewPolicy(
    characters_per_call=DEFAULT_CHARACTERS_PER_CALL,
    targets_per_call=DEFAULT_TARGETS_PER_CALL,
    context_codepoints=DEFAULT_CONTEXT_CODEPOINTS,
    max_prompt_codepoints=DEFAULT_MAX_PROMPT_CODEPOINTS,
    max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
    max_request_bytes=DEFAULT_MAX_REQUEST_BYTES,
    max_response_bytes=DEFAULT_MAX_RESPONSE_BYTES,
    timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    reasoning_effort=DEFAULT_REASONING_EFFORT,
)

_DEFAULTS: dict[str, Any] = {
    "characters_per_call": DEFAULT_CHARACTERS_PER_CALL,
    "targets_per_call": DEFAULT_TARGETS_PER_CALL,
    "context_codepoints": DEFAULT_CONTEXT_CODEPOINTS,
    "max_prompt_codepoints": DEFAULT_MAX_PROMPT_CODEPOINTS,
    "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS,
    "max_request_bytes": DEFAULT_MAX_REQUEST_BYTES,
    "max_response_bytes": DEFAULT_MAX_RESPONSE_BYTES,
    "timeout_seconds": DEFAULT_TIMEOUT_SECONDS,
    "reasoning_effort": DEFAULT_REASONING_EFFORT,
}


def _fail(label: str, detail: str) -> ReviewPolicyError:
    return ReviewPolicyError(label, detail)


def _validate_int(section: str, key: str, value: Any) -> int:
    # Exact int: bool is an int subclass and is explicitly rejected, as
    # are floats (even integral ones), zero, negatives and non-finite
    # values cannot occur for ints but are guarded by the type check.
    if type(value) is not int:
        raise _fail("wrong type", f"[{section}]{key} must be an integer")
    if value <= 0:
        raise _fail("invalid bound", f"[{section}]{key} must be positive")
    if value > MAX_TRANSPORT_CAP and key in ("max_request_bytes", "max_response_bytes"):
        raise _fail(
            "invalid bound",
            f"[{section}]{key} must not exceed 4294967295",
        )
    return value


def _validate_timeout(section: str, key: str, value: Any) -> float:
    # Finite positive int/float, bool excluded.  Huge ints overflow the
    # float conversion and are rejected as nonfinite, never as a crash:
    # an invalid config must fail clearly before bind.
    if type(value) is bool or type(value) not in (int, float):
        raise _fail("wrong type", f"[{section}]{key} must be a number")
    if type(value) is int:
        try:
            number = float(value)
        except OverflowError:
            raise _fail(
                "invalid bound", f"[{section}]{key} must be finite positive"
            ) from None
    else:
        number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise _fail("invalid bound", f"[{section}]{key} must be finite positive")
    return number


def _validate_effort(section: str, key: str, value: Any) -> str:
    if type(value) is not str or value not in REASONING_EFFORTS:
        raise _fail(
            "unknown reasoning effort",
            f"[{section}]{key} must be one of: "
            + ", ".join(REASONING_EFFORTS),
        )
    return value


def parse_review_policy_document(document: Any) -> ReviewPolicy:
    """Strictly parse one decoded TOML document into a ReviewPolicy.

    Unknown top-level sections/keys and unknown ``[review]`` keys fail;
    absent known keys take the documented defaults.  Raises
    :class:`ReviewPolicyError` with a fixed content-free label on any
    deviation; never echoes the offending value.
    """
    if not isinstance(document, dict):
        raise _fail("malformed document", "top level must be a table")
    for key in document:
        if key != REVIEW_SECTION:
            raise _fail("unknown section", f"{key}")
    review = document.get(REVIEW_SECTION)
    if review is None:
        raise _fail("missing review section", "[review]")
    if not isinstance(review, dict):
        raise _fail("wrong type", "[review] must be a table")
    for key in review:
        if key not in KNOWN_FIELDS:
            raise _fail("unknown key", f"[review]{key}")
    values: dict[str, Any] = dict(_DEFAULTS)
    for key in INT_FIELDS:
        if key in review:
            values[key] = _validate_int(REVIEW_SECTION, key, review[key])
    if TIMEOUT_FIELD in review:
        values[TIMEOUT_FIELD] = _validate_timeout(
            REVIEW_SECTION, TIMEOUT_FIELD, review[TIMEOUT_FIELD]
        )
    if EFFORT_FIELD in review:
        values[EFFORT_FIELD] = _validate_effort(REVIEW_SECTION, EFFORT_FIELD, review[EFFORT_FIELD])
    return ReviewPolicy(**values)


def _resolve_config_path(explicit_path: str | os.PathLike | None) -> Path:
    if explicit_path is not None:
        return Path(explicit_path)
    env = os.environ.get(REVIEW_CONFIG_ENV)
    if env is not None:
        return Path(env)
    return SHIPPED_CONFIG_PATH


def load_review_policy(explicit_path: str | os.PathLike | None = None) -> ReviewPolicy:
    """Load and strictly validate the selected review policy file.

    Discovery: ``explicit_path`` (canonical operator selection) wins; then
    ``CONCEPT_CONFIG``; then ONLY the shipped adjacent file.  Missing or
    unreadable files, malformed TOML, unknown sections/keys, wrong types
    (including bool-as-int), zero/negative/nonfinite numeric bounds and
    unknown effort names fail with a fixed content-free label - before any
    listener/model/corpus work when called at the normal entry point.
    """
    path = _resolve_config_path(explicit_path)
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except OSError:
        raise _fail("file missing or unreadable", path.name) from None
    except (tomllib.TOMLDecodeError, UnicodeDecodeError):
        # A byte file that is not valid UTF-8 is malformed TOML, not a
        # raw decode crash: the same fixed content-free label applies.
        raise _fail("malformed toml", path.name) from None
    return parse_review_policy_document(document)


def validate_reviewer_timeout(value: Any) -> float:
    """Validate one whole-exchange reviewer timeout (finite positive
    int/float, bool excluded) - used for the selected file value and the
    legacy ``CONCEPT_REVIEWER_TIMEOUT`` environment override EQUALLY,
    before any bind.  A value that overflows the float conversion (for
    example an enormous integer) is a nonfinite bound, not a crash."""
    if type(value) is bool or type(value) not in (int, float):
        raise ReviewPolicyError(
            "wrong type", "CONCEPT_REVIEWER_TIMEOUT must be a number"
        )
    if type(value) is int:
        try:
            number = float(value)
        except OverflowError:
            raise ReviewPolicyError(
                "invalid bound", "CONCEPT_REVIEWER_TIMEOUT must be finite positive"
            ) from None
    else:
        number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ReviewPolicyError(
            "invalid bound", "CONCEPT_REVIEWER_TIMEOUT must be finite positive"
        )
    return number
