"""EXPERIMENTAL / CONCEPT VERIFICATION / NOT PRODUCTION CODE. Owned
elapsed deadline and owned transport child for one reviewer batch.

Order 019-a: the whole runtime reviewer exchange - bounded prompt
preparation in the parent, then process startup, IPC, and the actual
resolver/connect/TLS/request upload/status/header/body work in one
owned ordinary stdlib transport child, plus the result transfer and
validation in the parent - runs under one finite positive monotonic
deadline anchored at ``review_batch`` entry (order 019-a,
requirement 1).  Byte arrival, interim 100 responses, phase
transitions and process startup never restart it; a late or racing
success is a failure, not a salvage, and expiry is rechecked after
the parent's own result validation as well.  The child owns every
potentially blocking HTTP phase so the parent can terminate and
actually reap the exact child before return; no worker, timer,
thread, descriptor or socket survives its request and no background
wait/reaper thread exists (order 019-a, requirement 4).  The IPC
channel is private pipes with strict versioned framing and finite
byte caps; the explicit runtime fields - including the credential -
travel only inside that private channel, never in argv or env.  The
parent-side pipe descriptors are nonblocking and every partial
write/read and readiness race is governed by the remaining deadline,
so a stalled or shrunken pipe cannot block past the bound.  The
child environment carries only explicitly forwarded TLS trust
references (``SSL_CERT_FILE`` / ``SSL_CERT_DIR``) and nothing else
(order 019-a, requirement 3).  The fixed normal main stays at the
existing 15-second reviewer cap and is independent of
``CONCEPT_TIMEOUT`` and ``CONCEPT_HEADER_TIMEOUT``; overall request
deadlines are untouched; the owned exchange additionally cooperates
with the current request's cancellation (order 030-a): the
attempted-dispatch marker is set on the parent side after the pure
frame/request bounds and before the actual owned launch, and the
governed parent waits consult the active request-local cancellation
check (a per-thread no-op outside an active request), after which
the exact owned child is terminated and actually reaped by the
existing cleanup and the contained RequestCancelled propagates.  No
hard real-time scheduling is claimed.
"""

from __future__ import annotations

import contextlib
import fcntl
import math
import os
import select
import struct
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import request_lifecycle
from batch_review import REASON_HTTP_FAILED
from config import (
    REVIEW_BATCH_MAX_REQUEST_BYTES,
    REVIEW_BATCH_MAX_RESPONSE_BYTES,
)

REVIEW_TRANSPORT_VERSION = 2

# Finite IPC caps (order 019-a, requirement 3).  The request body cap
# and response payload cap are the existing batch bounds; the address
# fields carry explicit finite limits so no unbounded accumulation is
# possible on either side of the pipe.
_MAGIC = b"RRTX"
_KIND_REQUEST = 1
_KIND_RESPONSE = 2
_SCHEME_HTTP = 0
_SCHEME_HTTPS = 1
_RESPONSE_OK = 0
_RESPONSE_TRANSPORT_FAILED = 1
_HOST_MAX_BYTES = 255
_PATH_MAX_BYTES = 2048
_AUTH_MAX_BYTES = 4096
_BODY_MAX_BYTES = REVIEW_BATCH_MAX_REQUEST_BYTES
_PAYLOAD_MAX_BYTES = REVIEW_BATCH_MAX_RESPONSE_BYTES

#: Version 2 (order 040-a, R3/R4 clarification): the request frame
#: carries the ACTUAL configured byte caps so the owned child/IPC
#: framing and read limits enforce the file-configured bounds instead
#: of hidden fixed defaults; the child bounds its reads from the
#: frame's own validated header fields, never from an untrusted
#: prefix length alone.  Ownership, reap, deadline and cancellation
#: semantics are unchanged.
_REQUEST_HEADER = struct.Struct(">4sBBBHHIIdIII")
_RESPONSE_HEADER = struct.Struct(">4sBBBHI")
_FRAME_PREFIX = struct.Struct(">Q")

_REQUEST_FIXED_BYTES = _REQUEST_HEADER.size
_RESPONSE_FIXED_BYTES = _RESPONSE_HEADER.size
#: Legacy default-policy caps (kept for direct unit use); the real
#: runtime path always passes the configured values explicitly.
REQUEST_FRAME_MAX_BYTES = (
    _REQUEST_FIXED_BYTES
    + _HOST_MAX_BYTES
    + _PATH_MAX_BYTES
    + _AUTH_MAX_BYTES
    + _BODY_MAX_BYTES
)
RESPONSE_FRAME_MAX_BYTES = _RESPONSE_FIXED_BYTES + _PAYLOAD_MAX_BYTES + 1

# Bounded cleanup slices for terminating and reaping the exact owned
# child; finite and honestly accounted, not a deadline extension.
_CLEANUP_SLICE_SECONDS = 0.5
_CLEANUP_POLL_SECONDS = 0.05

#: Finite 32-bit unsigned bound for caps carried in the frame (order
#: 040-a): the structured field admits at most this value, and the
#: startup policy validation applies the same bound.
_CAP_MAX = 0xFFFF_FFFF


def _validate_cap(value: object, what: str) -> int:
    # Exact positive int (bool rejected), bounded by the 32-bit frame
    # field; a nonconforming cap is a contract violation, never a
    # silent fallback to a fixed default.
    if type(value) is not int or value <= 0 or value > _CAP_MAX:
        raise FrameMalformed(f"reviewer {what} is not a valid byte cap")
    return value


class ReviewTransportError(ValueError):
    """Owned reviewer exchange failure; ``reason`` is a fixed label.

    The message is a fixed content-free diagnostic; no exception
    text, response bytes, header value or credential is carried.
    """

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class DeadlineExceeded(Exception):
    """The anchored absolute deadline passed before a phase finished."""


class FrameMalformed(Exception):
    """A private pipe frame violated the strict versioned contract."""


class FrameIncomplete(Exception):
    """The pipe ended before a complete frame arrived."""


class ProcessFailure(Exception):
    """The owned child exited nonzero, could not start, or its
    transport failed."""


@dataclass(frozen=True)
class ReviewRequest:
    """One decoded request frame (explicit runtime fields only).

    ``request_max_bytes`` / ``response_max_bytes`` are the configured
    caps carried in the frame (order 040-a, R3/R4 clarification): the
    child bounds its request-body acceptance and its HTTP response read
    with them, so the file-configured limits reach the owned child/IPC
    framing and read limits, not just the parent checks.
    """

    scheme: str
    host: str
    port: int | None
    path: str
    auth: str | None
    remaining_seconds: float
    body: bytes
    request_max_bytes: int
    response_max_bytes: int


@dataclass(frozen=True)
class ReviewResponse:
    """One decoded response frame."""

    ok: bool
    http_status: int
    payload: bytes


@dataclass(frozen=True)
class _RequestHeaderInfo:
    """Validated request-header fields before the body is read.

    The child uses ``total_frame_bytes`` to bound its reads from the
    frame's own validated header (bounded host/path/auth lengths and a
    body no larger than the frame's own configured request cap) instead
    of trusting the untrusted prefix length alone.
    """

    host_len: int
    path_len: int
    auth_len: int
    body_len: int
    request_max_bytes: int
    response_max_bytes: int

    @property
    def total_frame_bytes(self) -> int:
        return (
            _FRAME_PREFIX.size
            + _REQUEST_FIXED_BYTES
            + self.host_len
            + self.path_len
            + self.auth_len
            + self.body_len
        )


def _decode_utf8_bounded(data: bytes, limit: int, what: str) -> str:
    if len(data) > limit:
        raise FrameMalformed(f"reviewer {what} exceeds bound")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FrameMalformed(f"reviewer {what} is not UTF-8") from exc


def encode_request_frame(
    *,
    scheme: str,
    host: str,
    port: int | None,
    path: str,
    auth: str | None,
    remaining_seconds: float,
    body: bytes,
    request_max_bytes: int = _BODY_MAX_BYTES,
    response_max_bytes: int = _PAYLOAD_MAX_BYTES,
) -> bytes:
    """Encode one strict versioned request frame (wire-ready).

    ``port`` keeps the full 16-bit meaning: ``None`` encodes the
    default port as 0 and an explicit port must be 1-65535; an
    explicit 0 is rejected, never aliased to the default.  The
    configured byte caps (order 040-a, R3/R4 clarification) are
    validated and carried in the frame so the owned child enforces
    them; the defaults are the shipped policy's bounds.
    """
    if scheme not in ("http", "https"):
        raise FrameMalformed("reviewer scheme is invalid")
    request_max = _validate_cap(request_max_bytes, "request byte cap")
    response_max = _validate_cap(response_max_bytes, "response byte cap")
    host_bytes = host.encode("utf-8")
    if len(host_bytes) > _HOST_MAX_BYTES:
        raise FrameMalformed("reviewer host exceeds bound")
    path_bytes = path.encode("utf-8")
    if len(path_bytes) > _PATH_MAX_BYTES:
        raise FrameMalformed("reviewer path exceeds bound")
    auth_bytes = b""
    if auth is not None:
        auth_bytes = auth.encode("utf-8")
        if len(auth_bytes) > _AUTH_MAX_BYTES:
            raise FrameMalformed("reviewer credential field exceeds bound")
    if not math.isfinite(remaining_seconds) or remaining_seconds <= 0:
        raise FrameMalformed("reviewer remaining time is not finite positive")
    if len(body) > request_max:
        raise FrameMalformed("reviewer request body exceeds bound")
    if port is None:
        port_byte = 0
    else:
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 0xFFFF:
            raise FrameMalformed("reviewer port is out of range")
        port_byte = port
    header = _REQUEST_HEADER.pack(
        _MAGIC,
        REVIEW_TRANSPORT_VERSION,
        _KIND_REQUEST,
        _SCHEME_HTTPS if scheme == "https" else _SCHEME_HTTP,
        port_byte,
        len(host_bytes),
        len(path_bytes),
        len(auth_bytes),
        remaining_seconds,
        len(body),
        request_max,
        response_max,
    )
    frame = header + host_bytes + path_bytes + auth_bytes + body
    assert len(frame) <= (
        _REQUEST_FIXED_BYTES
        + _HOST_MAX_BYTES
        + _PATH_MAX_BYTES
        + _AUTH_MAX_BYTES
        + request_max
    )
    return _FRAME_PREFIX.pack(len(frame)) + frame


def decode_request_header(header: bytes) -> _RequestHeaderInfo:
    """Strictly validate one request frame's header bytes (no body).

    The child calls this BEFORE reading the body so the remaining read
    is bounded by the frame's own validated fields (order 040-a,
    R3/R4 clarification); a corrupted or oversized declaration is a
    contract violation, never an unbounded read.
    """
    if len(header) != _REQUEST_FIXED_BYTES:
        raise FrameMalformed("reviewer request frame is truncated")
    (
        magic,
        version,
        kind,
        scheme_byte,
        port_byte,
        host_len,
        path_len,
        auth_len,
        remaining_seconds,
        body_len,
        request_max,
        response_max,
    ) = _REQUEST_HEADER.unpack_from(header, 0)
    if (
        magic != _MAGIC
        or version != REVIEW_TRANSPORT_VERSION
        or kind != _KIND_REQUEST
    ):
        raise FrameMalformed("reviewer request frame identity is invalid")
    if host_len > _HOST_MAX_BYTES:
        raise FrameMalformed("reviewer host exceeds bound")
    if path_len > _PATH_MAX_BYTES:
        raise FrameMalformed("reviewer path exceeds bound")
    if auth_len > _AUTH_MAX_BYTES:
        raise FrameMalformed("reviewer credential field exceeds bound")
    if not math.isfinite(remaining_seconds) or remaining_seconds <= 0:
        raise FrameMalformed("reviewer remaining time is not finite positive")
    request_max = _validate_cap(request_max, "request byte cap")
    response_max = _validate_cap(response_max, "response byte cap")
    if body_len > request_max:
        raise FrameMalformed("reviewer request body exceeds bound")
    if scheme_byte not in (_SCHEME_HTTP, _SCHEME_HTTPS):
        raise FrameMalformed("reviewer scheme is invalid")
    return _RequestHeaderInfo(
        host_len, path_len, auth_len, body_len, request_max, response_max
    )


def decode_request_frame(frame: bytes) -> ReviewRequest:
    """Strictly decode one request frame; reject any deviation.

    The absolute frame bound is derived from the frame's own validated
    header (embedded configured caps), so a nondefault configured cap
    is enforced end to end (order 040-a, R3/R4 clarification).
    """
    if len(frame) < _REQUEST_FIXED_BYTES:
        raise FrameMalformed("reviewer request frame is truncated")
    info = decode_request_header(frame[:_REQUEST_FIXED_BYTES])
    if (
        len(frame)
        > _REQUEST_FIXED_BYTES
        + _HOST_MAX_BYTES
        + _PATH_MAX_BYTES
        + _AUTH_MAX_BYTES
        + info.request_max_bytes
    ):
        raise FrameMalformed("reviewer request frame exceeds bound")
    (
        magic,
        version,
        kind,
        scheme_byte,
        port_byte,
        host_len,
        path_len,
        auth_len,
        remaining_seconds,
        body_len,
        request_max,
        response_max,
    ) = _REQUEST_HEADER.unpack_from(frame, 0)
    if (
        magic != _MAGIC
        or version != REVIEW_TRANSPORT_VERSION
        or kind != _KIND_REQUEST
    ):
        raise FrameMalformed("reviewer request frame identity is invalid")
    if len(frame) != _REQUEST_FIXED_BYTES + host_len + path_len + auth_len + body_len:
        raise FrameMalformed("reviewer request frame length is inconsistent")
    if scheme_byte not in (_SCHEME_HTTP, _SCHEME_HTTPS):
        raise FrameMalformed("reviewer scheme is invalid")
    if not math.isfinite(remaining_seconds) or remaining_seconds <= 0:
        raise FrameMalformed("reviewer remaining time is not finite positive")
    offset = _REQUEST_FIXED_BYTES
    host = _decode_utf8_bounded(frame[offset : offset + host_len], _HOST_MAX_BYTES, "host")
    offset += host_len
    path = _decode_utf8_bounded(frame[offset : offset + path_len], _PATH_MAX_BYTES, "path")
    offset += path_len
    auth: str | None = None
    if auth_len:
        auth = _decode_utf8_bounded(
            frame[offset : offset + auth_len], _AUTH_MAX_BYTES, "credential"
        )
    offset += auth_len
    body = frame[offset : offset + body_len]
    if len(body) != body_len:
        raise FrameMalformed("reviewer request frame body is truncated")
    if len(body) > request_max:
        raise FrameMalformed("reviewer request body exceeds bound")
    return ReviewRequest(
        scheme="https" if scheme_byte == _SCHEME_HTTPS else "http",
        host=host,
        port=None if port_byte == 0 else port_byte,
        path=path,
        auth=auth,
        remaining_seconds=remaining_seconds,
        body=body,
        request_max_bytes=request_max,
        response_max_bytes=response_max,
    )


def encode_response_frame(
    *, ok: bool, http_status: int, payload: bytes, payload_max_bytes: int = _PAYLOAD_MAX_BYTES
) -> bytes:
    """Encode one strict versioned response frame (wire-ready).

    ``payload_max_bytes`` is the configured response bound (order
    040-a, R3/R4 clarification): the child over-reads by exactly one
    byte to detect a declared delivery past the bound, and a payload
    beyond ``payload_max_bytes`` is a contract violation, never a
    silent fixed-cap truncation.
    """
    payload_max = _validate_cap(payload_max_bytes, "response byte cap")
    if not ok:
        http_status = 0
        payload = b""
    elif not 0 <= http_status <= 0xFFFF or len(payload) > payload_max + 1:
        raise FrameMalformed("reviewer response fields exceed bound")
    header = _RESPONSE_HEADER.pack(
        _MAGIC,
        REVIEW_TRANSPORT_VERSION,
        _KIND_RESPONSE,
        _RESPONSE_OK if ok else _RESPONSE_TRANSPORT_FAILED,
        http_status,
        len(payload),
    )
    frame = header + payload
    assert len(frame) <= _RESPONSE_FIXED_BYTES + payload_max + 1
    return _FRAME_PREFIX.pack(len(frame)) + frame


def decode_response_frame(
    frame: bytes, payload_max_bytes: int = _PAYLOAD_MAX_BYTES
) -> ReviewResponse:
    """Strictly decode one response frame; reject any deviation.

    The absolute frame bound is the configured response cap plus the
    one-byte over-read marker (order 040-a, R3/R4 clarification).
    """
    payload_max = _validate_cap(payload_max_bytes, "response byte cap")
    if len(frame) > _RESPONSE_FIXED_BYTES + payload_max + 1:
        raise FrameMalformed("reviewer response frame exceeds bound")
    if len(frame) < _RESPONSE_FIXED_BYTES:
        raise FrameMalformed("reviewer response frame is truncated")
    magic, version, kind, status_byte, http_status, payload_len = (
        _RESPONSE_HEADER.unpack_from(frame, 0)
    )
    if (
        magic != _MAGIC
        or version != REVIEW_TRANSPORT_VERSION
        or kind != _KIND_RESPONSE
    ):
        raise FrameMalformed("reviewer response frame identity is invalid")
    if status_byte not in (_RESPONSE_OK, _RESPONSE_TRANSPORT_FAILED):
        raise FrameMalformed("reviewer response frame status is unknown")
    if len(frame) != _RESPONSE_FIXED_BYTES + payload_len:
        raise FrameMalformed("reviewer response frame length is inconsistent")
    payload = frame[_RESPONSE_FIXED_BYTES : _RESPONSE_FIXED_BYTES + payload_len]
    if status_byte == _RESPONSE_OK:
        return ReviewResponse(True, http_status, payload)
    return ReviewResponse(False, 0, b"")


def _select_governed(
    readable_fd: int | None,
    writeable_fd: int | None,
    deadline: float,
    cancelled: Callable[[], bool] | None = None,
) -> None:
    """Wait for one governed readiness slice before a pipe operation."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeadlineExceeded
        if cancelled is not None and cancelled():
            raise request_lifecycle.RequestCancelled()
        readable, writeable, _ = select.select(
            [readable_fd] if readable_fd is not None else [],
            [writeable_fd] if writeable_fd is not None else [],
            [],
            min(remaining, 0.05),
        )
        if readable or writeable:
            return


def _read_exact_governed(
    fd: int,
    count: int,
    deadline: float,
    cancelled: Callable[[], bool] | None = None,
) -> bytes:
    """Read exactly ``count`` bytes from a nonblocking fd under the
    deadline; partial reads and readiness races stay governed."""
    buffer = bytearray()
    while len(buffer) < count:
        _select_governed(fd, None, deadline, cancelled)
        try:
            chunk = os.read(fd, count - len(buffer))
        except BlockingIOError:
            continue
        if not chunk:
            raise FrameIncomplete
        buffer.extend(chunk)
    return bytes(buffer)


def _read_frame_governed(
    fd: int,
    max_bytes: int,
    deadline: float,
    cancelled: Callable[[], bool] | None = None,
) -> bytes:
    prefix = _read_exact_governed(
        fd, _FRAME_PREFIX.size, deadline, cancelled
    )
    (frame_len,) = _FRAME_PREFIX.unpack(prefix)
    if frame_len == 0 or frame_len > max_bytes:
        raise FrameMalformed("reviewer frame length exceeds bound")
    return _read_exact_governed(fd, frame_len, deadline, cancelled)


def _write_all_governed(
    fd: int,
    data: bytes,
    deadline: float,
    cancelled: Callable[[], bool] | None = None,
) -> None:
    """Write all bytes to a nonblocking fd under the deadline; partial
    writes on a stalled or shrunken pipe stay governed, never a
    deadline-unsafe blocking write."""
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        _select_governed(None, fd, deadline, cancelled)
        try:
            written = os.write(fd, view[offset:])
        except BlockingIOError:
            continue
        if written <= 0:
            raise FrameIncomplete
        offset += written


def _wait_exit_governed(
    proc: subprocess.Popen[bytes],
    deadline: float,
    cancelled: Callable[[], bool] | None = None,
) -> int:
    while True:
        code = proc.poll()
        if code is not None:
            return code
        if cancelled is not None and cancelled():
            raise request_lifecycle.RequestCancelled()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DeadlineExceeded
        try:
            return proc.wait(timeout=min(remaining, 0.05))
        except subprocess.TimeoutExpired:
            continue


def _terminate_and_reap(proc: subprocess.Popen[bytes]) -> None:
    """Terminate, kill if needed, and actually wait/reap the exact
    child before return.  A pre-allocated instance whose constructor
    never created a child has no pid and is a no-op: waitpid on an
    unset pid could touch other owned children, so the never-created
    state is distinguished by the unset pid, never by a process
    scan; an already-ended created child is reaped by the first
    poll without a signal.  Bounded polls cover the ordinary paths;
    if the child still cannot be reaped after SIGKILL, the final
    wait blocks until the kernel reaps it - an OS scheduling delay
    is honestly waited out, never converted into a live-child escape
    (order 019-a, requirement 4; order 023-a, requirement 3)."""
    if getattr(proc, "pid", None) is None:
        return
    if proc.poll() is None:
        with contextlib.suppress(OSError):
            proc.terminate()
        for _ in range(int(_CLEANUP_SLICE_SECONDS / _CLEANUP_POLL_SECONDS)):
            if proc.poll() is not None:
                return
            time.sleep(_CLEANUP_POLL_SECONDS)
    if proc.poll() is None:
        with contextlib.suppress(OSError):
            proc.kill()
    proc.wait()


def _child_environment() -> dict[str, str]:
    """Minimal child environment: only explicit TLS trust references."""
    env: dict[str, str] = {}
    for name in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        value = os.environ.get(name)
        if value:
            env[name] = value
    return env


def _make_pipe_nonblocking(fd: int) -> None:
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def run_review_exchange(
    *,
    scheme: str,
    host: str,
    port: int | None,
    path: str,
    body: bytes,
    auth: str | None,
    deadline: float,
    worker_path: Path,
    request_max_bytes: int = _BODY_MAX_BYTES,
    response_max_bytes: int = _PAYLOAD_MAX_BYTES,
) -> tuple[int, bytes]:
    """Run one owned-child reviewer exchange under an absolute deadline.

    ``deadline`` is the caller-anchored absolute ``time.monotonic()``
    deadline; it covers worker creation, IPC, the child's actual
    resolver/connect/TLS/upload/status/header/body work and the
    result transfer and validation here (order 019-a, requirement
    1).  A valid response arriving after the deadline is a failure
    (late success is not transferred).  On every path the exact
    owned child is terminated if still alive and actually
    wait/reaped - blocking after SIGKILL until the kernel reaps it,
    so no live child is ever returned across the call - and every
    owned descriptor is closed, before return (order 019-a,
    requirement 4).  Returns the HTTP status and raw
    payload for the caller's existing bounded validation; raises
    :class:`ReviewTransportError` with the fixed transport label on
    deadline expiry, malformed/oversized/incomplete/extra replies,
    nonzero child exit, spawn failure or child transport failure
    (no automatic retry; order 019-a, requirement 2).  The configured
    byte caps (order 040-a, R3/R4 clarification) are validated here,
    carried in the request frame and enforced by the owned child's
    request framing, response read limit, response framing and the
    parent's final decode.
    """
    try:
        request_max = _validate_cap(request_max_bytes, "request byte cap")
        response_max = _validate_cap(response_max_bytes, "response byte cap")
    except FrameMalformed as exc:
        # A nonconforming configured cap is a contract violation with
        # the fixed transport label; the startup policy validation
        # already fails before bind on the real path.
        raise ReviewTransportError(
            "review HTTP request failed", reason=REASON_HTTP_FAILED
        ) from exc
    if not math.isfinite(deadline):
        raise ReviewTransportError(
            "reviewer exchange deadline is not finite", reason=REASON_HTTP_FAILED
        )
    if deadline - time.monotonic() <= 0:
        # Already expired before worker creation: no spawn, no salvage.
        raise ReviewTransportError(
            "reviewer exchange deadline expired before dispatch",
            reason=REASON_HTTP_FAILED,
        )
    frame = encode_request_frame(
        scheme=scheme,
        host=host,
        port=port,
        path=path,
        auth=auth,
        remaining_seconds=deadline - time.monotonic(),
        body=body,
        request_max_bytes=request_max,
        response_max_bytes=response_max,
    )
    # Request-local attempted-dispatch marker (order 030-a,
    # requirement 7): set on the parent side AFTER the pure
    # frame/request bounds and BEFORE the actual owned launch (no
    # descriptor or child exists yet), so an interrupted exchange is
    # distinguished from a pre-dispatch cancellation.  The pre-
    # dispatch boundary itself (existing R4/R7, root final review):
    # if the current captured scope already carries an observed
    # cancellation at this post-pure-frame point, the dedicated
    # cancellation propagates BEFORE the marker and the owned launch
    # - no child is started, no attempted-dispatch count is created
    # and no HTTP POST can occur for an already-canceled request.
    # The per-thread slot is a no-op outside an active request
    # (startup, direct core tests, other threads) and the frozen
    # caller (qwen_client) is unchanged.  The same scope's
    # cancellation check is captured for the governed waits below
    # (explicit closure capture - no ContextVar assumption).
    scope = request_lifecycle.active_scope()
    if scope is not None:
        scope.raise_if_cancelled()
        scope.mark_review_dispatch_attempted()
    cancelled = scope.cancelled if scope is not None else None
    # Descriptor ownership starts before the first descriptor can
    # exist: no fallible ownership-tracking runs between descriptor
    # creation and cleanup responsibility, so a fault at any point -
    # pipe allocation, fcntl configuration, process-object
    # allocation, start, IPC - closes every direct known descriptor
    # local (with an ``fd >= 0`` guard) and reaps any created child
    # before raising.  Ordinary failure keeps the fixed transport
    # label; nonordinary caller control (KeyboardInterrupt,
    # MemoryError, ...) is cleaned first and re-raised with its
    # original identity (order 023-a, requirement 3).
    request_read = -1
    request_write = -1
    response_read = -1
    response_write = -1
    proc: subprocess.Popen[bytes] | None = None
    result: tuple[int, bytes] | None = None
    internal_error: BaseException | None = None
    try:
        request_read, request_write = os.pipe()
        response_read, response_write = os.pipe()
        _make_pipe_nonblocking(request_write)
        _make_pipe_nonblocking(response_read)
        # The exact process object is owned BEFORE the fallible start
        # can create any child: with the real Popen class the
        # pre-allocated instance carries the real (unmodified) Popen
        # construction, so a caller interruption after actual child
        # creation still leaves the owning instance (and its pid)
        # reachable by the finally below for terminate, kill
        # escalation and an actual wait/reap instead of an orphaned
        # child; the allocation itself sits inside the owned-FD
        # cleanup scope, so its failure leaves no child and the
        # descriptors are still closed (order 023-a, requirement 3).
        # A transparent non-class Popen replacement (a forwarding test
        # double) has no instance to pre-allocate: the direct call is
        # the observed start and ownership lands on return, preserving
        # the historical spy-observable behavior.
        popen = subprocess.Popen
        if isinstance(popen, type):
            proc = popen.__new__(popen)
            try:
                popen.__init__(
                    proc,
                    [sys.executable, "-I", "-S", "-B", str(worker_path)],
                    stdin=request_read,
                    stdout=response_write,
                    stderr=subprocess.DEVNULL,
                    env=_child_environment(),
                    close_fds=True,
                )
            except OSError as exc:
                raise ProcessFailure(
                    "owned reviewer child could not start"
                ) from exc
        else:
            try:
                proc = popen(
                    [sys.executable, "-I", "-S", "-B", str(worker_path)],
                    stdin=request_read,
                    stdout=response_write,
                    stderr=subprocess.DEVNULL,
                    env=_child_environment(),
                    close_fds=True,
                )
            except OSError as exc:
                raise ProcessFailure(
                    "owned reviewer child could not start"
                ) from exc
        # The child now owns its copies of the pipe ends; the parent
        # drops the two ends it does not use immediately, marking
        # each closed descriptor so the finally cannot close it twice.
        os.close(request_read)
        request_read = -1
        os.close(response_write)
        response_write = -1
        _write_all_governed(request_write, frame, deadline, cancelled)
        os.close(request_write)
        request_write = -1
        reply = _read_frame_governed(
            response_read,
            _RESPONSE_FIXED_BYTES + response_max + 1,
            deadline,
            cancelled,
        )
        # Strict single-frame check: exactly one frame, then the pipe
        # ends; any extra byte is a malformed reply.
        _select_governed(response_read, None, deadline, cancelled)
        try:
            extra = os.read(response_read, 1)
        except BlockingIOError:
            extra = b""
        if extra:
            raise FrameMalformed("reviewer reply carries extra data")
        code = _wait_exit_governed(proc, deadline, cancelled)
        if code != 0:
            raise ProcessFailure("owned reviewer child exited nonzero")
        response = decode_response_frame(
            reply, payload_max_bytes=response_max
        )
        # Expiry is checked after completion: a racing or late valid
        # frame loses to the deadline and is never transferred.
        if time.monotonic() >= deadline:
            raise DeadlineExceeded
        if not response.ok:
            raise ProcessFailure("owned reviewer child transport failed")
        result = (response.http_status, response.payload)
    except (
        DeadlineExceeded,
        FrameMalformed,
        FrameIncomplete,
        ProcessFailure,
        OSError,
    ) as exc:
        internal_error = exc
    finally:
        if proc is not None:
            _terminate_and_reap(proc)
        for fd in (request_read, request_write, response_read, response_write):
            if fd >= 0:
                with contextlib.suppress(OSError):
                    os.close(fd)
    if internal_error is not None:
        raise ReviewTransportError(
            "review HTTP request failed", reason=REASON_HTTP_FAILED
        ) from internal_error
    assert result is not None
    return result
