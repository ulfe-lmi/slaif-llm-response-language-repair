#!/usr/bin/env python3
"""Plain-terminal comparison client for the language-repair Responses API.

Loads Codex-style TOML configuration (``config.toml`` plus either a
``[profiles.NAME]`` table or a standalone ``NAME.config.toml``) and a model
catalog, then runs a simple text-only chat loop that calls the
``/v1/responses`` endpoint through the configured gateway/proxy.
Server-provided thinking is printed into ordinary terminal scrollback; the
authoritative delivered answer and, when actually returned, the original
answer are printed once the response completes.

Transport: the official OpenAI Python SDK (pinned ``openai==3.60.0``,
Responses API) is the actual request and event boundary; install it from
the pinned ``client`` dependency group (see examples/README.md).  The
request mode is fixed at startup: buffered (the default) sends one request
and displays the completed response, while ``--stream`` requests streamed
Responses events and prints server reasoning deltas as they arrive.
Retries are disabled (``max_retries=0``), every request uses a finite
timeout and deterministic cleanup, redirects are refused, and the client
never prints credentials, raw server messages or request/response bodies.
The client never starts an AI process, never reads ``auth.json`` or
performs OAuth, keeps no on-disk transcript and performs no terminal
screen manipulation (no ANSI cursor movement, alternate screen, clearing
or spinners).

Deterministic configuration precedence (documented in examples/README.md):

* ``~/.codex`` is the default configuration root; ``--codex-home`` selects an
  explicit one.  The ``CODEX_HOME`` environment variable is intentionally
  ignored so the location is explicit and never inherited from the shell.
* The selected config file is ``--config`` (when given) or
  ``<codex-home>/config.toml``.
* A requested profile is the ``[profiles.NAME]`` table of the selected
  config file, which overrides only the root settings it names; when that
  table is absent the standalone
  ``<config-file directory>/NAME.config.toml`` file is used; when neither
  exists the client fails with a clear error and never falls back silently
  to unrelated root model/provider settings.
* A standalone file is the complete selected configuration: it supplies its
  own model/provider/effort/catalog, and is not merged over the root.  If
  its provider table does not name the selected provider, the root
  ``model_providers`` definitions are consulted as a lookup only.
* Per setting: command line > selected profile (table or standalone
  file) > root defaults. The selected model's declared catalog default is
  a fallback for the reasoning effort only; it never supplies a missing
  model.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tomllib
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn

import httpx2
from openai import (
    APIConnectionError,
    APIError,
    APIResponseValidationError,
    APIStatusError,
    APITimeoutError,
    DefaultHttpxClient,
    OpenAI,
    OpenAIError,
)
from openai.types.responses import Response, ResponseOutputMessage, ResponseOutputText
from pydantic import ValidationError as PydanticValidationError

REPAIR_DETAILS_HEADER = "X-LLM-Language-Repair-Details"
MISSING_ORIGINAL_MESSAGE = "Original response was not provided."
CONVENTIONAL_EFFORTS: tuple[str, ...] = ("minimal", "low", "medium", "high")
FIXED_UNAVAILABLE_REASONS = frozenset(
    {"EDIT_MAPPING_UNAVAILABLE", "DETAILS_TOO_LARGE", "UPSTREAM_FIELD_COLLISION"}
)
# Closed set of machine error codes the client recognises.  Only these may
# surface verbatim; every other server code, message, header and body stays
# out of the terminal output.
KNOWN_ENDPOINT_FAILURE_CODES = frozenset({"responses_stream_event_not_supported"})
HEADER_TOKEN_CHARS = frozenset("!#$%&'*+-.^_`|~")
CLIENT_OWNED_HEADERS = frozenset(
    {"authorization", "content-type", REPAIR_DETAILS_HEADER.lower()}
)
DEFAULT_TIMEOUT_SECONDS = 900.0

HELP_TEXT = """\
Commands (one per line):
  /help              show this help
  /model             show the numbered model menu, then select on the next line
  /model SLUG|N      convenience: select a model directly
  /thinking          show the numbered effort menu, then select on the next line
  /thinking LEVEL    convenience: select an effort level directly
  /quit              exit (end-of-input also exits)
Any other line is sent as one user message.  Menus accept a number or the
name itself; blank or 0 cancels; an invalid selection leaves the current
choice unchanged.  Menus and slash commands make zero API calls.
Request mode (buffered by default, --stream for live events) is fixed at
startup.
"""

_MENU_CANCELLED = "cancelled"
_MENU_EXIT = "exit"


class ClientError(Exception):
    """A fixed, actionable, secret-free failure of configuration or request."""


def _fail(message: str) -> NoReturn:
    raise ClientError(message)

@dataclass(frozen=True)
class CatalogModel:
    slug: str
    display_name: str | None = None
    default_effort: str | None = None
    efforts: tuple[str, ...] = ()  # empty = catalog declares no levels


@dataclass(frozen=True)
class Provider:
    name: str
    base_url: str | None
    wire_api: str | None
    env_key: str | None
    bearer_token: str | None
    http_headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Resolved:
    url: str
    token: str
    extra_headers: dict[str, str]
    timeout: float


@dataclass(frozen=True)
class Settings:
    resolved: Resolved
    base: dict[str, Any]
    config_dir: Path


@dataclass
class Session:
    models: tuple[CatalogModel, ...]
    catalog_loaded: bool
    model_index: int = -1
    explicit_model: str | None = None
    effort: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    def current_model(self) -> CatalogModel | None:
        if self.models and self.model_index >= 0:
            return self.models[self.model_index]
        return None

    def wire_model(self) -> str:
        if self.explicit_model is not None:
            return self.explicit_model
        return self.models[self.model_index].slug


def _str_key(table: dict[str, Any], key: str, context: str) -> str | None:
    if key not in table:
        return None
    value = table[key]
    if type(value) is not str or not value:
        _fail(f"invalid or empty string for '{key}' in {context}")
    return value


def _valid_header_name(name: str) -> bool:
    return bool(name) and all(
        char.isascii() and (char.isalnum() or char in HEADER_TOKEN_CHARS)
        for char in name
    )


def load_toml_file(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except OSError:
        _fail(f"cannot read configuration file: {path}")
    except tomllib.TOMLDecodeError:
        _fail(f"invalid TOML in configuration file: {path}")
    if not isinstance(value, dict):
        _fail(f"configuration root must be a table: {path}")
    return value


def select_profile(
    args: argparse.Namespace, root_cfg: dict[str, Any], config_file: Path
) -> tuple[dict[str, Any], str]:
    """Return (settings table, source description) for the selected scope."""
    if not args.profile:
        if not root_cfg:
            _fail(
                f"no configuration found: {config_file} is missing and no "
                "profile was selected"
            )
        return root_cfg, f"configuration file {config_file}"
    profiles = root_cfg.get("profiles")
    if isinstance(profiles, dict) and isinstance(profiles.get(args.profile), dict):
        # A standard [profiles.NAME] table overrides only the root settings
        # it names and inherits everything else (model_provider, effort,
        # catalog, ...).  A standalone file is, in contrast, the complete
        # selected configuration.
        merged = dict(root_cfg)
        merged.update(profiles[args.profile])
        return merged, f"[profiles.{args.profile}] in {config_file}"
    standalone = config_file.parent / f"{args.profile}.config.toml"
    if standalone.is_file():
        return load_toml_file(standalone), f"standalone profile file {standalone}"
    _fail(
        f"profile '{args.profile}' not found: no [profiles.{args.profile}] "
        f"table in {config_file} and no standalone file {standalone}; refusing "
        "to fall back to unrelated model/provider settings"
    )


def load_provider(
    name: str, root_cfg: dict[str, Any], base: dict[str, Any], context: str
) -> Provider:
    for holder, scope in ((base, context), (root_cfg, "root configuration")):
        providers = holder.get("model_providers")
        if not isinstance(providers, dict):
            continue
        table = providers.get(name)
        if table is None or not isinstance(table, dict):
            continue
        base_url = _str_key(table, "base_url", scope)
        wire_api = _str_key(table, "wire_api", scope)
        env_key = _str_key(table, "env_key", scope)
        bearer_token = _str_key(table, "experimental_bearer_token", scope)
        headers: dict[str, str] = {}
        raw_headers = table.get("http_headers", {})
        if not isinstance(raw_headers, dict):
            _fail(f"http_headers must be a table of strings in {scope}")
        for key, value in raw_headers.items():
            if type(key) is not str or type(value) is not str:
                _fail(f"http_headers entries must be strings in {scope}")
            if not _valid_header_name(key):
                _fail(
                    f"http_headers name {key!r} in {scope} contains invalid "
                    "characters; header names are single-line tokens"
                )
            if key.lower() in CLIENT_OWNED_HEADERS:
                _fail(
                    f"http_headers in {scope} must not set '{key}'; that "
                    "header is owned by the client request and cannot be "
                    "overridden"
                )
            if "\r" in value or "\n" in value:
                _fail(
                    f"http_headers value for '{key}' in {scope} must not "
                    "contain CR or LF"
                )
            if any(ord(char) > 0xFF for char in value):
                _fail(
                    f"http_headers value for '{key}' in {scope} must be "
                    "encodable as latin-1 header data"
                )
            headers[key] = value
        return Provider(
            name=name,
            base_url=base_url,
            wire_api=wire_api,
            env_key=env_key,
            bearer_token=bearer_token,
            http_headers=headers,
        )
    _fail(
        f"model_providers.{name} is not defined in the selected profile or "
        "the root configuration; add the provider table or choose another profile"
    )


def resolve_token(provider: Provider) -> str:
    if provider.env_key is not None:
        token = os.environ.get(provider.env_key)
        if not token:
            _fail(
                f"environment variable '{provider.env_key}' is not set; "
                "set it to the provider API key and rerun"
            )
        context = f"environment variable '{provider.env_key}'"
    elif provider.bearer_token is not None:
        token = provider.bearer_token
        context = "the provider configuration"
    else:
        _fail(
            f"provider '{provider.name}' defines neither env_key nor "
            "experimental_bearer_token; this example does not read auth.json or "
            "perform OAuth - add one of those to the provider table"
        )
    # Validate before any HTTP layer can render the value: an invalid
    # credential is rejected with a fixed message that never echoes it.
    if not token.isascii() or not token.isprintable():
        _fail(
            f"credential from {context} is not a valid single-line printable "
            "ASCII header value; fix that credential and rerun"
        )
    return token


def build_url(base: str | None, override: str | None) -> str:
    """Validate and normalize the API base URL.

    The official SDK appends exactly one ``/responses`` path segment to the
    returned base, so a base that already ends with ``/responses`` is
    rejected instead of producing a doubled path.
    """
    chosen = override if override is not None else base
    if not chosen:
        _fail(
            "no API base URL: the provider has no base_url and --base-url "
            "was not given"
        )
    parts = urllib.parse.urlsplit(chosen)
    if parts.scheme not in ("http", "https"):
        _fail(
            f"unsupported URL scheme '{parts.scheme or '<none>'}'; "
            "only http and https are supported"
        )
    if parts.username is not None or parts.password is not None:
        _fail("URL userinfo (user:pass@) is not supported")
    if parts.fragment:
        _fail("URL fragments are not supported")
    if not parts.hostname:
        _fail("the API base URL is missing a host")
    path = parts.path.rstrip("/")
    if path.endswith("/responses"):
        _fail(
            "the API base URL must not already end with /responses; "
            "the SDK appends it exactly once"
        )
    return f"{parts.scheme}://{parts.netloc}{path}"



def _catalog_entry(entry: Any, context: str) -> CatalogModel:
    if type(entry) is str:
        if not entry:
            _fail(f"empty model entry in {context}")
        return CatalogModel(slug=entry)
    if not isinstance(entry, dict):
        _fail(f"model entries must be objects or strings in {context}")
    slug = entry.get("slug", entry.get("id"))
    if type(slug) is not str or not slug:
        _fail(f"model entry missing 'slug' (or 'id') in {context}")
    display = entry.get("display_name")
    if display is not None and not (type(display) is str and display):
        _fail(f"model entry display_name must be a nonempty string in {context}")
    default = entry.get("default_reasoning_level")
    if default is not None and not (type(default) is str and default):
        _fail(f"model entry default_reasoning_level must be a string in {context}")
    levels_raw = entry.get("supported_reasoning_levels", [])
    if not isinstance(levels_raw, list):
        _fail(f"model entry supported_reasoning_levels must be a list in {context}")
    levels: list[str] = []
    for item in levels_raw:
        if not isinstance(item, dict):
            _fail(f"supported_reasoning_levels items must be objects in {context}")
        effort = item.get("effort")
        if not (type(effort) is str and effort):
            _fail(
                f"supported_reasoning_levels items need a string 'effort' "
                f"in {context}"
            )
        levels.append(effort)
    return CatalogModel(
        slug=slug,
        display_name=display if type(display) is str else None,
        default_effort=default if type(default) is str else None,
        efforts=tuple(levels),
    )


def load_catalog(
    args: argparse.Namespace, base: dict[str, Any], config_dir: Path
) -> tuple[tuple[CatalogModel, ...], bool]:
    path: Path | None = None
    if args.models:
        path = Path(os.path.expanduser(args.models))
    else:
        catalog_key = _str_key(base, "model_catalog_json", "configuration")
        if catalog_key:
            raw = Path(os.path.expanduser(catalog_key))
            path = raw if raw.is_absolute() else (config_dir / raw)
        else:
            sibling = config_dir / "models.json"
            if sibling.is_file():
                path = sibling
    if path is None:
        return (), False
    if not path.is_file():
        _fail(f"model catalog not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        _fail(f"model catalog is not valid UTF-8 JSON: {path}")
    entries = data.get("models") if isinstance(data, dict) else data
    if not isinstance(entries, list) or not entries:
        _fail(f"model catalog must contain a nonempty 'models' list: {path}")
    models: list[CatalogModel] = []
    seen: set[str] = set()
    for entry in entries:
        model = _catalog_entry(entry, "model catalog")
        if model.slug in seen:
            _fail(f"duplicate model slug '{model.slug}' in {path}")
        seen.add(model.slug)
        models.append(model)
    return tuple(models), True


def initialize_session(
    args: argparse.Namespace, base: dict[str, Any], config_dir: Path
) -> Session:
    models, catalog_loaded = load_catalog(args, base, config_dir)
    session = Session(models=models, catalog_loaded=catalog_loaded)
    model_choice = args.model or _str_key(base, "model", "configuration")
    effort_choice = args.thinking or _str_key(base, "model_reasoning_effort", "configuration")
    if not model_choice:
        _fail(
            "no model selected: set 'model' in the configuration/profile or "
            "pass --model"
        )
    if catalog_loaded:
        index = next((i for i, m in enumerate(models) if m.slug == model_choice), -1)
        if index < 0:
            _fail(
                f"model '{model_choice}' is not in the catalog; "
                f"available: {', '.join(m.slug for m in models)}"
            )
        session.model_index = index
    else:
        session.explicit_model = model_choice
    model = session.current_model()
    if effort_choice is not None:
        if model is not None and model.efforts and effort_choice not in model.efforts:
            _fail(
                f"effort '{effort_choice}' is not supported by model "
                f"'{model.slug}'; allowed: {', '.join(model.efforts)}"
            )
        session.effort = effort_choice
    elif model is not None:
        session.effort = model.default_effort
    return session


def allowed_efforts(session: Session) -> tuple[str, ...]:
    model = session.current_model()
    if model is not None and model.efforts:
        return model.efforts
    return CONVENTIONAL_EFFORTS


def _read_line(prompt: str) -> str | None:
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        # End-of-input or Ctrl-C at a prompt (not mid-request) exits the
        # session cleanly.
        return None


def _menu(
    title: str,
    entries: tuple[tuple[str, str], ...],  # (display, value)
    current_value: str | None,
    noun: str,
) -> str:
    """Print a plain numbered menu and read ONE selection line.

    Returns the selected value, _MENU_CANCELLED (blank/0/invalid, already
    reported) or _MENU_EXIT (EOF or /quit from the menu).  Ordinary
    append-only lines; no screen manipulation.
    """
    print(title)
    for number, (display, value) in enumerate(entries, 1):
        marker = " [current]" if value == current_value else ""
        print(f"  {number}. {display}{marker}")
    line = _read_line("select> ")
    if line is None:
        return _MENU_EXIT
    line = line.strip()
    if line in ("", "0"):
        print(f"{noun.capitalize()} selection cancelled.")
        return _MENU_CANCELLED
    if line == "/quit":
        return _MENU_EXIT
    if line.isdigit():
        number = int(line)
        if 1 <= number <= len(entries):
            return entries[number - 1][1]
    else:
        for _display, value in entries:
            if value == line:
                return value
    print(f"Invalid selection '{line}'; {noun} unchanged.")
    return _MENU_CANCELLED


def _select_model(session: Session, index: int) -> None:
    model = session.models[index]
    previous = session.effort
    session.model_index = index
    session.explicit_model = None
    if previous and model.efforts and previous in model.efforts:
        session.effort = previous
    elif model.efforts:
        session.effort = model.default_effort or model.efforts[0]
        if previous:
            print(
                f"Effort reset to '{session.effort}' for {render_visible(model.slug)} "
                f"('{previous}' is not supported by this model)."
            )
        else:
            print(f"Effort set to the model default '{session.effort}'.")
    else:
        session.effort = previous
        if previous:
            print(
                f"Effort kept at '{previous}' for {render_visible(model.slug)} (the catalog "
                "declares no levels for this model; support is not verified)."
            )
    print(f"Next request will use model: {render_visible(model.slug)}")


def _command_model_direct(session: Session, argument: str) -> None:
    if session.models:
        offset = None
        if argument.isdigit():
            number = int(argument)
            if 1 <= number <= len(session.models):
                offset = number - 1
        else:
            offset = next(
                (i for i, m in enumerate(session.models) if m.slug == argument),
                None,
            )
        if offset is None:
            print(
                f"Model '{argument}' is not in the catalog; "
                f"available: {', '.join(render_visible(m.slug) for m in session.models)}. "
                "Use bare /model for the menu."
            )
            return
        _select_model(session, offset)
    else:
        session.explicit_model = argument
        print(f"Next request will use model: {render_visible(argument)}")


def _command_thinking_direct(session: Session, argument: str) -> None:
    levels = allowed_efforts(session)
    if argument in levels:
        session.effort = argument
        print(f"Next request will use thinking effort: {argument}")
    else:
        model = session.current_model()
        note = (
            ""
            if model is not None and model.efforts
            else " (conventional values; the catalog declares no levels for this model)"
        )
        print(
            f"Level '{argument}' is not available for {render_visible(session.wire_model())}; "
            f"allowed: {', '.join(levels)}{note}. Current choice unchanged."
        )


def render_visible(text: str) -> str:
    """Render for a plain terminal: keep printable characters, newline and
    tab; show every other control character visibly.  The original string is
    retained internally for history."""
    out: list[str] = []
    for char in text:
        code = ord(char)
        if char in ("\n", "\t") or 0x20 <= code <= 0x7E or code >= 0xA0:
            out.append(char)
        else:
            out.append(f"\\u{code:04x}")
    return "".join(out)


def _user_message(text: str) -> dict[str, Any]:
    return {"role": "user", "content": [{"type": "input_text", "text": text}]}


def _assistant_message(text: str) -> dict[str, Any]:
    # The accepted strict request contract accepts role/content text
    # messages with input_text parts for both roles.
    return {"role": "assistant", "content": [{"type": "input_text", "text": text}]}

@dataclass
class _ReasoningChannel:
    """Stream state of one reasoning channel (raw or summary): deltas of
    the open part append into one line, done/terminal replays are
    suppressed per part, and each part is shown at most once."""

    printed: bool = False
    open_key: str | None = None
    open_has_text: bool = False
    done: set[str] = field(default_factory=set)


@dataclass
class _TurnState:
    raw: _ReasoningChannel = field(default_factory=_ReasoningChannel)
    summary: _ReasoningChannel = field(default_factory=_ReasoningChannel)
    completed: Response | None = None
    failed: bool = False
    failed_error_code: str | None = None
    incomplete: bool = False
    server_error: bool = False


def _event_part_key(event: Any, index_name: str) -> str | None:
    """Stable per-part identity of one streamed reasoning delta/done
    event, used to show each reasoning part at most once across streamed
    deltas, done events and the terminal output.  Events without an item
    id fall back to channel-level dedup only."""
    item_id = getattr(event, "item_id", None)
    if type(item_id) is not str or not item_id:
        return None
    index = getattr(event, index_name, None)
    if type(index) is int and not isinstance(index, bool):
        return f"{item_id}:{index}"
    return item_id


def _channel(state: _TurnState, kind: str) -> _ReasoningChannel:
    return state.raw if kind == "raw" else state.summary


def _close_open(state: _TurnState) -> None:
    """Terminate any still-open reasoning line so following output starts
    on its own line."""
    for channel in (state.raw, state.summary):
        if channel.open_has_text:
            print(flush=True)
            channel.open_has_text = False
            channel.open_key = None


def _emit_reasoning_delta(
    state: _TurnState, kind: str, text: str, key: str | None
) -> None:
    """Append one streamed delta: consecutive deltas of the same part are
    flushed without newlines, so the part renders as contiguous text."""
    channel = _channel(state, kind)
    if channel.open_key != key or not channel.open_has_text:
        if channel.open_has_text:
            print(flush=True)
            channel.open_has_text = False
        if not channel.printed:
            print("Thinking" if kind == "raw" else "Thinking summary", flush=True)
            channel.printed = True
        channel.open_key = key
        channel.open_has_text = True
    print(render_visible(text), end="", flush=True)
    if key is not None:
        channel.done.add(key)


def _emit_reasoning_terminal(
    state: _TurnState, kind: str, text: str, key: str | None
) -> None:
    """Print one complete reasoning part (done event or terminal output).
    A part that was already streamed is not repeated; an unstreamed part
    (item-only, buffered response) is shown once on its own line."""
    channel = _channel(state, kind)
    if key is not None:
        if key in channel.done:
            if channel.open_key == key and channel.open_has_text:
                print(flush=True)
                channel.open_has_text = False
                channel.open_key = None
            return
    elif channel.printed:
        return
    if channel.open_has_text:
        print(flush=True)
        channel.open_has_text = False
    if not channel.printed:
        print("Thinking" if kind == "raw" else "Thinking summary", flush=True)
        channel.printed = True
    print(render_visible(text), flush=True)
    channel.open_key = None
    if key is not None:
        channel.done.add(key)


def _typed_part_texts(parts: Any) -> list[str]:
    """Texts of the typed reasoning parts that actually carry a ``text``
    field (``reasoning.text`` content parts and ``summary_text`` summary
    parts); non-text parts such as encrypted content are skipped."""
    if not isinstance(parts, (list, tuple)):
        return []
    texts: list[str] = []
    for part in parts:
        text = getattr(part, "text", None)
        if type(text) is str:
            texts.append(text)
    return texts


def _emit_reasoning_item(state: _TurnState, item: Any) -> None:
    """Render one reasoning item's parts through the shared per-part dedup
    path, whether it arrived as an output_item.done event or as part of
    the terminal response output."""
    item_id = getattr(item, "id", None)
    prefix = item_id if type(item_id) is str and item_id else None
    for index, text in enumerate(_typed_part_texts(getattr(item, "content", None))):
        if text:
            _emit_reasoning_terminal(state, "raw", text, f"{prefix}:{index}" if prefix else None)
    for index, text in enumerate(_typed_part_texts(getattr(item, "summary", None))):
        if text:
            _emit_reasoning_terminal(
                state, "summary", text, f"{prefix}:{index}" if prefix else None
            )


def _is_reasoning_item(item: Any) -> bool:
    return item is not None and getattr(item, "type", None) == "reasoning"


def _response_error_code(response: Any) -> str | None:
    """The machine code of a terminal error, when it is a plain string.
    Free-form error text is never rendered; only codes from the closed
    known set become a recognized diagnostic."""
    error = getattr(response, "error", None)
    if error is None:
        return None
    code = getattr(error, "code", None)
    return code if type(code) is str and code else None


def _validate_terminal(response: Any, state: _TurnState) -> None:
    """Only an explicitly successful completion carrying no error may
    commit.  Queued, in-progress, missing, mismatched or errored terminals
    never do."""
    if not isinstance(response, Response) or response.error is not None:
        state.failed = True
        return
    status = response.status
    if type(status) is str and status == "completed":
        state.completed = response
    elif type(status) is str and status == "incomplete":
        state.incomplete = True
    else:
        state.failed = True


def _handle_sdk_event(event: Any, state: _TurnState) -> None:
    """Dispatch one SDK-parsed stream event.  Frames the SDK parsed but
    that carry no string event type are malformed stream data; event
    types the client does not render (created, queued, in-progress,
    output-text deltas, unknown names) are accepted and ignored."""
    etype = getattr(event, "type", None)
    if type(etype) is not str:
        _fail("malformed stream data; nothing was committed")
    if etype == "response.reasoning_text.delta":
        delta = getattr(event, "delta", None)
        if type(delta) is str and delta:
            _emit_reasoning_delta(state, "raw", delta, _event_part_key(event, "content_index"))
    elif etype == "response.reasoning_summary_text.delta":
        delta = getattr(event, "delta", None)
        if type(delta) is str and delta:
            _emit_reasoning_delta(state, "summary", delta, _event_part_key(event, "summary_index"))
    elif etype == "response.reasoning_text.done":
        text = getattr(event, "text", None)
        if type(text) is str and text:
            _emit_reasoning_terminal(state, "raw", text, _event_part_key(event, "content_index"))
    elif etype == "response.reasoning_summary_text.done":
        text = getattr(event, "text", None)
        if type(text) is str and text:
            _emit_reasoning_terminal(
                state, "summary", text, _event_part_key(event, "summary_index")
            )
    elif etype == "response.output_item.done":
        # A reasoning item may arrive as a done event without deltas and
        # before the terminal snapshot repeats it.  Message items are NOT
        # answer material: only the completed response output is.
        item = getattr(event, "item", None)
        if _is_reasoning_item(item):
            _emit_reasoning_item(state, item)
    elif etype == "response.completed":
        _validate_terminal(getattr(event, "response", None), state)
    elif etype == "response.failed":
        state.failed = True
        state.failed_error_code = _response_error_code(getattr(event, "response", None))
    elif etype == "response.incomplete":
        state.incomplete = True
    elif etype == "error":
        # The error payload is untrusted endpoint text; report a fixed,
        # content-free label and never echo code/message/headers.
        state.server_error = True


def _extract_delivered(
    response: Response,
) -> tuple[str | None, int | None, int | None]:
    output = response.output
    if not isinstance(output, list):
        return None, None, None
    for out_index, item in enumerate(output):
        if not isinstance(item, ResponseOutputMessage) or item.type != "message":
            continue
        role = item.role
        if role is not None and role != "assistant":
            continue
        content = item.content
        if not isinstance(content, list):
            continue
        for content_index, part in enumerate(content):
            if (
                isinstance(part, ResponseOutputText)
                and part.type == "output_text"
                and type(part.text) is str
            ):
                return part.text, out_index, content_index
    return None, None, None


def _terminal_reasoning_items(response: Response) -> list[Any]:
    """Reasoning items of the terminal response, in output order.  Used
    for the buffered path and to complete streamed reasoning once per
    part; summary text lives in the item's ``summary`` parts, raw text in
    its ``content`` parts."""
    return [item for item in response.output if _is_reasoning_item(item)]



def _extract_original(
    ext: dict[str, Any], out_index: int, content_index: int
) -> tuple[str | None, str | None]:
    """Return (original_text, fixed_reason) from the ROOT
    llm_language_repair field only - no recursive search of same-named user
    data.  Index values must be real ints bound to the delivered part."""
    # Every extension value is untrusted; strict type checks keep invalid
    # shapes (bool versions, non-string statuses, list reasons, ...) on the
    # missing-original path instead of a crash.
    version = ext.get("version")
    if type(version) is not int or version != 1:
        return None, None
    status = ext.get("status")
    if type(status) is not str:
        return None, None
    if status == "unavailable":
        reason = ext.get("reason")
        fixed = (
            reason
            if type(reason) is str and reason in FIXED_UNAVAILABLE_REASONS
            else None
        )
        return None, fixed
    if status != "complete":
        return None, None
    oi = ext.get("output_index")
    ci = ext.get("content_index")
    if type(oi) is not int or type(ci) is not int:
        return None, None
    if oi != out_index or ci != content_index:
        return None, None
    text = ext.get("original_text")
    if type(text) is not str:
        return None, None
    return text, None


def _edit_summary(ext: dict[str, Any]) -> str | None:
    edits = ext.get("edits")
    if not isinstance(edits, list):
        return None
    if not edits:
        return "no edits"
    ranges: list[str] = []
    for edit in edits:
        if not isinstance(edit, dict):
            return None
        for key in (
            "original_start",
            "original_end",
            "repaired_start",
            "repaired_end",
        ):
            if type(edit.get(key)) is not int:
                return None
        if type(edit.get("replacement")) is not str:
            return None
        ranges.append(
            f"{edit['original_start']}-{edit['original_end']} -> "
            f"{edit['repaired_start']}-{edit['repaired_end']}"
        )
    return f"{len(ranges)} edit(s), code-point ranges: " + "; ".join(ranges)


def _finish_turn(
    response: Response,
    session: Session,
    user_text: str,
    state: _TurnState,
) -> None:
    delivered, out_index, content_index = _extract_delivered(response)
    if delivered is None or out_index is None or content_index is None:
        _close_open(state)
        print("No assistant answer text was returned; nothing was committed.")
        return
    for item in _terminal_reasoning_items(response):
        _emit_reasoning_item(state, item)
    _close_open(state)
    print("Delivered response")
    print(render_visible(delivered), flush=True)
    original: str | None = None
    fixed_reason: str | None = None
    # Unknown root-level fields (including the repair extension) survive
    # SDK parsing in model_extra; the value itself is still untrusted.
    extra = response.model_extra
    ext = extra.get("llm_language_repair") if isinstance(extra, dict) else None
    if isinstance(ext, dict):
        original, fixed_reason = _extract_original(ext, out_index, content_index)
    if original is not None:
        print("Original response")
        print(render_visible(original), flush=True)
        summary = _edit_summary(ext) if isinstance(ext, dict) else None
        if summary is not None:
            print(f"Applied edits: {summary}")
    else:
        print(MISSING_ORIGINAL_MESSAGE)
        if fixed_reason is not None:
            print(f"(reason: {fixed_reason})")
    session.history.append(_user_message(user_text))
    session.history.append(_assistant_message(delivered))


def _request_kwargs(
    session: Session, user_text: str, headers: dict[str, str], *, stream: bool
) -> dict[str, Any]:
    """The exact request: one main call per prompt, the delivered-only
    history, store disabled, the selected reasoning effort, and the
    client-owned details header.  No tools, conversation, metadata or
    retry semantics are added."""
    kwargs: dict[str, Any] = {
        "model": session.wire_model(),
        "input": [*session.history, _user_message(user_text)],
        "store": False,
        "stream": stream,
        "extra_headers": dict(headers),
    }
    if session.effort is not None:
        kwargs["reasoning"] = {"effort": session.effort}
    return kwargs


def _consume_sdk_json_fallback(stream_obj: Any, state: _TurnState) -> None:
    """R3 JSON fallback: when the server actually returns JSON for this
    request, read the body once and parse it with the SDK's own typed
    ``Response`` model.  Same single request; no retry, no mode fallback,
    and the terminal is still validated exactly like the buffered path."""
    raw = stream_obj.response.read()
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        _fail("endpoint returned malformed data; nothing was committed")
    if not isinstance(data, dict):
        _fail("endpoint returned non-object JSON; nothing was committed")
    try:
        response = Response.model_validate(data)
    except PydanticValidationError:
        _fail("endpoint returned data the SDK could not parse; nothing was committed")
    _validate_terminal(response, state)


def _known_failure_message(code: str) -> str:
    """Fixed, content-free diagnostic for a closed-set machine code."""
    if code == "responses_stream_event_not_supported":
        return (
            "the endpoint rejected the streamed request with the known gateway "
            "code 'responses_stream_event_not_supported': this gateway does not "
            "support streamed Responses events for this request shape. Rerun "
            "without --stream (buffered mode). Nothing was committed."
        )
    return "response failed on the endpoint with an unrecognized code; nothing was committed."


def _status_error_message(exc: APIStatusError) -> str:
    code = exc.code
    if type(code) is str and code in KNOWN_ENDPOINT_FAILURE_CODES:
        return _known_failure_message(code)
    if exc.status_code == 401:
        return "authentication was rejected by the endpoint (HTTP 401); nothing was committed."
    if exc.status_code == 403:
        return "the endpoint denied access (HTTP 403); nothing was committed."
    return f"HTTP {exc.status_code} from the endpoint; nothing was committed."


def send_once(session: Session, resolved: Resolved, user_text: str, stream: bool) -> None:
    headers: dict[str, str] = {}
    for name, value in resolved.extra_headers.items():
        # Load-time validation already rejects conflicts; this keeps the
        # client-owned framing and credentials winning regardless.
        if name.lower() not in CLIENT_OWNED_HEADERS:
            headers[name] = value
    headers[REPAIR_DETAILS_HEADER] = "1"
    # One owned SDK client per request: explicit zero retries, finite
    # timeout, and a redirect-refusing transport (the bearer token and the
    # conversation must never be resent to a different origin).
    client = OpenAI(
        api_key=resolved.token,
        base_url=resolved.url,
        max_retries=0,
        timeout=resolved.timeout,
        http_client=DefaultHttpxClient(follow_redirects=False, timeout=resolved.timeout),
    )
    state = _TurnState()
    try:
        if stream:
            stream_obj: Any = client.responses.create(
                **_request_kwargs(session, user_text, headers, stream=True)
            )
            content_type = (stream_obj.response.headers.get("Content-Type") or "")
            content_type = content_type.split(";", 1)[0].strip().lower()
            try:
                if content_type.endswith("json"):
                    _consume_sdk_json_fallback(stream_obj, state)
                else:
                    for event in stream_obj:
                        _handle_sdk_event(event, state)
            finally:
                stream_obj.close()
        else:
            response = client.responses.create(
                **_request_kwargs(session, user_text, headers, stream=False)
            )
            if not isinstance(response, Response):
                # The SDK returns non-object JSON bodies as-is; only a
                # typed Response object may validate as a terminal.
                _fail("endpoint returned non-object JSON; nothing was committed")
            _validate_terminal(response, state)
    except KeyboardInterrupt:
        _close_open(state)
        print("Request interrupted; nothing was committed.", flush=True)
        return
    except ClientError as exc:
        _close_open(state)
        print(f"error: {exc}", flush=True)
        return
    except APITimeoutError:
        _close_open(state)
        print(
            f"request timed out after {resolved.timeout:g} seconds; "
            "nothing was committed.",
            flush=True,
        )
        return
    except APIStatusError as exc:
        _close_open(state)
        print(_status_error_message(exc), flush=True)
        return
    except APIResponseValidationError as exc:
        _close_open(state)
        if 300 <= exc.status_code < 400:
            print(f"HTTP {exc.status_code} from the endpoint; nothing was committed.", flush=True)
        else:
            print(
                "endpoint returned data the SDK could not parse; nothing was committed.",
                flush=True,
            )
        return
    except APIConnectionError:
        _close_open(state)
        print(
            "could not reach the configured endpoint; nothing was committed.",
            flush=True,
        )
        return
    except APIError:
        # The SDK wraps stream-level endpoint error frames here; the
        # server text is untrusted, so only a fixed label is printed.
        _close_open(state)
        print("the endpoint reported an error; nothing was committed.", flush=True)
        return
    except OpenAIError:
        _close_open(state)
        print("an unexpected SDK error occurred; nothing was committed.", flush=True)
        return
    except json.JSONDecodeError:
        _close_open(state)
        if stream:
            # Malformed data inside an SSE frame is an honest stream
            # failure, not an ignorable frame.
            print("malformed stream data; nothing was committed.", flush=True)
        else:
            print("endpoint returned malformed data; nothing was committed.", flush=True)
        return
    except UnicodeDecodeError:
        _close_open(state)
        if stream:
            print("stream ended with invalid UTF-8; nothing was committed.", flush=True)
        else:
            print("endpoint returned malformed data; nothing was committed.", flush=True)
        return
    except httpx2.TimeoutException:
        # Raw transport timeout from a direct owned response read (the
        # SDK-wrapped iteration path raises APITimeoutError instead).
        _close_open(state)
        print(
            f"request timed out after {resolved.timeout:g} seconds; "
            "nothing was committed.",
            flush=True,
        )
        return
    except httpx2.RequestError:
        _close_open(state)
        print(
            "connection problem while reading the response; nothing was committed.",
            flush=True,
        )
        return
    finally:
        client.close()
    _close_open(state)
    if state.failed:
        if state.failed_error_code is not None:
            print(f"error: {_known_failure_message(state.failed_error_code)}", flush=True)
        else:
            print("response failed on the endpoint; nothing was committed.", flush=True)
        return
    if state.incomplete:
        print("response incomplete on the endpoint; nothing was committed.", flush=True)
        return
    if state.server_error:
        print("the endpoint reported an error; nothing was committed.", flush=True)
        return
    if state.completed is None:
        print("stream ended before completion; nothing was committed.", flush=True)
        return
    _finish_turn(state.completed, session, user_text, state)


def run_loop(session: Session, resolved: Resolved, stream: bool) -> int:
    mode = "stream (live events)" if stream else "buffered (completed response)"
    print(
        f"repair chat client - request mode: {mode} - "
        "/help for commands, /quit to exit"
    )
    while True:
        line = _read_line("you> ")
        if line is None:
            return 0
        text = line.strip()
        if not text:
            continue
        if text == "/quit":
            return 0
        if text in ("/help", "/?"):
            print(HELP_TEXT)
            continue
        if text == "/model":
            if not session.models:
                print(
                    "No catalog was loaded; select a model directly with "
                    "/model MODEL_ID (there is no menu without a catalog)."
                )
                continue
            entries = tuple(
                (
                    render_visible(m.slug)
                    + (
                        f"  ({render_visible(m.display_name)})"
                        if m.display_name
                        else ""
                    ),
                    m.slug,
                )
                for m in session.models
            )
            value = _menu(
                "Models (enter a number or slug, 0 to cancel):",
                entries,
                session.wire_model(),
                "model",
            )
            if value == _MENU_EXIT:
                return 0
            if value != _MENU_CANCELLED:
                index = next(i for i, m in enumerate(session.models) if m.slug == value)
                _select_model(session, index)
            continue
        if text.startswith("/model "):
            _command_model_direct(session, text[len("/model "):].strip())
            continue
        if text == "/thinking":
            entries = tuple((level, level) for level in allowed_efforts(session))
            title = (
                f"Thinking effort for {render_visible(session.wire_model())} "
                "(enter a number or level, 0 to cancel):"
            )
            model = session.current_model()
            if model is None or not model.efforts:
                title += "  [conventional values; the catalog declares no levels for this model]"
            value = _menu(title, entries, session.effort, "thinking")
            if value == _MENU_EXIT:
                return 0
            if value != _MENU_CANCELLED:
                session.effort = value
                print(f"Next request will use thinking effort: {value}")
            continue
        if text.startswith("/thinking "):
            _command_thinking_direct(session, text[len("/thinking "):].strip())
            continue
        if text.startswith("/"):
            print(HELP_TEXT)
            continue
        send_once(session, resolved, text, stream)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="repair_chat.py",
        description=(
            "Plain-terminal comparison client: Codex-style TOML profiles and "
            "model catalogs, server thinking, and the delivered and original "
            "answers from the language-repair Responses endpoint via the "
            "official OpenAI Python SDK.  Buffered completed responses are "
            "the default; --stream requests live streamed events. Session "
            "commands: /model and /thinking show numbered menus and read the "
            "selection from the next line; /help and /quit."
        ),
    )
    parser.add_argument("--profile", help="profile NAME (table or standalone file)")
    parser.add_argument("--config", help="explicit configuration file")
    parser.add_argument(
        "--codex-home",
        help=(
            "configuration root directory (default ~/.codex; the CODEX_HOME "
            "environment variable is intentionally ignored)"
        ),
    )
    parser.add_argument("--models", help="explicit model catalog JSON file")
    parser.add_argument("--model", help="select the model (slug or explicit ID)")
    parser.add_argument(
        "--thinking",
        help="select the reasoning effort for MAIN requests only",
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help=(
            "request streamed Responses events (live reasoning display); "
            "the default is the buffered completed response"
        ),
    )
    parser.add_argument(
        "--base-url",
        help="override the API base URL (for example a local proxy)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"finite request timeout in seconds (default {int(DEFAULT_TIMEOUT_SECONDS)})",
    )
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be a positive finite number of seconds")
    return args


def load_settings(args: argparse.Namespace) -> Settings:
    codex_home = (
        Path(os.path.expanduser(args.codex_home))
        if args.codex_home
        else Path.home() / ".codex"
    )
    if args.config:
        config_file = Path(os.path.expanduser(args.config))
        if not config_file.is_file():
            _fail(f"configuration file not found: {config_file}")
    else:
        config_file = codex_home / "config.toml"
    root_cfg: dict[str, Any] = {}
    if config_file.is_file():
        root_cfg = load_toml_file(config_file)
    base, context = select_profile(args, root_cfg, config_file)
    provider_name = _str_key(base, "model_provider", context)
    if not provider_name:
        _fail(
            "no 'model_provider' in the selected configuration/profile; "
            "name the provider table under model_providers"
        )
    provider = load_provider(provider_name, root_cfg, base, context)
    if provider.wire_api is not None and provider.wire_api != "responses":
        _fail(
            f"provider '{provider.name}' uses wire_api '{provider.wire_api}'; "
            "only the Responses wire API is supported by this example"
        )
    url = build_url(provider.base_url, args.base_url)
    token = resolve_token(provider)
    return Settings(
        resolved=Resolved(
            url=url,
            token=token,
            extra_headers=dict(provider.http_headers),
            timeout=args.timeout,
        ),
        base=base,
        config_dir=config_file.parent,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings(args)
        session = initialize_session(args, settings.base, settings.config_dir)
    except ClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return run_loop(session, settings.resolved, args.stream)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    raise SystemExit(main())
