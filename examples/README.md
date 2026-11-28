# Example: plain-terminal comparison client

`repair_chat.py` is a runnable plain-terminal client for this proxy: it
compares the **delivered answer** with the **original answer** when the
language-repair extension is actually returned, and displays the
server-provided thinking. It loads a Codex-style TOML configuration and model
catalog, and its actual request/event boundary is the **official OpenAI
Python SDK** (`openai==3.60.0`, Responses API), pinned in the `client`
dependency group:

```bash
# uv project: install the pinned SDK and run the client in one command
# (the profile's configured gateway/proxy base URL is used as-is)
uv run --frozen --group client --python 3.12 python examples/repair_chat.py --profile early-beta
```

Optionally install the environment first with `uv sync --frozen --group client`
and then run the same `uv run` command.  No global install is required.
Outside the uv project, use one interpreter for both install and run:

```bash
python3.12 -m pip install openai==3.60.0
python3.12 examples/repair_chat.py --profile early-beta
```

The request mode is fixed at startup and announced on the first line:

* **buffered (default)** — one request, one completed `Response`; the
  server's reasoning and the delivered answer are shown when the response
  completes.
* **`--stream`** — the SDK's streamed Responses events; server reasoning
  deltas are printed as they arrive.  If the server actually answers a
  stream request with `application/json`, that body is parsed with the
  SDK's typed `Response` model on the same single request (no retry, no
  mode fallback).

Against the current gateway, buffered requests complete on both the repaired
and the unrepaired RTX 3090 routes (reasoning item and delivered answer
present; the repaired route currently returns no original extension, so the
client prints the fixed missing-original message).  A streamed text-only
request is rejected by that gateway with the known code
`responses_stream_event_not_supported`, which the client reports as a fixed,
content-free diagnostic.  Buffered completion is not live early-reasoning
proof: streamed deltas are only displayed when the endpoint actually streams
them.

`--model qwen3.8-27b-rtx3090-repaired` selects the gateway routing alias (the
`-repaired` suffix is a gateway alias, not an upstream model ID) through the
profile's configured gateway. Use `--base-url` only when pointing directly at
an upstream or an installed proxy: then supply that endpoint's actual model
ID and its own credentials — a gateway alias combined with direct
credentials is an invalid combination. See INSTALL.md for the direct proxy
setup.

The session is ordinary terminal scrollback with a prompt: plain numbered
`/model` and `/thinking` menus (the selection is read from the next line),
append-only lines with no ANSI cursor movement, alternate screen, clearing or
spinners. It changes the reasoning of the **main** requests only — the
proxy's reviewer and its settings are untouched.

Useful options (`--help` lists all of them):

| Option | Meaning |
| --- | --- |
| `--profile NAME` | select a profile (see below) |
| `--config FILE` | explicit configuration file (default `<root>/config.toml`) |
| `--codex-home DIR` | configuration root; default `~/.codex`. The `CODEX_HOME` environment variable is intentionally ignored, so the location is explicit |
| `--models FILE` | explicit model catalog JSON |
| `--model SLUG` | select the model |
| `--thinking LEVEL` | initial reasoning effort for MAIN requests |
| `--stream` | request streamed Responses events (live reasoning display); default is the buffered completed response |
| `--base-url URL` | override the API base (for example a local proxy) |
| `--timeout SECONDS` | finite request timeout, default 900 |

## Configuration

The configuration root is `~/.codex` unless `--codex-home` is given. A profile
is resolved, in order, as:

1. the `[profiles.NAME]` table inside the selected config file — a **table
   profile overrides only the root settings it names** and inherits the root
   `model_provider`, effort defaults and provider definitions; or
2. when that table is absent, the standalone `NAME.config.toml` file next to
   the config file — a **standalone file is the complete selected
   configuration**: it supplies its own model, provider, effort and catalog,
   and is not merged over the root. If its provider table does not name the
   selected provider, the root `model_providers` definitions are consulted as
   a lookup only; and
3. when neither exists the client fails with a clear error; it never falls
   back silently to unrelated root model/provider settings.

For each setting, the command line overrides the selected settings; a table
profile inherits the root defaults for everything it does not name; a
standalone file's model/effort/catalog settings are complete and do not fall
back to the root (root provider definitions are a lookup only); and the
selected model's declared catalog default is a fallback for the reasoning
**effort** only — it never supplies a missing model.
`~` is expanded; a relative `model_catalog_json` is resolved against the
selected config directory. `--models` overrides `model_catalog_json`. A
`models.json` next to the config file is used only when neither an explicit
path nor a catalog path names a file.

The complete synthetic standalone example below matches the command above
(replace the gateway URL with the one you use; never copy real credentials
into examples):

```toml
# ~/.codex/early-beta.config.toml
model = "qwen3.8-27b"
model_provider = "slaif"
model_reasoning_effort = "low"
model_catalog_json = "early-beta-models.json"

[model_providers.slaif]
name = "SLAIF gateway"
base_url = "https://gateway.example/v1"
wire_api = "responses"
env_key = "SYNTHETIC_API_KEY"
```

`~/.codex/early-beta-models.json`:

```json
{
  "models": [
    {
      "slug": "qwen3.8-27b",
      "display_name": "Qwen 3.8 27B",
      "default_reasoning_level": "medium",
      "supported_reasoning_levels": [
        {"effort": "none"}, {"effort": "low"},
        {"effort": "medium"}, {"effort": "xhigh"}
      ]
    },
    {
      "slug": "qwen3.8-27b-rtx3090-repaired",
      "display_name": "Qwen 3.8 27B repaired",
      "default_reasoning_level": "medium",
      "supported_reasoning_levels": [
        {"effort": "none"}, {"effort": "low"},
        {"effort": "medium"}, {"effort": "xhigh"}
      ]
    }
  ]
}
```

The provider authenticates with `env_key` (an environment variable) or
`experimental_bearer_token` from the provider table; the credential value is
never printed, logged or written back, and an OAuth-only provider gets a fixed
actionable explanation instead of an `auth.json` read. Configured
`http_headers` are accepted with validation, but the client-owned
`Authorization`, `Content-Type` and `X-LLM-Language-Repair-Details` headers
cannot be overridden from configuration, and values containing CRLF are
rejected.

## Session

Every line you type is one user message; blank lines are ignored; an unknown
slash command shows the help; `/quit` or end-of-input exits cleanly; Ctrl+C
during a request closes the owned HTTP response and returns the prompt without
a false completion.

`/model` and `/thinking` print a plain numbered menu and read the selection
from the **next** input line (a number or the name itself; blank or `0`
cancels; an invalid selection leaves the current choice unchanged):

```text
you> /model
Models (enter a number or slug, 0 to cancel):
  1. qwen3.8-27b  (Qwen 3.8 27B) [current]
  2. qwen3.8-27b-rtx3090-repaired  (Qwen 3.8 27B repaired)
select> 2
Next request will use model: qwen3.8-27b-rtx3090-repaired
you> /thinking
Thinking effort for qwen3.8-27b-rtx3090-repaired (enter a number or level, 0 to cancel):
  1. none
  2. low [current]
  3. medium
  4. xhigh
select> 3
Next request will use thinking effort: medium
```

The menus list exactly the entries of your catalog file (your own file may
declare more choices). `/model SLUG|N` and `/thinking LEVEL` are direct
conveniences; they do not replace the menu workflow. Switching models
preserves the chosen effort when the new model supports it and otherwise
resets it to the model's declared default with a clear message — never a
silent unsupported effort. If no catalog is loaded, `/model` takes an
explicit model ID and effort uses conventional values without an invented
support claim. Menus and slash commands make zero API calls; the selection
applies to the next request.

A session with thinking and a repaired answer (illustrative output, not a
real model transcript):

```text
repair chat client - request mode: buffered (completed response) - /help for commands, /quit to exit
you> Napiši kratek stavek o sončnem dnevu.
Thinking
Kratek stavek o sončnem dnevu.
Delivered response
Lep sončen dan 😀.
Original response
Lep sončn dan 😀.
Applied edits: 1 edit(s), code-point ranges: 4-9 -> 4-10
you> /quit
```

When no valid extension comes back (absent, unavailable or invalid metadata),
the original block is exactly the fixed message, optionally followed by the
fixed content-free reason; the delivered answer is still displayed and
committed:

```text
you> Napiši kratek stavek o deževnem dnevu.
Delivered response
Dan je deževen.
Original response was not provided.
```

## What it sends, and honest limits

- Each turn is one `POST /v1/responses` with `store: false`, `stream: false`
  (buffered default) or `stream: true` (with `--stream`), the exact selected
  model, `reasoning.effort`, the
  `X-LLM-Language-Repair-Details: 1` header, and an in-memory text-only
  history containing only successful user prompts and delivered assistant
  answers (reasoning, original text, extension metadata and failed or partial
  turns are never sent; there is no on-disk transcript, `previous_response_id`
  or session linkage, and no tools or catalog coding instructions).
- `reasoning.effort` changes **main user requests only**. The proxy's
  reviewer, its low default and all proxy settings are untouched.
- Thinking is displayed only when the server actually provides reasoning —
  streamed deltas or the completed JSON / item / done fallback content; a
  higher effort does not imply support, and the client never fabricates
  thinking. Raw text and summary channels are kept distinct.
- The original answer appears only when a deployed proxy returns a valid
  `llm_language_repair` extension for the completed response and the
  `X-LLM-Language-Repair-Details` request header survives any gateway in
  between. There is no proxy startup switch to enable: this client opts in on
  every request, and an old, unsupported or missing extension simply prints
  the fixed missing-original message. The default (transparent) proxy accepts
  `reasoning` on the main request; the legacy strict request validator does
  not, so point this client at a normal/transparent deployment.
- Credentials, raw headers, API error details and tracebacks are never
  echoed; failures are fixed, actionable, content-free messages. Selected
  settings such as the model name and effort are intentionally displayed.
  HTTP redirects are not followed, so credentials and prompts are never
  forwarded to another origin.
- Malformed streams, non-completed terminal states, premature EOF and
  transport errors fail honestly: the turn is not committed, and the next
  request history keeps only the successful turns plus the new prompt.
- It never starts an AI process, reads `auth.json` or performs OAuth, sends
  tools or `previous_response_id`, retries, or keeps an on-disk transcript; it
  supports the Responses wire API only, and it is not a full Codex client.
- Transport is the official SDK with `max_retries=0`, a finite timeout per
  request, deterministic cleanup, and a redirect-refusing transport; the
  hand-written urllib/SSE transport is gone rather than kept as a fallback.

Contract tests exercising the real script over an owned loopback HTTP server
live in `tests/contract/test_repair_chat.py` (synthetic homes, catalogs and
keys only).
