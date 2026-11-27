# Example: plain-terminal comparison client

`repair_chat.py` is a runnable, deliberately simple example of a non-agent
Responses client for this proxy. It loads a Codex-style TOML configuration and
model catalog, runs a text-only chat loop, and shows — in ordinary terminal
scrollback — the server-provided thinking (when the server sends any), the
authoritative delivered answer, and the original answer when the language-repair
extension is actually returned.

It is a comparison example, not a product component and not a full Codex
client. It never starts an AI process, reads `auth.json` or performs OAuth,
retries requests, keeps any on-disk transcript, or manipulates the terminal
screen: output is plain append-only lines with a prompt (no ANSI cursor
movement, alternate screen, clearing or spinners), and it supports the
Responses wire API only.

## Run it

Python 3.12 standard library only — no new SDK or package is required:

```bash
# against the local installed proxy (see INSTALL.md for the proxy setup)
python3.12 examples/repair_chat.py --profile early-beta \
  --model qwen3.8-27b-rtx3090-repaired --base-url http://127.0.0.1:18024/v1
```

Useful options (`--help` lists all of them):

| Option | Meaning |
| --- | --- |
| `--profile NAME` | select a profile (see below) |
| `--config FILE` | explicit configuration file (default `<root>/config.toml`) |
| `--codex-home DIR` | configuration root; default `~/.codex`. The `CODEX_HOME` environment variable is intentionally ignored, so the location is explicit |
| `--models FILE` | explicit model catalog JSON |
| `--model SLUG` | select the model |
| `--thinking LEVEL` | initial reasoning effort for MAIN requests |
| `--base-url URL` | override the API base (for example the local proxy) |
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

For each setting the precedence is: command line > selected profile > root
defaults > the selected model's declared catalog default.
`~` is expanded; a relative `model_catalog_json` is resolved against the
selected config directory. `--models` overrides `model_catalog_json`. A
`models.json` next to the config file is used only when neither an explicit
path nor a catalog path names a file.

Minimal synthetic configuration (never copy real credentials into examples):

```toml
# ~/.codex/config.toml
model = "qwen3.8-27b"
model_provider = "slaif"
model_reasoning_effort = "low"
model_catalog_json = "models.json"

[model_providers.slaif]
name = "SLAIF gateway"
base_url = "http://127.0.0.1:18024/v1"
wire_api = "responses"
env_key = "SYNTHETIC_API_KEY"
```

```json
// ~/.codex/models.json
{
  "models": [
    {
      "slug": "qwen3.8-27b",
      "display_name": "Qwen 3.8 27B",
      "default_reasoning_level": "low",
      "supported_reasoning_levels": [
        {"effort": "minimal"}, {"effort": "low"},
        {"effort": "medium"}, {"effort": "high"}
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
  2. qwen3.8-27b-rtx3090-repaired
select> 2
Next request will use model: qwen3.8-27b-rtx3090-repaired
you> /thinking
Thinking effort for qwen3.8-27b-rtx3090-repaired (enter a number or level, 0 to cancel):
  1. minimal
  2. low [current]
  3. medium
  4. high
select> 3
Next request will use thinking effort: medium
```

`/model SLUG|N` and `/thinking LEVEL` are direct conveniences; they do not
replace the menu workflow. Switching models preserves the chosen effort when
the new model supports it and otherwise resets it to the model's declared
default with a clear message — never a silent unsupported effort. If no
catalog is loaded, `/model` takes an explicit model ID and effort uses
conventional values without an invented support claim. Menus and slash
commands make zero API calls; the selection applies to the next request.

A session with thinking, a repaired answer and the missing-original message
looks like this:

```text
repair chat client - /help for commands, /quit to exit
you> Napiši en kratek stavek o sončnem dnevu.
Thinking
Sončen dan, topel zrak, modro nebo.
Delivered response
Napišem kratek stavek o sončnem dnevu.
Original response
Napišem kratek stavek o sončnem dnevu.
Applied edits: 1 edit(s), code-point ranges: 4-7 -> 4-7
you> /quit
```

(If the extension is absent, unavailable or invalid the last block is exactly
`Original response was not provided.`, optionally followed by the fixed
content-free reason; the delivered answer is still displayed and committed.)

## What it sends, and honest limits

- Each turn is one `POST /v1/responses` with `store: false`, `stream: true`,
  the exact selected model, `reasoning.effort`, the
  `X-LLM-Language-Repair-Details: 1` header, and an in-memory text-only
  history containing only successful user prompts and delivered assistant
  answers (reasoning, original text, extension metadata and failed or partial
  turns are never sent; there is no on-disk transcript, `previous_response_id`
  or session linkage, and no tools or catalog coding instructions).
- `reasoning.effort` changes **main user requests only**. The proxy's
  reviewer, its low default and all proxy settings are untouched.
- Thinking is displayed only when the server actually streams reasoning
  events; a higher effort does not imply support, and the client never
  fabricates thinking. Raw text and summary channels are kept distinct.
- The original answer appears only when a deployed proxy with the repair
  details enabled returns a valid `llm_language_repair` extension whose
  headers survive any gateway in between; an unrepaired model, an off proxy,
  or a filtered header simply means no extension and the fixed
  missing-original message. The default (transparent) proxy accepts
  `reasoning` on the main request; the legacy strict request validator does
  not, so point this client at a normal/transparent deployment.
- Credentials, configuration values, headers and API error details are never
  echoed; failures are fixed, actionable, content-free messages. HTTP
  redirects are not followed, so credentials and prompts are never forwarded
  to another origin.
- Malformed streams, non-completed terminal states, premature EOF and
  transport errors fail honestly: the answer is not committed and the next
  request starts from only the new prompt.

Contract tests exercising the real script over an owned loopback HTTP server
live in `tests/contract/test_repair_chat.py` (synthetic homes, catalogs and
keys only).
