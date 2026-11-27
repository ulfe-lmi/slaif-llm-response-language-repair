# Transparent proxy chaining

The normal launcher forwards API traffic to the configured upstream and applies
language repair only where it can safely recognize a completed plain-text
Responses answer. Eligibility controls repair, not whether an API call is allowed.
The upstream retains responsibility for its supported APIs and authentication.

## Attach to an existing chain

After installing Python 3.12, the index and pinned Pandoc using [INSTALL](../INSTALL.md),
set the following in the same dedicated shell:

```bash
export CONCEPT_PORT=18023
export CONCEPT_HOST=0.0.0.0
export CONCEPT_UPSTREAM_URL=http://127.0.0.1:18021/v1
export CONCEPT_REVIEWER_URL=http://127.0.0.1:18021/v1
export CONCEPT_REVIEWER_MODEL=qwen3.8-27b
export CONCEPT_TIMEOUT=300
export CONCEPT_HEADER_TIMEOUT=300
export CONCEPT_DELIVERY_TIMEOUT=300
export CONCEPT_REVIEWER_TIMEOUT=120
python3.12 -B concept-verification/proxy.py
```

`CONCEPT_INDEX`, `CONCEPT_PANDOC` and the reviewer credential `QWEN_API_KEY` remain
configured as in INSTALL. Every main request carries the client's own upstream
credential. Choose timeout values to suit the model; they are finite operational
budgets, not a guarantee about response latency.

`CONCEPT_REVIEWER_TIMEOUT=120` overrides only the `timeout_seconds` of the
selected review policy file (environment > file) and is validated with the
same rules before bind; the other review parameters (allowance, batch bounds,
thinking effort) come from the policy file. See
[INSTALL.md — Review policy configuration](../INSTALL.md#review-policy-configuration).

Local clients use `http://127.0.0.1:18023/v1`; clients on other machines use
`http://YOUR-PROXY-HOST:18023/v1`. For the existing three-hop stack:

```text
Client → 18023 language repair → 18021 image filter → 18022 gateway → 18020 vLLM
```

Main generation and any review request use the upstream endpoint directly. The
reviewer never calls the repair listener, preventing recursion and requiring no
second model. The existing services and their configuration need no changes.
`CONCEPT_HOST=0.0.0.0` listens on all IPv4 interfaces, matching the existing
three services. The default is `127.0.0.1` when `CONCEPT_HOST` is unset.
Both local and network clients must supply the upstream API key; missing or
invalid credentials retain the upstream's unauthorized response. Strict legacy
mode remains loopback-only.

## What passes through

- Other API paths and queries, including `/v1/models`, `/health`, Chat Completions,
  stored-response routes, and ordinary GET/HEAD/POST/PUT/PATCH/DELETE/OPTIONS methods.
- Request bodies, including tools, function-call history, images and unknown
  vendor fields, without JSON reserialization or field removal.
- Client authorization and other end-to-end request headers; upstream status,
  body and end-to-end response headers, including repeated headers and errors.
  The private per-request control header
  `X-LLM-Language-Repair-Details` is the exception: the proxy consumes it and
  strips it case-insensitively from the forwarded headers of **every** route.
  It is effective only on `POST /v1/responses` (absent or one trimmed `0`
  means off, one trimmed `1` opts in; duplicates or any other value are a
  fixed content-free `400` there, with zero main calls); other routes relay
  their request bodies and responses exactly as before.
- Tool-call, structured, stored, background, incomplete and otherwise ineligible
  responses without repair, with defined logprobs removed where recognized.

HTTP framing is reconstructed for each hop. Host, connection headers and headers
named by `Connection` are not forwarded. Chunked uploads are decoded and sent
with the resulting Content-Length; request trailers are consumed, not forwarded.
Uploads have a 256 MiB bound, and the upstream may impose a smaller limit.
This is an HTTP API reverse proxy, not a CONNECT tunnel or WebSocket gateway.

## When repair runs

Only `POST /v1/responses` with explicit `store: false`, no stored-conversation
linkage or background generation, and no structured-output format is considered.
The request may declare tools or contain multimodal input and tool history.
The response must independently satisfy the existing completed plain-text
envelope and SSE ownership rules. Function arguments, reasoning and tool results
are never repair targets. A final text answer may therefore be repaired even
when its request declared tools.

Eligible JSON responses are buffered before inspection and repair. Eligible SSE
forwards proven initial reasoning as frames complete and buffers from the first
unproven frame on; the answer itself is never forwarded early (ARCHITECTURE
section 5.3). Other traffic streams through as bytes arrive. If a candidate
response exceeds the 8 MB repair-capture limit, its
buffered prefix and remaining bytes pass through without repair. Compressed
responses pass through without decompression or repair.

On the eligible repair path, the existing detector, Pandoc protection, isolated
reviewer, acceptance checks and defined logprobs removal remain in use. If bytes
change, stale entity validators are removed and Content-Length is recomputed.
Without eligible suspicion there are zero review calls. This forwarding change
does not establish linguistic quality.

With the opt-in header set, an eligible completed JSON or SSE response adds
the single root `llm_language_repair` metadata field (original final answer
plus the final applied edits in code-point coordinates); in streaming mode the
unchanged prefix frames still arrive first and the metadata appears only in
the terminal completion. The header never reaches the upstream. See
[INSTALL.md — Original answer and applied-edit details](../INSTALL.md#original-answer-and-applied-edit-details-opt-in).

## Legacy strict mode

`CONCEPT_PASSTHROUGH=0` selects the previous narrow API admission behavior.
Direct Python callers of `ConceptProxy` retain that strict default; pass
`passthrough_mode=True` to select the forwarding handler. The architecture's
strict request-profile and normalization descriptions apply to that legacy mode;
this document describes the normal launcher's forwarding layer.
