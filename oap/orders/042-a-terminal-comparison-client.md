# Work order 042-a — Plain-terminal Python comparison client

Status: FINAL

## Identity

Objective042, round042-a, CREATE_NEW_PR, branch oap/042-terminal-comparison-client; repository ulfe-lmi/slaif-llm-response-language-repair, defaultmain, accepted base8fc78802c907203c6cf7b8be9fa5609690890e26. Remap inert042 seed to this owner-requested client example; no restart of historical queue. One coherent example PR. Only original foreground primary coding pane under normal tmux/FIFO; no new agent, codex exec, background model, provider/home/auth change or restart of a live pane.

## Provenance

H: Owner2026-11-27 requests Python quasi-harness loading Codex TOML and models.json, --profile resolving in ~/.codex, /model selection, /thinking changing MAIN USER request reasoning (never proxy reviewer), /quit, both answer displays, explicit missing-original message, and thinking display. Owner adds ordinary scrollback/no fancy screen manipulation. A: accepted ARCHITECTURE2.4/INSTALL original-details contract. E: inspected real early-beta.config.toml has standalone root model/provider/effort/model_catalog_json; models catalog contains models[].slug, display_name, supported_reasoning_levels[].effort/default_reasoning_level. Responses provider credentials may use env_key or experimental_bearer_token; values never printed/copied. Official Codex config and Responses streaming-event docs consulted. User is asking a non-agent API client, not another Codex process.

## Current verified state

Main8fc78802c907203c6cf7b8be9fa5609690890e26 accepted, PR47 merged after all four checks plus independent semantic review; localmain/private acceptedrefs refreshed with backups. Active041-a completed; both original foreground sessions idle. No open PR observed. Tracked clean; preserve all ignored/untracked corpus/scratch/key.txt and branches/worktrees/private history. No deployed service/backend/GPU/network change authorized by this client task.

## Governance

No architecture/law/context change needed: external illustrative client for the existing API. All accepted sources/projections must remain byte-identical. Candidate-review requires a nonempty known allowlist; permit the following manifest only for validator compatibility, with NO actual change planned or authorized absent a demonstrated necessary task-scoped projection fix brought to root:

- oap/governance/MANIFEST.json

Necessary existing GENERATED-FILES records for scoped doc/workflow hashes may update. CI candidate-review pins only to exact042-a/orderdigest/base above; all guards/timeouts retained. FrozenPLAN/CRITICAL/seeds/helpers/oldordersreports unchanged.

## Goal and dependencies

Deliver an understandable runnable Python example that calls the selected Responses model through the configured gateway/proxy, streams server-provided thinking promptly in ordinary terminal scrollback, then prints authoritative delivered answer and original text when actually returned. Depends on accepted041 extension. It is a simple text client, no tools/agent actions/credential mutation or proxy runtime change.

## Scope

New examples/repair_chat.py and concise examples/README.md; meaningful tests at tests/contract/test_repair_chat.py, using actual script CLI/HTTP entry point and synthetic temporary Codex-home/config/catalog/owned-loopback upstream. One short link in INSTALL existing repair-details section (README only if necessary for discoverability, avoid redundant pointers). CI pins, GENERATED-FILES exact scoped records, new order/active/report and paired research machine counter only. Primary owns all implementation/docs/tests; no second-coder work needed for this bounded example.

## Non-goals

Do not modify runnable proxy/repair engine/reviewer/config/Pandoc/protection/history/error/usage/logprobs contracts, architecture/governance law, dependencies/lock, backend/gateway/services, real Codex config/auth/catalog files or tools. Do not launch any Codex client/agent for testing. No real model request needed: automated tests use synthetic transports through actual client HTTP. No auto retry, OAuth/browser-token reading/refresh, full Codex emulation, encrypted reasoning replay, tools/image support, persistent transcripts, research programme or human evaluation dataset. No terminal screen libraries, ANSI clearing/cursor/alternate screen/status animation. No raw customer/review data or credentials in report/logs/public fixtures.

## Files and boundaries

Read compact coding law/read set and exact order. Existing INSTALL repair-details subsection is API authority; bounded responses_contract.py request validation and reasoning_streaming tests/event forms useful. Real config facts are supplied above; coding must NOT read key.txt or real credential-bearing TOML/auth files. Standard references: https://developers.openai.com/codex/config-reference/ and https://developers.openai.com/api/reference/resources/responses/streaming-events . Example config/catalog fixtures must be synthetic/portable, no private machine roots/credentials. Inspect test discovery and baseline driver before selecting meaningful checks; no unrelated suite cleanup.

## Requirements

1. Self-contained Python3.12 plain text CLI, preferably standard library only so `python3.12 examples/repair_chat.py --profile early-beta --model qwen3.8-27b-rtx3090-repaired` works without a new SDK/package. Never start an AI process. Options --profile, --config, --models (catalog override), --model, --thinking, --base-url and finite --timeout (default900seconds) with --help. Default configuration root ~/.codex, explicit --codex-home allowed; do not accidentally inherit strategic CODEX_HOME. Profiles: standard config.toml [profiles.NAME] overrides root defaults, plus owner-observed standalone NAME.config.toml fallback when named table absent; explicit --config chooses that file and table selection if requested, with documented deterministic precedence and clear missing/invalid-profile error (never silently use unrelated model/provider). Expand ~ and resolve relative model_catalog_json against selected config directory. --models overrides model_catalog_json; models.json next to config may be used only if no explicit/catalog path. No network model discovery required. Reject non-Responses wire_api clearly, support selected provider base_url and env_key / experimental_bearer_token without ever printing secrets. HTTP-header settings may be supported with clear validation; never run commands from config. No auth.json/OAuth access; unresolved OAuth-only auth gets an actionable fixed explanation. Local endpoint --base-url override must work. URL construction appends /responses exactly once to API base (normally ends /v1), reject unsupported scheme/userinfo.
2. Catalog models[].slug/display_name/default_reasoning_level/supported_reasoning_levels[].effort; optionally accept ordinary API id entries if simple, not required. /model lists choices+current; /model SLUG or NUMBER selects and reports next-request setting. /thinking lists current+allowed levels; /thinking LEVEL validates selected catalog and modifies only `reasoning.effort` of future MAIN calls, never proxy reviewer settings. Initialization precedence CLI > selected profile/root > selected model default for effort. On model switch preserve chosen effort if supported else reset to selected model's declared default with a clear message; no silent unsupported effort. If no catalog exists, explicit model IDs and documented conventional effort values may be used; no invented support claim. /quit and EOF exit cleanly; blank input ignores; unknown commands show help; Ctrl+C during request closes owned HTTP response and returns prompt/clean exit with no false completion.
3. Request POST /v1/responses, store:false, stream:true, exact model, reasoning.effort and X-LLM-Language-Repair-Details:1. No tools, previous_response_id, conversation/session linkage, catalog coding instructions or proxy review settings. Basic in-memory text-only conversation is allowed: append only successful USER prompt and authoritative delivered ASSISTANT text, using simple role/content text messages accepted by actual strict request contract. Never send reasoning, original_text, extension metadata or failed/partial turns as assistant history; no on-disk transcript. A next prompt must not make extra model calls beyond one request; slash commands zero requests. No retries to hide unsupported/missing fields.
4. Incrementally parse actual Responses SSE (comments/blank frames/multiple data lines/UTF8/CRLF); thinking from response.reasoning_text.delta and response.reasoning_summary_text.delta prints immediately/flushed, with appropriate readable heading, before waiting for final answer. Handle done/item/terminal reasoning content when no deltas supplied without duplication; actual server-provided text only, no fabricated thinking. Preserve meaningful distinction if summaries versus raw text. No screen manipulation, artificial spinner, cursor reposition or destructive carriage return; ordinary print/input/flush and scrollback. Literal model control characters should not be allowed to clear/move terminal; render unsafe controls visibly if necessary while retaining original strings internally. Server may return JSON despite stream:true; display available reasoning/final fields. Handle response.completed, response.failed, response.incomplete, error and premature EOF/[DONE] honestly; no truncated stream declared success, no failed/partial response committed to chat history. Unknown event types safely ignored. Finite request timeout and owned context-managed HTTP resources, no transport retries or background clients. HTTP/nonJSON/malformed stream errors concise, no raw headers/config/token/traceback echo. State unsupported/no-text conditions clearly.
5. Only authoritative completed response.output assistant output_text forms the final delivered answer; never label intermediate unrepaired answer deltas as final. Plain headings Thinking (or summary), Delivered response, Original response. When valid llm_language_repair v1/statuscomplete/original_text exists, use exact returned original and correct output_index/content_index binding; optionally show count/ranges of returned edits in concise format with Unicode-codepoint units. With absent/unavailable/invalid extension simply print `Original response was not provided.` (fixed unavailable reason may be added), never infer original from delivered/no-edit assumptions, diffs, reasoning or logs. Missing original must not prevent delivered answer display. No recursive extraction of unrelated same-named fields. Multi-part/unexpected output must be handled without selecting a reasoning item as answer; supported proxy path has one answer part. No alteration of answer/history strings for presentation except safe terminal rendering.
6. Meaningful tests exercise actual subprocess CLI using temporary Codex files and actual finite owned loopback HTTP. Prove standalone early-beta-style profile and standard table precedence/CLI overrides/relative+tilde catalog, model+thinking slash commands reflected in EXACT outgoing main JSON, header1, storefalse, streamtrue, no proxy settings/history leakage, zero calls for slash commands, /quit. SSE barrier test MUST observe flushed reasoning stdout while server holds final completion, then original+delivered output and success history; no fake copy of client parser. Include JSON fallback, absent/null/unavailable/invalid metadata, Unicode/index identity, done-only reasoning/no duplicate, malformed SSE/error/incomplete/premature EOF no success/history, HTTP/auth/config failure with secret redaction, timeout/cancel cleanup as practical. Do not overbuild combinatorial tests; focused actual-boundary proofs are preferred. Synthetic keys only, no real configs copied into fixtures. Record tested source SHA/hash and truthful red failures/notrun evidence.
7. Concise example guide with exact command examples, synthetic portable TOML/catalog schema, env-key authentication (never realkey), --profile standalone/table behavior, optional --base-url to local installed proxy, /model /thinking /quit session, ordinary terminal sample with thinking/original/delivered and missing-original message. Explain reasoning only if server exposes it, no support inferred from higher effort; original requires deployed proxy+header surviving gateway; unrepaired model may return no extension. It changes main reasoning only; reviewer low/default/settings untouched. Explain text-only in-memory history/no tools/OAuth/automatictruncation/retries, delivered final goes into next turn, metadata/original/reasoning do not. No full Codex compatibility claim. Link from INSTALL repair-details section. Preserve SLAIF docs layout/branding and otherinstallation content. Root reads actual guide and runs ordinary terminal example visually (no screen layout system needed).

## Acceptance criteria

A1: actual config/profile/catalog selection and /model /thinking mainrequest shape proven at real CLI/HTTP. A2: immediate flushed server thinking before held completion in ordinary scrollback, no screen controls/duplicate reasoning. A3: actual delivered+original extension displayed or exact missing message, JSON/SSE, no inventedoriginal and no failedhistory. A4: safe finite request/config/auth errors and resource cleanup, no actual credential read/logging or protected changes. A5: readable runnableexample/docs, focusedtests/staticchecks and existing4finalCI, truthfulSELFpublication/independentreview/developmentmerge/mainprivate reconciliation; no deploy or new product gate.

## Verification

Before implementation `python3.12 examples/repair_chat.py --help` demonstrably absent at acceptedbase, no model call. Focused new actual CLI/HTTP tests; relevant existing Responses/repair-details contract regression only if client request shape requires confirming; Ruff and strict mypy on new Python files where applicable. Do not rerun long full local baseline: actual CI full baseline handles broader checks. Four final publication-head checks all green: Application baseline, Research reproducibility, OAP bootstrap acceptance, OAP report history. CI guard pins to immutable042-a/base must retain exact order digest. Pre-report machine counter mismatch expected until report publication; accurately label it rather than weakening validation. No live model required, no new CodexCLI agent for tests.

## Local setup and constraints

OAP_ROLE=coding explicit every helper and handoff. Existing primary%23 shellPID3338200/Codex3341327 adopted under foreground-only workflow; original argv/home/auth/model/effort unchanged, relaunch only if actually dead using recorded argv. No second agent needed. Private0700/0600; root hands private taskdir path at dispatch. Tests own loopbackport0/tempdir/finitechild PIDs and cleanup, no protected ports/services/network/auth edits. Realprofile was structurally inspected by root; coding does not read it. Do not read/stage ignoredkey.txt or unrelateduntracked files. No new dependencies/cache inrepo; appropriate existing toolenvironment allowed. Owner earlier live/publication allowancespersist but no real model calls needed.

## Documentation

New examples/README.md is user-facing concise how-to with plainterminal sample; INSTALL only short link. No architecture/runtimecontract change. Scoped inventoryhashes after finalsourcebytes. No private roots/credential/raw customer or reviewertext in public artifacts. Synthetic user/client prompts only. No human evaluation dataset/research programme/milestone closure claims.

## Git and report publication

Root publishes immutable042-a and active pointer as two durable writes, activation-only commit on newbranch from exactbase; normal foreground dispatcher consumes ONEcontrol. Primary alone opens onePR/pushesnonreportimplementation. Paired research/RESEARCH-STATE.md counter104to105 for exactly this forthcoming report; do not rewritehistory. Root independently reviews source/tests/negativepaths/docs and PRIVATEdraftreport before exactbyteapproval. Canonical oap/reports/042-a-terminal-comparison-client.md finalsole-pathSELF with literalimplementationparent, push, normal remoteverify --repository, ONEresponseOK thenidle. No canonicalreportbefore root exactapproval, no edits afterSELF without appropriate corrective round. Coding nevermerge/automerge. Root finalheadall4CI/guard/semanticreview then expected-headdevelopmentmerge, verifiesremote/localmain/privateacceptedrefresh. No deployment.

## Decision classification

D0 explicitowner clientexample. Strongest reason against: simplistic Codex-compatible config or streamed thinking handling could mislead users about actual selection/completion/original availability or expose credentials. Contain as documented text-only example, exactprofile precedence, realCLI/HTTPbarriers/negative tests, noauth.json/tools/retries, explicit originalmissing and server-thinking limits, safe terminal rendering and secret-free errors. No new consequential provisionaljudgment/DHAentry or D2action.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "042-a",
  "title": "Plain-terminal Python comparison client using Codex profiles and model catalogs",
  "objective": "042",
  "status": "FINAL",
  "phase": "PYTHON_COMPARISON_EXAMPLE",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "8fc78802c907203c6cf7b8be9fa5609690890e26",
  "branch": "oap/042-terminal-comparison-client",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "041"
  ],
  "local_work": "Preserve all untracked corpus/scratch, ignored key.txt0600, original foreground sessions, branches/worktrees/private history. Tracked acceptedmain clean.",
  "prior_review": "041-a PR47 merged with all4finalheadCI, independent code/docs/source review and verified merge/localmain/private acceptedrefresh at8fc7880. No unfinished041 or open PR observed.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner asks Python quasi harness loading codex TOML/models.json, --profile in ~/.codex, /model /thinking /quit, streamed thinking, original/repaired display with honest missing original; explicitly plain scrollback/no fancy screen manipulation."
    },
    {
      "kind": "A",
      "reference": "Accepted ARCHITECTURE2.4 and INSTALL original-answer extension: caller header1, stateless Responses, llm_language_repair only terminal completion, exact original/finalanswer distinction; no proxy/reviewer policy change."
    },
    {
      "kind": "E",
      "reference": "Read-only real config structural inspection: early-beta.config.toml standalone file, model_catalog_json points to early-beta-models.json with models[].slug/default_reasoning_level/supported_reasoning_levels[].effort; provider Responses/experimental_bearer_token. No credential value printed. Official Responses event/config reference inspected."
    }
  ],
  "decision_class": "D0",
  "lr": [
    "LR-001",
    "LR-003",
    "LR-004",
    "LR-006",
    "LR-010",
    "LR-012",
    "LR-014"
  ],
  "relevant_gates": [],
  "required_checks": [
    "Application baseline",
    "Research reproducibility",
    "OAP bootstrap acceptance",
    "OAP report history"
  ],
  "governance_ref": "8fc78802c907203c6cf7b8be9fa5609690890e26",
  "governance": {
    "AGENTS.md": "118be17b2e9e5b50d628c796b136ee1b8154ebf844af0007839f65848fd69f09",
    "ARCHITECTURE-for-agents.md": "cec7d29a7345d450d1b04543f4d5d94f9a60a056a7bf6179f173c03cee2e352e",
    "ARCHITECTURE.md": "2f1a61d5488fc430a29b4e0e195582c07b95936762d3b86e28eafc1327c26fa1",
    "OAP-COMMUNICATION-coding-agent.md": "313d77900f9bbf1c974e76ea3f546083b117eeaac5f5fc2e964e77b3c19b9ccc",
    "PLAN.md": "d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0",
    "SECURITY.md": "1e3e9e59259ea6a57a3d8e368e896560c214f453c98792f7979e8d99b4154069",
    "TESTING.md": "7ce05ea2bf7e5b411e7a61ece0a0f577bb45bcc9bbd9f40b6480853f24a0ab27",
    "oap/coding-instructions/AGENTS.md": "db624bb41b34d8263bc5745083d86e7db5bb8907816cd8725871e7225eb6ce9d",
    "oap/governance/DISTILLATION-MAP.md": "d600b7f3b189eef055b884d785f5d9207a3acdca1a80a9ee201a3a25e2501a2d",
    "oap/governance/WORKSPACE-LAYOUT.json": "90f8b69f4054dffb4d93313da54841ccfddafdca511b86604765d51b63631d16",
    "oap/prompts/coding-round.md": "c682b1bbe016429bf4712a4174ae5ca4ec8b78ab36ebbc2c9b3ce625d83d3ffd",
    "oap/prompts/ica-start.md": "2311778a8c2f8cbfa24bbc9be1eed30d14289f3d82c2b5e8da70d806b12d70f4",
    "oap/prompts/strategic-start.md": "ba91f90ef1a8692472661900f1f91faacafa1a47954222c0e75e2f9d9752f6ae",
    "oap/strategic-instructions/AGENTS.md": "c08be7ea6f50ff218948e06ca942d40ce370b041f3580035a4f8ae8ef37c8221",
    "oap/strategic-instructions/OAP-COMMUNICATION-strategic.md": "83c87a5277ca798609422927e47caa2babab2cf555824813cbbe7f4ac2531ea5",
    "oap/strategic-instructions/strategic_model_init_material.md": "ac972d8e5fcd58b263df11d044aafed9fb1017d2173157d829fbc2953a1ba3b6"
  }
}
```
