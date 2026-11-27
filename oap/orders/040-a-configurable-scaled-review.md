# Work order 040-a — Configurable length-scaled isolated review

Status: FINAL

## Identity

Objective040, round040-a, CREATE_NEW_PR on branch oap/040-configurable-scaled-review. Repository ulfe-lmi/slaif-llm-response-language-repair, default main. Remap inert seed040 (main-history/isolation already implemented and must be preserved) to the owner's current review-policy/configuration change. One coherent PR. Only the two existing owner-started foreground coding panes; no background coding agents or new models.

## Provenance

H: Owner2026-11-26 requests default one review call per1024 answer characters so longer answers permit more corrections, operator config for thinking/all displayed review parameters, and explicitly selects low as default thinking. E: accepted source still hardcodes detector maximum4/hard cap8, one batch, boolean cancellation-dispatch accounting and last-result usage; reviewer body has no reasoning effort. A: accepted ARCHITECTURE2.2 sections4/9/10/11/12/14/18, all LR001–014 and immutable PLAN1.0 sections1.2/3/4/5/13. Initial numeric defaults are superseded by the explicit owner decision; PLAN bytes remain protected. [Responses API primary reference](https://developers.openai.com/api/reference/resources/responses/methods/create) documents reasoning.effort; actual provider acceptance is a separate bounded qualification, not inferred from OpenAI model defaults.

## Current verified state

Trusted accepted main d39361c4b8f93981436da76a4a77d8cf6b48396c, PR45 merged;039-a remotely verified with its sole-path SELF publication50a0bc7. No open PRs. Tracked clean. Preserve ignored key.txt (coding must never read/stage it), untracked corpus/, .rclone-speed-test/, .research-test-scratch/, all historical/private worktrees and evidence. Both owner foreground Codex sessions alive with exact recorded launch argv; no relaunch unless dead, then exactly recorded argv. No backend deployment or protected service change.

## Governance

Compare against the exact trusted accepted base above and old ARCHITECTURE2.2 SHA2566a93315cdbc24cee9124b7c187c4757f5531f3ee65be453d0db0471f511a35db. Authorize only these governance source/projection paths:

- ARCHITECTURE.md
- ARCHITECTURE-for-agents.md
- oap/governance/DISTILLATION-MAP.md
- oap/governance/MANIFEST.json

Revise current architecture to2.3 for this explicit policy: relevant startup/component, selection, review, accounting/budgets/failure clauses and new section18.5 revision record. Preserve previous revision records/PLAN/CRITICAL/bootstrap seeds and published order/report bytes. Synchronously update affected compact clauses/coverage/identities and necessary GENERATED-FILES hashes/CONTEXT-BUDGET measured counts; do not change cap law. Compact at20000-byte bound: compression may remove redundancy/formatting only, preserving every unrelated condition; strategy must independently review. CI candidate-review pins may change only to this immutable040-a digest/base, retaining all guards.

## Goal and dependencies

After complete main capture, one CPU pass selects a length-scaled number of exact original occurrences, reviews needed batches sequentially with explicit configured low effort, independently validates proposals, composes once and delivers one final answer. Longer eligible answers can actually repair more than four sites. Depends on accepted039; no research restart, human evaluation dataset or new milestone gate.

## Scope

Primary: runnable concept-verification/proxy.py, batch_review.py, qwen_client.py, runtime_detection.py, config.py, request_lifecycle.py and review_transport.py as tightly necessary; one focused typed policy/loader module and adjacent shipped repair-proxy.toml; existing meaningful tests plus new policy-unit test file; bounded README/INSTALL/QUICKSTART/docs/PROXY-CHAINING configuration instructions and architecture/projections/CI pins. Only necessary research/RESEARCH-STATE machine oap_reports_reviewed101to102 update paired with this report. Secondary independently owns ONLY new concept-verification/tests/test_scaled_review_integration.py in its isolated040 worktree, local commits/private receipt, no runtime/docs/push/PR/active/report/FIFO. Strategy approves exact test import and publication report bytes.

## Non-goals

No rewriting whole answers, iterative correction passes, retries, parallel model reviews, main-request reasoning changes, relaxed corpus acceptance, new corpus/downloads, parser changes, probability-policy switches, provider/model/profile/auth-home changes, production host/GPU/network/service edits or deployment. Keep existing main capture/intake/admission/delivery/Pandoc/evidence budgets unchanged. No generic configuration framework or new dependencies.

## Files and boundaries

Inspect compact coding law/order; actual RepairEngine detect/prepare/review/accept/composed patch path, actual CLI startup before bind, batch prompt/request/envelope parser, review_transport owned worker dispatch, CancellationScope counter and success/abort usage observer; existing tests for these boundaries. Bounded full architecture sections4/9/10/14/18 allowed for source update; no mandatory full PLAN/strategic law/research history for coding. Frozen detection.py and research programme remain unchanged.

## Requirements

1. Default review-call ALLOWANCE B=ceil(N/1024), where N=len(immutable completed eligible answer text) in original Unicode code points, not UTF8 bytes, model tokens, envelope/metadata/reasoning or edited length. N=0 permits0;1..1024 permits1;1025..2048 permits2. These are maxima: no eligible suspicion means0 and never issue empty/filler requests. Existing optional-analysis gate32768cp/131072UTF8 remains: over-limit original still bypasses entirely. Thus existing default eligibility bounds default B at32; do not introduce a hidden eight-target cap or arbitrary lower call ceiling.
2. Scale actual CPU-selected nonoverlapping occurrences to at most B*configured targets_per_call (default4). Preserve detector scores, original ranking/tie-breaking, lexical/grapheme/protected gates, one original evidence pass and final original-order output. Remove only the obsolete runnable-path cap that would silently keep coverage at4/8; preserve frozen/historical detector contracts. Select/prepare all occurrences against the same immutable original before dispatch, with unique response-local occurrence IDs across batches and exact explicit batch-to-original mapping; repeated words cannot use substring relocation. Batch each occurrence at most once, no re-detection on edited text.
3. Add adjacent shipped concept-verification/repair-proxy.toml and typed immutable per-instance review policy. Canonical operator selection CONCEPT_CONFIG: explicit supplied path; if absent use ONLY the deterministic shipped adjacent file, never historical/directory searching. No hot reload. Missing/unreadable/malformed selected/default file, unknown sections/keys, wrong types (including bool-as-int), zero/negative/nonfinite numeric bounds and unknown effort names fail clearly before any listener/model/corpus work, with no secret/value echo. Use Python3.12 tomllib, no new dependency. File values flow to real runtime, not just parsed/echoed. Existing CONCEPT_REVIEWER_TIMEOUT remains the sole legacy timeout override, precedence environment override > selected/shipped file; validate it equally before bind. Keep credentials/profiles outside TOML and existing endpoint/model/auth environment behavior unchanged.
4. Required [review] defaults: characters_per_call=1024; targets_per_call=4; context_codepoints=240; max_prompt_codepoints=8192; max_output_tokens=1536; max_request_bytes=32768; max_response_bytes=32768; timeout_seconds=15.0; reasoning_effort="low". All numeric settings finite strictly positive (exact ints for integer fields), timeout finite positive int/float excluding bool. Support explicit standard reasoning effort names with documented backend dependence; default wire field exactly reasoning:{effort:"low"}. Never silently downgrade/omit requested effort after rejection. Preserve stream:false/store:false/no tools or main-history/session links. Config affects reviewer requests ONLY; incoming main reasoning settings remain identical. Keep schema batch-v1 and fixed exact-span/proposal/CPU acceptance safety constraints, including existing128cp target/replacement and8 replacement words, outside this operator change.
5. Each batch is a fresh isolated request, at most one in flight, independent per-batch configured absolute whole-exchange timeout and byte/token/prompt bounds. No retry, no extra pass or automatic refinement. On any batch preparation/transport/structural/incomplete-response failure, STOP later batches, discard uncommitted proposals and return completed original answer TEXT (defined logprobs removal still applies). Individual structurally valid rejected proposals retain existing independent behavior. Only after successful review phase run existing independent CPU acceptance and combined conflict/mapping checks and compose once. No whole-answer or AST render regeneration. One invalid final mapping/evidence exhaustion retains existing all-original failure semantics.
6. Actual attempted-call ledger must count0..B accurately at the real owned dispatch boundary, including mid-second-call cancellation/transport failure. Replace/extend the old boolean-only state without breaking existing single-call interfaces unnecessarily. Aggregate actual reviewer usage across all attempted calls ONLY when every attempted result supplies validated usage; if any attempted usage missing/invalid/failed, aggregate is unknown, never partial-as-total/zero. A failure before another dispatch adds no fake attempt/token cost. Preserve completed known usage through later CPU cancellation, clear all request-local state between requests, emit exactly one content-free success-or-abort observation and never main/review usage conflation. User cancellation halts later dispatch, kills/reaps current owned child, closes corpus/cursor/guards and keeps existing quiet-abort semantics. No content logging.
7. Preserve mandatory pinned Pandoc/startup/exact original span proof, protected content, Unicode/formatting, strict independent corpus evidence, per-query/cumulative evidence budgets, all JSON/SSE envelope/usage/citations/annotations/IDs/status behavior and unconditional defined-location logprobs removal. Eligible early reasoning continues while final answer waits for capture+all optional work; strict SSE remains buffered. Test cross-batch interacting edits through the actual combined validator; reject/revert conflicts without accepting unapproved slices. No changes to protected GPU/weights/vLLM/llama.cpp/gateway/network/auth.
8. Primary implements/tests/config/docs; secondary independently authors actual HTTP/startup regressions using real handler/loader/transport/CPU/Pandoc and fake upstreams OUTSIDE those boundaries. Test defaults and nondefaults reaching wire; Unicode lengths1023/1024/1025/2048/2049, clean zero calls, long answer >4 eligible occurrences actually reviewed/repaired across >=2 batches, unique IDs/repeated occurrences/exact spans, no unapproved text or main settings change, batch2 bad JSON/incomplete/timeout -> original+no later calls, cancellation before/between/during calls with exact count/known-unknown usage and no leak into next request. Validate invalid config before bind/no model work, missing file/unknown keys and legacy timeout precedence. Reuse old protection/math/Unicode/patch/cancellation/logprobs suites; never weaken old safety assertions or fake the named boundary. Record old baseline failure of focused new positive cases at the actual missing/scaled behavior, not an unrelated fixture/import error. Root independently checks actual module/source bindings after import.
9. Update current architecture2.3 against trusted old hash/base, exact changed clauses, owner rationale/D0 and projections. Explain one frozen-original pass with multiple independent batches, proportional allowance vs actual calls, remaining analysis cutoff, default worst-case reviewer waiting sum(B*timeout) and unchanged one-active admission, usage aggregation and all-original batch-failure semantics. Document config file discovery, every setting/unit/default, thinking wire/default/provider dependence, secrets, env precedence, startup failures/restart and short/long examples in INSTALL; QUICKSTART concise supported path, README summary/pointer. Preserve SLAIF branding/ack/support and human-friendly document roles. Root visually checks changed user docs and architecture at desktop/mobile; diagrams must reflect bounded sequential batches if labels changed. No deployment claims.
10. Strategy alone may run bounded synthetic live qualification against already-authorized RTX3090 Responses endpoint, initially up to12 requests/concurrency1 within owner's existing1M allowance (routine extension inside that authorization requires no new human permission). Confirm actual request low effort acceptance/response shape; separate accepted wire compatibility from internal provider effort interpretation and linguistic quality. Coding cannot access key.txt/SSH/provider auth. No live service/model changes, no raw model text/credentials in versioned artifacts. Before canonical report, provide PRE_REPORT actual PR/head/diff/test identities/results and private draft. Root must approve exact bytes; use schema governance=issued order map, observed_source_governance=current, checks.sha existing ancestor40hex with clear working-byte equivalence, NOT RUN with space, no future push/CI claims.

## Acceptance criteria

A1 actual default budgetceil(N/1024),0clean and >4-site multi-batch correction through real handler. A2 typed operator TOML controls all nine specified parameters; low is explicit reviewer-only wire default; malformed config fails pre-bind. A3 unique immutable span mapping and one combined validation/composition, no retry or unsafe structures. A4 actual dispatch/usage/cancellation counts across batches and fresh-request reset, unknown never partial/zero. A5 earlier protections/streaming/envelopes/metadata/failure/resource boundaries unchanged and meaningful negative integration evidence. A6 truthful config/docs/projection/visual and bounded live compatibility evidence. A7 approved immutable SELF publication, all four final-head CI and independent expected-head development merge/main verification; deployment separate.

## Verification

Focused config loader/unit/request builder/selection tests and independent new test_scaled_review_integration.py; existing test_review_batch, test_request_cancellation, test_review_transport, test_analysis_policy, test_evidence_budget, test_composed_validation, test_pandoc_protection, test_responses_contract, test_reasoning_streaming, test_minimal_lifecycle_streaming and relevant passthrough/metadata limits. Capture complete private logs with explicit CONCEPT_PANDOC=/usr/bin/pandoc and qualified Python3.12, strict private modes. Do not repeat entire long local suites once focused+broader proof passes without new failure/change. Full development baseline via existing final-head CI driver is mandatory; avoid local full driver from dirty/untracked workspace. Required checks Application baseline, Research reproducibility, OAP bootstrap acceptance, OAP report history all green on actual final head. Exact candidate-review order digest/base, whitespace/transcript/history/protected-source guards retained. Not a linguistic benchmark or new milestone ICA/dataset gate.

## Local setup and constraints

OAP_ROLE=coding explicitly in handoff/helper env. Only existing owner foreground panes, recorded argv and original auth homes; no background coders, codex exec, new providers/models, auth/profile relocation or auto-restart. Primary REPO_ROOT, secondary strategy-created040 worktree. Private receipts/logs0700/0600, umask077. Existing qualified Python3.12 and Pandoc3.7.0.2 only. No real keys/customer text/public raw payloads; no backend rollout. Preserve all unrelated dirty/untracked/history/evidence. Numeric policy updates are owner-authorized D0; do not invent a D1 or human permission gate.

## Documentation

Professionally arranged operator-facing settings/example file with explicit units/defaults and bounded semantics, links from README/QUICKSTART to INSTALL, architecture diagrams/clauses and compact/manifest consistency in the same PR. No private exact paths outside authorized layout, keys or raw native model output. No human evaluation dataset or unrelated research programme.

## Git and report publication

Strategy publishes immutable040-a then active pointer, commits only activation on the new branch from exact accepted base, consumes/signals one control through the foreground dispatcher for primary only. Secondary local test commits only; root approves exact import. Primary one PR, complete scoped implementation/docs/bookkeeping, then PRE_REPORT exact bytes/root approval. Canonical oap/reports/040-a-configurable-scaled-review.md: sole-path SELF commit with literal actual implementation parent, push, remote verify exact bytes/head/parent, ONE responseOK then quiescent. Preserve all issued history. Report counter102 anticipates this report; pre-report count mismatch is not a runtime bug, record accurately and verify final-head resolution. After immutable report, further changes require bounded same-PR suffix; coding never merges/enables auto-merge. Root independently reviews exact final head/all checks/effects/rollback and uses expected-SHA guarded development merge; no deployment.

## Decision classification

D0 under explicit owner product/resource/config decisions. Strongest argument against: more calls/candidates can increase latency/GPU cost, interaction risk and accounting mistakes; configurable limits can drift from actual enforcement. Mitigation: finite original-analysis bound and derived allowance, sequential fresh batches/no retry, actual-wire nondefault tests, exact original positions/one composed validation, all-original optional-phase failure, authoritative per-request dispatch/usage ledger and cancellation tests, strict startup policy. Acceptance evidence, safety gates, model services and human release authority unchanged. No unresolved consequential dilemma requiring a CRIT entry.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "040-a",
  "title": "Configurable length-scaled isolated review with low reasoning",
  "objective": "040",
  "status": "FINAL",
  "phase": "CONFIGURABLE_LENGTH_SCALED_REVIEW",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "d39361c4b8f93981436da76a4a77d8cf6b48396c",
  "branch": "oap/040-configurable-scaled-review",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "039"
  ],
  "local_work": "Preserve ignored key.txt, untracked corpus/ and both scratch directories, all prior orders/reports/worktrees/private evidence; tracked accepted main clean.",
  "prior_review": "039-a/PR45 merged, final-head checks passed, local/private accepted sources reconciled at d39361c. No open PRs or unfinished implementation.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner 2026-11-26: default one reviewer call per1024 answer characters, all displayed review parameters and thinking in operator config file; explicit default thinking low. Existing foreground-only workflow and prior publication/RTX3090 authorizations retained."
    },
    {
      "kind": "E",
      "reference": "Accepted runnable source has detect(maximum=4), runtime detection hard cap8, one batch/cancellation boolean dispatch and last-result usage, fixed batch limits, and no reasoning.effort in reviewer request. Merely changing a number would not implement the owner decision."
    },
    {
      "kind": "A",
      "reference": "Accepted ARCHITECTURE2.2 sections4,9,10,11,12,14,18; immutable PLAN1.0 sections1.2/3/4/5/13: replace initial numerical one-call proposal under explicit new owner instruction while retaining one frozen-original pass, isolation, strict CPU evidence and failure safety."
    }
  ],
  "decision_class": "D0",
  "lr": [
    "LR-001",
    "LR-002",
    "LR-003",
    "LR-004",
    "LR-005",
    "LR-006",
    "LR-007",
    "LR-008",
    "LR-009",
    "LR-010",
    "LR-011",
    "LR-012",
    "LR-013",
    "LR-014"
  ],
  "relevant_gates": [],
  "required_checks": [
    "Application baseline",
    "Research reproducibility",
    "OAP bootstrap acceptance",
    "OAP report history"
  ],
  "governance_ref": "d39361c4b8f93981436da76a4a77d8cf6b48396c",
  "governance": {
    "AGENTS.md": "118be17b2e9e5b50d628c796b136ee1b8154ebf844af0007839f65848fd69f09",
    "ARCHITECTURE-for-agents.md": "e52d30b391c1a84b96c706c09803e06b81b5a140755e851b6c59db13bd001a17",
    "ARCHITECTURE.md": "6a93315cdbc24cee9124b7c187c4757f5531f3ee65be453d0db0471f511a35db",
    "OAP-COMMUNICATION-coding-agent.md": "313d77900f9bbf1c974e76ea3f546083b117eeaac5f5fc2e964e77b3c19b9ccc",
    "PLAN.md": "d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0",
    "SECURITY.md": "1e3e9e59259ea6a57a3d8e368e896560c214f453c98792f7979e8d99b4154069",
    "TESTING.md": "7ce05ea2bf7e5b411e7a61ece0a0f577bb45bcc9bbd9f40b6480853f24a0ab27",
    "oap/coding-instructions/AGENTS.md": "db624bb41b34d8263bc5745083d86e7db5bb8907816cd8725871e7225eb6ce9d",
    "oap/governance/DISTILLATION-MAP.md": "aaa945c445640937a231eb8e236f60e0085a13fe4f9a0afcabda3c052ad5b927",
    "oap/governance/WORKSPACE-LAYOUT.json": "90f8b69f4054dffb4d93313da54841ccfddafdca511b86604765d51b63631d16",
    "oap/prompts/coding-round.md": "c682b1bbe016429bf4712a4174ae5ca4ec8b78ab36ebbc2c9b3ce625d83d3ffd",
    "oap/prompts/ica-start.md": "2311778a8c2f8cbfa24bbc9be1eed30d14289f3d82c2b5e8da70d806b12d70f4",
    "oap/prompts/strategic-start.md": "ba91f90ef1a8692472661900f1f91faacafa1a47954222c0e75e2f9d9752f6ae",
    "oap/strategic-instructions/AGENTS.md": "c08be7ea6f50ff218948e06ca942d40ce370b041f3580035a4f8ae8ef37c8221",
    "oap/strategic-instructions/OAP-COMMUNICATION-strategic.md": "83c87a5277ca798609422927e47caa2babab2cf555824813cbbe7f4ac2531ea5",
    "oap/strategic-instructions/strategic_model_init_material.md": "ac972d8e5fcd58b263df11d044aafed9fb1017d2173157d829fbc2953a1ba3b6"
  },
  "architecture_revision": {
    "authorized": true,
    "source": "Explicit owner2026-11-26 authorizes length-scaled review budget, operator TOML parameters and low reasoning default.",
    "accepted_ref": "d39361c4b8f93981436da76a4a77d8cf6b48396c",
    "old_sha256": "6a93315cdbc24cee9124b7c187c4757f5531f3ee65be453d0db0471f511a35db",
    "scope": "ARCHITECTURE2.3 bounded configurable review policy/multi-batch accounting; unchanged exact-original spans, mandatory Pandoc, strict evidence/one composed pass, history/envelope/failure and protected resources."
  }
}
```
