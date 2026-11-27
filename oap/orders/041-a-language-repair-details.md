# Work order 041-a — Opt-in original answer and exact applied-edit metadata

Status: FINAL

## Identity

Objective 041, round 041-a, CREATE_NEW_PR, branch oap/041-language-repair-details. Repository ulfe-lmi/slaif-llm-response-language-repair, default main. Remap inert seed041 (history/adapter concerns already implemented) to this owner-requested additive API feature. One proof-sized PR, same-PR corrective suffixes if necessary. Use ONLY the two existing owner-launched foreground coding panes, original launch homes/auth and recorded argv; no background coding agents, new Codex invocation, codex exec or provider/session relocation.

## Provenance

H: Owner2026-11-27 wants original answer text and begin/end markers of applied corrections returned alongside the repaired answer without confusing clients; accepted the proposed namespaced extension and explicitly selected field llm_language_repair (never slaif_repair), then authorized implementation. The preceding proposal specified caller opt-in, original and repaired code-point coordinates and final-completion SSE metadata. E: accepted RepairEngine already holds immutable original, final edits and repaired answer, but delivered JSON/SSE has no extension; transient trace decisions are not a substitute for authoritative final applied edits. A: accepted ARCHITECTURE2.3 sections5,6,12–15,18 and PLAN1.0 sections1.2/4.1/5.2/11.3–11.4/13.4. This is final response metadata, not a second assistant message or main-history injection. Codex0.172 upstream SSE parser selects standard completed-response fields and ignores this extra property; actual installed-binary/gateway compatibility remains a separate evidence layer.

## Current verified state

Trusted accepted main 0c9c5dc848f82044e7dcace9a0405e88cdcc8f6d, PR46 merged after all four final-head checks and independent actual-source review; local main and backed-up private accepted references reconciled. Active040-b has its remotely verified report; no unfinished040 work or open PR. Tracked clean. Preserve ignored key.txt0600 (coding must not read/stage it), untracked corpus/, .rclone-speed-test/, .research-test-scratch/ and all other ignored/untracked work, all issued history, retained branches/worktrees/private evidence. Existing foreground panes and exact launch argv verified alive; do not relaunch unless dead, then only recorded argv. No backend deployment or protected gateway/service modification.

## Governance

Trusted accepted base above; old current architecture2.3 SHA256 3c7b9a734e2b9773480ea4b9f54ebeb8de36aa2e90c326a696ec3e0b829732a2. Only authorized governance source/projection changes:

- ARCHITECTURE.md
- ARCHITECTURE-for-agents.md
- oap/governance/DISTILLATION-MAP.md
- oap/governance/MANIFEST.json

Advance current architecture2.4 with bounded request-header, final-edit result/coordinates, JSON/SSE metadata, failure/size/privacy contracts and revision record18.6. Preserve all prior revision records and unrelated clauses/LR001–014. Add/distill the new contract and source identities together; necessary GENERATED-FILES hashes/CONTEXT-BUDGET measured counts allowed, existing hard byte caps unchanged. Compact19992/20000: remove redundancy only; retain every unrelated normative condition and stable ID; any ambiguity goes to strategy. CI candidate-review pins may change only to this immutable041-a digest/base; retain all guards. PLAN/CRITICAL/bootstrap seeds, helpers and existing orders/reports are frozen.

## Goal and dependencies

Caller-requested llm_language_repair reports the ORIGINAL final-answer text and the exact FINAL applied edits in both original and delivered coordinate spaces. Normal answer stays repaired text, review behavior/cost unchanged, no extra LLM calls. Preserve early reasoning, exact-span protection, independent acceptance, always-defined-location logprobs removal and every existing failure/cancellation/budget contract. Depends on accepted040 only; no research restart, separate human evaluation dataset, new milestone or deployment gate.

## Scope

Primary owns minimal runnable proxy.py integration, one focused repair_details.py (or equivalently narrow module), final-edit outcome bookkeeping where necessary in the real engine, focused unit/legacy regression tests, bounded operator docs and architecture/projections/CI pins. New test module concept-verification/tests/test_repair_details_integration.py belongs ONLY to secondary in its isolated041 worktree, local commits/private receipt, no runtime/docs/push/PR/active/report/FIFO. Primary may import only after root exact approval. Existing tests may change only for genuinely changed opted-in contracts; absent-opt-in old behavior and assertions must remain. Necessary research/RESEARCH-STATE machine oap_reports_reviewed103to104 paired with this report only.

## Non-goals

No new model/reviewer call, text redetection/rewrite/diff algorithm, parser/Pandoc change, acceptance/evidence relaxation, probability switch, reviewer TOML/default/budget change, new API route or persistent store, logging of extra text, client UI/Codex patch, provider/auth/GPU/network/gateway/deployment changes. No new SDK/runtime dependency. No new Codex agent for testing; installed --version/help read-only inspection is allowed.

## Files and boundaries

Inspect compact coding law/order, real RepairEngine final surviving edits/apply_edits and request-local state; transform_json/transform_sse/transform_sse_blocks; strict and transparent actual handlers including early prefix and buffered tail; header forwarding, Content-Length/cache validators, cancellation/usage/trace drain. Relevant tests: scaled_review_integration, reasoning_streaming, minimal_lifecycle_streaming, responses_contract, delivery_transport, request_cancellation, passthrough, composed_validation, analysis_policy. Bounded full architecture sections5/6/12–15/18 allowed; no mandatory full PLAN/strategic doctrine/research history for coding.

## Requirements

1. Negotiate per request with X-LLM-Language-Repair-Details: 1. Absent or one value0 means off; exactly one trimmed1 means on. For POST /v1/responses, duplicates or any other value reject400 with a fixed content-free REPAIR_DETAILS_HEADER_INVALID before any main/review work (after existing admission/intake precedence). Never reflect value. Consume/remove this private control header case-insensitively from ALL upstream forwarding, including transparent non-Responses routes; those other routes otherwise preserve their existing relay behavior. Never forward to reviewer. Do not add it to the main JSON or change main reasoning/history. No new startup setting required; shipped nine-field reviewer config unchanged.
2. For opted-in supported complete200 plain-answer JSON and supported complete SSE, add ONLY root llm_language_repair (JSON response root; in SSE the response object of terminal response.completed). No new event kinds/messages/deltas. Shape: version:1, status:"complete", offset_unit:"unicode_codepoints", output_index:<actual integer>, content_index:<actual integer>, original_text:<exact original answer part>, edits:[...]. Every edit contains original_start, original_end, repaired_start, repaired_end, replacement. Zero-based inclusive start/exclusive end, original code points versus delivered code points, never bytes/UTF16/modeltokens. Identify the actual existing answer part even after reasoning items. Do not expose reasoning, prompts, corpus evidence or rejected proposals in this object. No correction means exact original_text plus empty edits, not an extra call.
3. Authoritative FINAL applied edits only, after individual acceptance, composed conflict rollback and final patch validation. Preserve or expose the real final immutable original/edit outcome without breaking existing repair(text)->str callers. Do NOT reconstruct via textual diff, substring search, zip with initial candidates or stale trace acceptance flags. Require sorted disjoint valid original spans, exact original slice proof, replacement!=original and exact final composition equals delivered answer. Repaired offsets derive arithmetically from preceding actual length changes; validate delivered slices too. Repeated substrings, emoji, combining marks, CRLF and length-changing edits must map exactly. Candidate/rejected/rolled-back/technical-abort proposals never appear as applied edits. Request-local outcome reset and trace/usage drains must not leak or erase another request's required result.
4. Metadata is optional presentation and must never weaken repair/failure semantics. When exact edit-outcome proof is unavailable, return llm_language_repair={version:1,status:"unavailable",reason:"EDIT_MAPPING_UNAVAILABLE"}; do not fabricate an empty edit list or incorrect coordinates, do not change the successfully decided answer or trigger new review. Cap serialized extension at1,048,576 UTF8bytes with bounded construction (cheap prechecks before duplicating a huge original; at most bounded intermediate allocation); on overflow use the same small unavailable object with reason DETAILS_TOO_LARGE. No partial/truncated original or edit list. Fixed reasons only. Do not enlarge any existing main capture/intake/analysis/review/delivery bound. If otherwise supported opted-in upstream response ALREADY carries root llm_language_repair, reserve that root field and replace its untrusted value with unavailable/UPSTREAM_FIELD_COLLISION, without changing normal answer/review eligibility or unrelated metadata. Default-off preserves unknown upstream fields byte/semantically as before. Never recursively remove same-named user data.
5. Unsupported/non-completed/non200/malformed/structured/stored/over-hard-capture fallback paths retain existing behavior and add no repair-details success object. The original final answer must be fully/safely captured before details; never emit original as a provisional answer. In early-reasoning streaming, proven prefix frames remain ordinal-identical and arrive before final capture/review; extension exists only in actual terminal completion, once. Preserve all answer copies/delta/done/item consistency, annotations/citations/usage/IDs/status/errors, defined-logprobs removal even with no edits/off/unavailable, Content-Length and transformed-body cache-validator handling. Cancellation after headers remains quiet with no synthetic terminal/[DONE]/metadata. Metadata generation/failure has no effect on actual review attempt or usage accounting; private traces remain opt-in and no new text logging/storage occurs.
6. Primary focused units cover header parsing, strict builder/schema, exact mapping proof/rejection and finite-byte fallback. Secondary real HTTP integration (both handler modes and JSON/SSE) uses actual engine/CPU tiny corpus/Pandoc/patcher and fake main/reviewer OUTSIDE those boundaries. Cover off/0/on, invalid+duplicate headers zero-main, header stripped from main and arbitrary-route relay, single+multiple length-changing edits, repeated occurrences/Unicode/CRLF, actual output-part identity, no-suspicion0calls, reviewer-keep/reject, batch failure original+emptyeds, composed rollback surviving edit set, request-local reset. Negative metadata proof/size/collision cases must prove original answer/other metadata/logprobs policy remain correct. Reuse existing failure/protection/math tests. Actual streaming barrier must demonstrate early reasoning precedes held final completion and details only at terminal; cancelled/unsupported/malformed/incomplete/capture-limit cases do not invent completion. Ordinary old clients receive exactly prior behavior when opt-in absent.
7. Codex compatibility: source-bound inspection at official openai/codex rust-v0.172.0 codex-rs/codex-api/src/sse/responses.rs (ResponsesStreamEvent/ResponseCompleted/process_responses_event), plus new wire-contract tests preserving all standard fields and actual lifecycle. Explicitly distinguish actual product HTTP proof, actual pinned-source parser proof and installed-binary/gateway E2E. No Python mirror may be called actual Codex evidence. Do not launch codex exec/new agent/new session/provider override for this. If actual installed-client qualification cannot be done within the existing foreground-only workflow, record NOT RUN with exact limitation; retain source-backed compatibility expectation, no universal-client or binary-tested claim. Root may perform bounded independent parser/CLI investigation without changing the product or gateway; no new runtime dependency or CI toolchain gate.
8. Update INSTALL (exact header/curl and field schema, coordinates/browserUTF16 warning, onlyfinaledits, size/unavailable/collision/unsupported boundaries, payload duplication, client/gateway may discard fields, no new logging), concise README/QUICKSTART pointers and docs/PROXY-CHAINING for control-header handling. Reflect opt-in metadata on existing architecture lifecycle diagram without putting original in main history. Preserve SLAIF branding/ack/support and documentation roles. Root visually reviews changed user docs/diagram at desktop/mobile. Full architecture2.4, compact/map/manifest/current source identities update together; existing20k cap retained. No standalone documentation rewrite or milestone claims.

## Acceptance criteria

A1: opt-in only, private control header consumed and negative ingress before upstream, normal clients unchanged. A2: exact original/delivered spans from final applied edits with actual positive multi-edit proof and no rejected edit leakage. A3: complete JSON+SSE/early-prefix integration, unchanged surrounding metadata/protection/failure/cancellation/usage. A4: bounded honest unavailable/size/collision paths. A5: meaningful source-bound Codex compatibility and explicitly qualified missing E2E. A6: operator docs, visually checked diagrams, coherent accepted-base governance. A7: truthful immutable SELF report, exact final-head four CI and independent guarded development merge/main/private refresh; no deployment.

## Verification

Primary focused units and affected regression modules; secondary test_repair_details_integration.py with real named boundaries. Preserve full private logs/source hashes/tested SHA. Before implementation, use new focused positive case to show accepted-base missing extension at the actual HTTP response boundary, not fixture/import failure. Once focused+appropriate broader tests pass, do not repeat long local suites without new failure/change. Required final-head CI: Application baseline, Research reproducibility, OAP bootstrap acceptance, OAP report history (all green, pending/skipped/cancelled not passed). Existing CI driver/package/offline/nativeOAP, candidate-review/order hash, whitespace/protected-source/transcript/history checks retained. No full baseline driver from local workspace with ignored key/untracked corpus; use clean tracked-only archive if needed or actual CI. No real Qwen calls necessary for presentation-only behavior; synthetic integrations are functional evidence, not linguistic evaluation.

## Local setup and constraints

Explicit OAP_ROLE=coding in helper environment and handoff. Adopt only existing foreground panes, recorded codex --yolo --profile early-beta argv, original auth homes. Primary REPO_ROOT; secondary strategy-created isolated041 worktree with explicit tool cwd. Private0700/0600, umask077, qualified Python3.12 and exact installed Pandoc3.7.0.2; no historical executable searching. Finite owned loopback0 test servers/processes only, exact PID/start cleanup, no process-pattern kill. Preserve all unrelated data/history. Existing owner RTX/publication allowances persist but this feature needs zero real model calls. No separate human evaluation dataset.

## Documentation

Requirements8 define bounded operator changes. Keep implementation details out of ordinary reader introduction, document exact units/limits and current client evidence honestly. No credentials, private host paths, raw customer/review prompts/responses in public artifacts; only safe synthetic examples. Update projected hashes/counts after final source bytes; do not rehash frozen audit seeds or change hard caps.

## Git and report publication

Strategy publishes immutable041-a then active pointer as separate durable writes; activation-only commit on new branch from literal accepted base. Primary alone receives ONE control through verified foreground dispatcher; secondary only explicit scoped handoff after publication, no second FIFO consumption. Primary creates one PR, handles implementation/docs/bookkeeping and exact approved secondary import, pushes all non-report work, then PRIVATE exact final report draft/root approval. Canonical oap/reports/041-a-language-repair-details.md: sole-path SELF commit, literal actual implementation parent, push, normal remote verification with --repository, ONE response OK then quiesce. The current machine counter is103; new104 anticipates this report (before publication the expected count remains103). Record pre-report mismatch honestly; final-head check must resolve it. Further correction after SELF requires new same-PR suffix; no history rewrite or invented PR/SHA. Coding never merges or enables auto-merge. Strategy independently reviews final diff/source/evidence/CI/effects/privacy/rollback, merges with expected reviewed head and verifies remote/local main. Accepted private governance refresh only after actual mergedmain, backed up; no deployment.

## Decision classification

D0 under explicit owner additive-metadata request. Strongest counterargument: extra fields can trigger strict clients, expose a larger original payload, or publish misleading offsets after rollback; duplicated payload can increase memory/delivery work. Mitigate caller opt-in, final-only namespaced object, actual final-edit ledger with exact composition checks,1MiB extension cap/unavailable result, existing budgets/quiet failure preserved, no storage, source-qualified client compatibility and actual HTTP regressions. Collision policy is a contained reserved-field rule, not a license to alter unrelated metadata. No unresolved consequential judgment requiring CRIT.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "041-a",
  "title": "Opt-in original answer and exact applied-edit metadata",
  "objective": "041",
  "status": "FINAL",
  "phase": "ORIGINAL_ANSWER_AND_APPLIED_EDIT_DETAILS",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "0c9c5dc848f82044e7dcace9a0405e88cdcc8f6d",
  "branch": "oap/041-language-repair-details",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "040"
  ],
  "local_work": "Preserve ignored key.txt, untracked corpus/scratch and all histories/worktrees/private evidence; tracked accepted main clean.",
  "prior_review": "040-a/b PR46 merged after all4finalheadCI and independent review; remote/local/main/private acceptedrefs reconciled at0c9c5dc. No openPR or unfinished040.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner2026-11-27 requests original answer and changed begin/end markers; approves opt-in extension proposal, selects llm_language_repair and says go ahead."
    },
    {
      "kind": "A",
      "reference": "Accepted ARCHITECTURE2.3 sections5/6/12\u201315/18 and immutable PLAN1.0 sections1.2/4.1/5.2/11.3\u201311.4/13.4; final metadata only, no main-history message."
    },
    {
      "kind": "E",
      "reference": "Accepted engine keeps immutable original/final edits internally; actual JSON/SSE transforms do not expose original/coordinates. Installed Codex0.172 version and matching official parser inspected, E2E untested."
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
  "governance_ref": "0c9c5dc848f82044e7dcace9a0405e88cdcc8f6d",
  "governance": {
    "AGENTS.md": "118be17b2e9e5b50d628c796b136ee1b8154ebf844af0007839f65848fd69f09",
    "ARCHITECTURE-for-agents.md": "1e8bb039022980ca51ef930a7fafeecfeb095061efb8076fafab443039018f52",
    "ARCHITECTURE.md": "3c7b9a734e2b9773480ea4b9f54ebeb8de36aa2e90c326a696ec3e0b829732a2",
    "OAP-COMMUNICATION-coding-agent.md": "313d77900f9bbf1c974e76ea3f546083b117eeaac5f5fc2e964e77b3c19b9ccc",
    "PLAN.md": "d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0",
    "SECURITY.md": "1e3e9e59259ea6a57a3d8e368e896560c214f453c98792f7979e8d99b4154069",
    "TESTING.md": "7ce05ea2bf7e5b411e7a61ece0a0f577bb45bcc9bbd9f40b6480853f24a0ab27",
    "oap/coding-instructions/AGENTS.md": "db624bb41b34d8263bc5745083d86e7db5bb8907816cd8725871e7225eb6ce9d",
    "oap/governance/DISTILLATION-MAP.md": "8b53ab6a19bdfe627d29ef31835a1dff6326f85d0870d1ddf1eda35164730cdc",
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
    "source": "Explicit owner2026-11-27 additive final-response original/markers extension llm_language_repair with caller opt-in as proposed.",
    "accepted_ref": "0c9c5dc848f82044e7dcace9a0405e88cdcc8f6d",
    "old_sha256": "3c7b9a734e2b9773480ea4b9f54ebeb8de36aa2e90c326a696ec3e0b829732a2",
    "scope": "Current architecture2.4: opt-in header and JSON/terminalSSE details, exact final original/repaired coordinates, bounded unavailable/size/collision/privacy contract; no repair/parser/protection/budget/metadata policy relaxation."
  }
}
```
