# Work order 039-a — Stream reasoning after minimal lifecycle snapshots

Status: FINAL

## Identity

Objective039, round039-a, CREATE_NEW_PR; branch oap/039-minimal-lifecycle-streaming. Repository ulfe-lmi/slaif-llm-response-language-repair, default main. Remap inert seed039 (adapter integration already implemented) to the owner-requested V100 reasoning-streaming compatibility correction. One branch, PR and canonical report. Use only the two existing owner-started foreground coding panes.

## Provenance

H: Owner 2026-11-24 requests fixing our repair proxy, publishing, testing and merging its PR; clarifies A100 uses vLLM and V100 uses llama.cpp. E: strategy's authorized deployed-backend diagnosis is privately preserved as backend-diagnosis.json in the preceding latency diagnostic task. Actual upstream emits response.created and response.in_progress with response keys id/object/status, status in_progress, and no output field. Current classifier rejects the first frame as lifecycle snapshot unproven; proxy.py latches its irreversible buffer boundary. Upstream reasoning arrived at 0.436 s; a 128-token repaired-port probe released first reasoning at 3.5169 s and completion at 3.5187 s. Native Codex showed eleven minutes of silence followed by a burst; the unrepaired V100 control streamed after about12 s. These are transport/shape observations, not linguistic-quality evidence. A: ARCHITECTURE2.1 sections3/5.3/6/13/14 and LR001/007/011/013/014.

## Current verified state

Accepted main is 6f7ae1aa662e103ddecd86375579e380c16468ea, merged PR44. Prior038-a report independently remotely reverified at64cbe1df46f3204726bad309f1fc426978d7c068; no open PRs. Local tracked clean. Preserve ignored key.txt and untracked corpus/, .rclone-speed-test/, .research-test-scratch/, all prior orders/reports/private worktrees and credential/diagnostic files. Deployed backend is already on accepted main, exact proxy/helper hashes verified, transparent mode, unchanged running process. No backend update is part of this order.

## Governance

Trusted governance_ref is the accepted base above. Authorize only the bounded current architecture description of the new proven minimal lifecycle variant and synchronized projections, comparing old ARCHITECTURE2.1 hash2fb71ce6b210d1c1cc26f44d81f8e80b34d9be1ec0569e68b44e0748ed81f63c against that trusted base. Preserve historical revision records, PLAN/CRITICAL/bootstrap seeds and all published orders/reports. Exact permitted governance source paths:

- ARCHITECTURE.md
- ARCHITECTURE-for-agents.md
- oap/governance/DISTILLATION-MAP.md
- oap/governance/MANIFEST.json

Update necessary GENERATED-FILES identities and CONTEXT-BUDGET measured counts without changing their budget law. Compact architecture is near its existing byte cap: any trimming must remove only redundancy/formatting, retain every condition, and receive strategic semantic review. Candidate-review CI pins may be updated only to this exact immutable039-a order digest and trusted accepted base; preserve all guards.

## Goal and dependencies

Allow reasoning to stream after the observed answer-free llama.cpp minimal lifecycle start frames, while preserving the completed-answer repair contract and the conservative one-way boundary. Depends on accepted038; this is scoped product compatibility, no research restart, separate human dataset, milestone gate or backend rollout.

## Scope

Runtime: concept-verification/sse_streaming.py only unless strategy approves a concrete tightly necessary proxy.py integration correction. Primary owns helper unit tests in existing test_sse_prefix_streaming.py, narrow docs/governance/CI, commits/PR/report. Secondary independently owns a NEW test_minimal_lifecycle_streaming.py in its isolated039 worktree. Add only necessary report-count bookkeeping in research/RESEARCH-STATE.md. Exact order/active and report paths are included. Do not edit unrelated tests or source law to make failures disappear.

## Non-goals

No provider/model switch or auto-detection; no caller flag/configuration, general missing-output relaxation, answer streaming, gateway or model changes, endpoint/profile/auth changes, research/data/installation work, service restart, deployment or release.

## Files and boundaries

Compact read set first, exact order, helper _lifecycle_denial_reason/classify_prefix_frame/_carrier_denial, actual proxy emit_frame one-way latch, existing helper tests and real HTTP harnesses in test_reasoning_streaming.py/test_passthrough.py. Bounded full architecture section5.3 and section18 are authorized for source revision. Do not require coding to load full PLAN, strategic constitution or historic sources. Private diagnosis exposes only structural fields/timing, never keys or raw model text.

## Requirements

1. Add an explicitly proven MINIMAL variant only for top-level response.created and response.in_progress events. Its event keys are exactly type/response/sequence_number; sequence_number is an actual nonnegative integer (not bool). Its response keys are exactly id/object/status; id is a nonempty string, object is exactly response, status exactly in_progress. output is ABSENT, not null. Existing strict parsing/event-label agreement still applies. Do not infer this shape from a provider/model name.
2. Scope that variant to the direct lifecycle classification branch. Keep the existing general full-snapshot validator and reasoning embedded-carrier checks conservative; no global rule that absent output means empty. Existing supported full vLLM-style snapshots and reasoning families retain behavior. Additional keys, wrong/missing types, null, unknown or terminal statuses, other lifecycle event types, answer-bearing carriers, malformed JSON, duplicate keys and label mismatch remain boundaries. The minimal path cannot carry answer text because its complete allowed structure excludes it.
3. Preserve the irreversible boundary after any real unknown/message/tool/terminal event. Full bounded immutable capture precedes review and answer delivery, sent prefix is never replayed/reordered, exact approved answer edits remain coherent. No empty-output synthesis into received events, no answer regeneration, no modification to Pandoc, protected spans, detector/acceptance rules or reviewer prompts. Preserve all defined-location logprobs removal, metadata, failure-after-headers, capture limits, cancellation and delivery budgets.
4. Primary owns implementation plus helper unit tests; secondary authors the isolated real-HTTP regression file independently. Secondary may make only assigned local test commits; no push/PR/runtime/docs/active/report/FIFO. Strategy coordinates import after checkpoint. Do not concurrently edit a file. Only the primary receives canonical control/response protocol. Existing foreground launch argv and CODEX_HOME remain unchanged; no subagents, background codex exec or other models.
5. Independent tests exercise the actual transparent HTTP handler with a gated fake upstream outside that boundary: observed minimal created/progress frames followed by reasoning are received while completion is held; answer stays withheld; later answer/unknown frame latches all later reasoning; completion delivers correct ordered output without replay. Include genuine clean zero-review and an accepted exact-edit path, reusing established fixtures where useful. Prove malformed/extra/answer-carrying minimal-lookalikes and embedded minimal-response carriers remain buffered, and supported full snapshots remain early-forwardable. Use events/barriers and finally cleanup, not timing guesses or a bypassed request.
6. Record old-runtime failure of the positive regression at its forbidden delay/receipt assertion and new-runtime PASS on the same test bytes, with explicit imported module identities. Root independently reviews implementation/negative cases and runs focused proof against actual primary runtime, never the stale secondary checkout by accidental import order.
7. Update root architecture current revision to2.2 with accepted old hash/base, exact bounded rationale/shape, D0 classification and affected projections. Keep existing diagrams unless meaning changes; visually inspect any changed documentation surface at desktop/mobile. Update README/chaining only if actual wording requires it, no editorial rewrite. Do not trim unrelated compact obligations to fit the cap.
8. Strategy owns bounded actual V100 qualification (up to6 synthetic main calls, at most one concurrent, small output caps). Coding must not access SSH credentials, key.txt, private provider profiles, environment secrets or remote hosts. Candidate qualification may use an explicitly isolated temporary loopback listener with candidate source and existing read-only index/Pandoc; never replace or restart the installed proxy/gateway/model. It must name the actual entry point/source and distinguish real HTTP delivery from classifier-only evidence. No raw prompts/responses/IDs or credentials in versioned artifacts.
9. Before canonical report publication provide PRE_REPORT: actual branch/PR/head, full diff, scoped source/projection changes, independent test commit/receipt, positive/negative results with tested commits and source hashes, draft report. Strategy must approve exact report bytes before publication. Preserve failures/timeouts/missing evidence accurately; report.governance equals this issued order map, current hashes belong in observed_source_governance; checks.sha is one existing40-hex commit and result uses the actual schema enum.

## Acceptance criteria

A1 observed minimal snapshots allow early reasoning. A2 unsafe lookalikes and non-lifecycle/embedded carriers remain conservative. A3 final answer remains withheld, exact repair/order/metadata intact and zero-review route verified. A4 independent secondary contribution and old-FAIL/new-PASS proof with real handler/explicit modules. A5 source/docs/projections bounded and truthful; no secrets or protected changes. A6 accurate bounded live V100 qualification. A7 immutable SELF publication, all four final-head CI green, independent reviewed-head development merge and remote-main verification. Deployment is separate.

## Verification

Focused: test_sse_prefix_streaming, new test_minimal_lifecycle_streaming, existing test_reasoning_streaming and relevant passthrough/metadata/cancellation/transport suites. Reuse existing Pandoc/protection/patch-integrity regressions; no weakening. Full development baseline is required through the existing Application baseline CI driver on the final reviewed tree (full pytest/OAP/lint/types/package/offline checks); do not run a redundant long local full baseline once this exact-tree full CI passes. Required final-head checks: Application baseline, Research reproducibility, OAP bootstrap acceptance, OAP report history. Use actual candidate-review bound to exact order digest/base, report/transcript/history/whitespace/source guards. No missing/pending/cancelled check passes. No milestone or linguistic-quality certification.

## Local setup and constraints

OAP_ROLE=coding explicitly for helpers. Only owner-started foreground panes; primary remains REPO_ROOT, secondary uses strategy-created039 worktree and assigned local branch/detached commits. Private receipts under this039 task directory0700/0600. Reuse existing qualified Python3.12/Pandoc3.7.0.2, owned ephemeral test resources. No model/profile/auth-home/service/network/GPU/weights mutation or dependency changes. Preserve all dirty/unrelated work.

## Documentation

Explain precise known minimal start-frame compatibility and unchanged conservative fallback, complete-answer gating and no-probability/exact-span promises. Name observed engine difference as evidence, not a universal engine guarantee or runtime switch. No private addresses/paths, credentials or raw model text in public docs/report/PR.

## Git and report publication

Strategy publishes immutable order then active pointer, commits those process files on branch oap/039-minimal-lifecycle-streaming from exact accepted base, and dispatches only primary via one exact OK. Primary imports only reviewed secondary test commits, commits/pushes implementation/docs/needed bookkeeping, creates one PR, and supplies PRE_REPORT. Canonical report oap/reports/039-a-minimal-lifecycle-streaming.md: sole-path SELF commit with actual implementation parent; push, remotely verify exact bytes/head/parent, then one response OK. No later mutation of this report/order. Any post-publication correction uses a same-PR suffix. Coding never merges or enables auto-merge. Strategy independently checks final head/CI/effects/rollback before expected-head-guarded development merge; no backend rollout.

## Decision classification

D0 bounded compatibility bugfix under explicit owner instruction. Strongest counterargument: permitting a previously rejected lifecycle shape could leak unexpected answer data or silently widen other envelopes. Mitigate by an exact-key, correctly typed, nonterminal direct-event variant, unchanged general validator/one-way boundary, independent negative tests, actual wire qualification and frozen protection/repair rules. No unresolved human-intent or D1 dilemma.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "039-a",
  "title": "Stream reasoning after minimal lifecycle snapshots",
  "objective": "039",
  "status": "FINAL",
  "phase": "MINIMAL_LIFECYCLE_COMPATIBILITY",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "6f7ae1aa662e103ddecd86375579e380c16468ea",
  "branch": "oap/039-minimal-lifecycle-streaming",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "038"
  ],
  "local_work": "Preserve all ignored/untracked data, old worktrees and private credentials; tracked clean accepted main.",
  "prior_review": "038-a remotely verified and PR44 merged; owner-authorized deployed V100 diagnosis independently reproduced before this fix.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner 2026-11-24 explicitly requests our repair proxy fix, publication, tests and development merge, and identifies vLLM versus llama.cpp backend difference."
    },
    {
      "kind": "E",
      "reference": "Deployed source/mode verified; minimal response.created/in_progress lacks output; actual classifier rejects frame1; bounded raw0.436s versus repaired3.5169s reasoning/3.5187s completion reproduced."
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
  "governance_ref": "6f7ae1aa662e103ddecd86375579e380c16468ea",
  "governance": {
    "AGENTS.md": "118be17b2e9e5b50d628c796b136ee1b8154ebf844af0007839f65848fd69f09",
    "ARCHITECTURE-for-agents.md": "0d453224ad82948829e2bfea9381120fd0eaa41281afc0a5de99cdfff56ffd31",
    "ARCHITECTURE.md": "2fb71ce6b210d1c1cc26f44d81f8e80b34d9be1ec0569e68b44e0748ed81f63c",
    "OAP-COMMUNICATION-coding-agent.md": "313d77900f9bbf1c974e76ea3f546083b117eeaac5f5fc2e964e77b3c19b9ccc",
    "PLAN.md": "d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0",
    "SECURITY.md": "1e3e9e59259ea6a57a3d8e368e896560c214f453c98792f7979e8d99b4154069",
    "TESTING.md": "7ce05ea2bf7e5b411e7a61ece0a0f577bb45bcc9bbd9f40b6480853f24a0ab27",
    "oap/coding-instructions/AGENTS.md": "db624bb41b34d8263bc5745083d86e7db5bb8907816cd8725871e7225eb6ce9d",
    "oap/governance/DISTILLATION-MAP.md": "2352a60133d7e71ca578919c580e4f0c6d1253945e6b6d2cc0d12aa26675611e",
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
    "source": "Owner 2026-11-24 requests scoped repair-proxy compatibility fix; strategy authorizes exact known minimal nonterminal lifecycle variant consistent with initial reasoning intent.",
    "accepted_ref": "6f7ae1aa662e103ddecd86375579e380c16468ea",
    "old_sha256": "2fb71ce6b210d1c1cc26f44d81f8e80b34d9be1ec0569e68b44e0748ed81f63c",
    "scope": "ARCHITECTURE2.2 bounded minimal lifecycle compatibility; complete answer capture, strict acceptance, exact spans and conservative default unchanged."
  }
}
```
