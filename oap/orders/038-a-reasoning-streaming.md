# Work order 038-a — Stream upstream reasoning before repaired answer delivery

Status: FINAL

## Identity

Objective038, round038-a, CREATE_NEW_PR, branch oap/038-reasoning-streaming. Repository ulfe-lmi/slaif-llm-response-language-repair, default main. Seed038 capture work is already implemented; remap this numeric objective to the explicit owner-requested reasoning-streaming feature. One canonical order, branch, PR and SELF report; two owner-started foreground executors share scoped assignments below.

## Provenance

H: Owner 2026-11-15 requests realtime thinking streaming, accepts the bounded initial-reasoning-prefix proposal, supplies two foreground Qwen coding panes, and explicitly directs strategy to start implementation and keep both busy in parallel until done. A: PLAN sections1/5/11.3 and LR001/005/007/010/011 preserve completed answer repair, reasoning noneditability, isolated review and truthful failure. E: merged PR43 transparent handler buffers the entire potentially repairable SSE body before delivery; reasoning is delayed although never edited. PR42 rename fix merged and main verified. This order explicitly authorizes reasoning delivery before completed capture; final answer delivery/review remain gated on completion. It overrides earlier single-executor wording only for these two existing foreground agents; no background or spawned agents.

## Current verified state

Trusted base 8c1a24fe36a9e5e058fbca12ab1a0233ea134cf1 includes merged PR42 and PR43. Prior037-a canonical report remotely verified; PR40 merged. Local tracked clean; preserve untracked .rclone-speed-test/, .research-test-scratch/, corpus/, ignored key.txt and all private research/data/auth. Panes%23 and%83 both replied READY; exact owner-started launch argv verified. No live services, model profiles or host configuration are to change. Canonical architecture2.0 predates PR43: reconcile only affected transport/listener/eligibility clauses against actual merged runtime plus this feature.

## Governance

Trusted accepted source revision is the exact base above; order metadata pins its identities. PLAN, CRITICAL, bootstrap source lock and historical orders/reports remain byte-identical. Authorized bounded current-source evolution records old hash/revision, source-faithful PR43 delta and this explicitly authorized initial-reasoning exception. Allowed governance paths:

- ARCHITECTURE.md
- ARCHITECTURE-for-agents.md
- oap/governance/DISTILLATION-MAP.md
- oap/governance/MANIFEST.json

Use existing candidate-review actual CLI bound to this exact order digest and trusted base. Update current workflow candidate-review invocations to this exact authorization if necessary; retain all protected-source/history checks and explicit default-deny. Do not weaken bootstrap source guards or rewrite fixtures to accept unrelated source drift. Synchronize necessary generated identities and CONTEXT-BUDGET current counts without changing its budget law.

## Goal and dependencies

On supported streaming Responses requests, deliver exposed upstream reasoning promptly, retain final-answer exact-span repair and preserve merged transparent forwarding, cancellation, resource bounds and metadata contracts. JSON/nonstreaming behavior remains intact. After reconciliation of merged037/PR42/PR43; no separate human evaluation dataset, research restart, repository-history cleanup or new milestone gate.

## Scope

Runtime changes limited to concept-verification/proxy.py and an incremental SSE helper if useful, tightly necessary delivery/lifecycle integration and focused tests. Do not change Pandoc protection, detector/acceptance rules, language evidence, reviewer prompts or inference profile. Documentation: affected architecture/compact/projections, README, INSTALL, QUICKSTART, concept-verification/README and docs/PROXY-CHAINING as necessary. OAP: exact order/active, necessary current generated identities, narrowly ordered candidate-review CI adaptation, existing report counter and final report. No unrelated refactor, packaging, research corpus migration or deployment.

## Non-goals

No answer-token streaming before completed repair, arbitrary interleaved reasoning reordering, acceptance/protection weakening, research restart or data migration, new model/provider, live deployment or unrelated feature.

## Files and boundaries

Compact coding read set first. Runtime transparent path (_forward_transparently/_transform equivalents), strict handler, SSE parser/transform_sse/normalization, request_lifecycle and delivery timeout ownership. Existing tests/test_passthrough.py, test_responses_contract.py, test_request_cancellation.py, test_delivery_transport.py, test_header_transport.py, test_upstream_setup.py, Pandoc/protection/patch-integrity suites. Bounded full ARCHITECTURE sections3/5/13/14/18 are authorized when reconciling transport prose. Upstream Responses streaming reference and actual bounded synthetic Qwen event trace are evidence, not inferred wire guarantees.

## Requirements

1. State machine: recognize complete SSE frames incrementally across arbitrary byte/chunk/UTF8 and CRLF boundaries. Initially allow only validated response-start/lifecycle nonterminal events with no answer payload, recognized reasoning item/part/text/summary events and harmless framing/comments. Preserve original event order, item IDs/output indices/sequence numbers and reasoning content. Explicit reasoning event allowlist; never classify answer text by substring or content heuristics. No unknown event may be treated as safe early content. On unknown/ambiguous or first non-prefix event, irrevocably buffer that frame and every later event; this includes the first message/answer/tool/terminal event. No later reasoning jumps ahead of buffered answer. Do not synthesize model thinking or expose encrypted fields through new routes.
2. Full main capture/valid completion precedes detector, review and any answer delivery on repairable bounded paths. Keep complete original stream (bounded by existing total capture limit) for existing ownership/eligibility validation and transformation. Forwarded prefix is never replayed; transformed tail and terminal snapshots agree with final answer. Completed/failed/incomplete/[DONE] cannot be forwarded early. No duplicate/lost frames or answer slices. No targets -> zero review; accepted edit remains exact-original-span; protected reasoning bytes/content unchanged except defined metadata normalization where applicable.
3. Use normal launcher default transparent forwarding and also preserve strict-mode semantics. Prefer the same early-prefix behavior for supported strict SSE path; at minimum do not regress strict behavior and explicitly document any intentional scope limitation for strategic review. Non-Responses passthrough stays streaming. Tools/images/stored/structured/opaque upstream errors retain PR43 behavior. Existing transparent capture-limit fallback remains coherent: emit already-forwarded prefix exactly once and remaining original bytes in order, no repair; do not change strict hard-limit failure into silent truncation. No early flush bypass of total capture/event-size budget; prefix is counted. Malformed framing/JSON/nonfinite/duplicate-key events never become an approved early prefix.
4. Failure before headers retains normal HTTP failure. After first headers/prefix, later upstream failure/truncation/timeouts must be observably incomplete: no second HTTP response, no successful completed/[DONE] fabrication, no review of incomplete main, no provisional answer release. Prefer close/error behavior consistent with actual Responses clients; if a protocol failure event is synthesized it must be fixed content-free and correctly ordered, with evidence clients treat it as failure. Explicit failed/incomplete upstream events remain faithful. Unknown unsupported event buffering must not silently lose events.
5. Cancellation/backpressure: client disconnect during reasoning interrupts exactly request-owned main work; no new review dispatch. Delivery writes are bounded without letting a single final-delivery deadline incorrectly cover all upstream generation/review time. Preserve independent phase budgets and cleanup/accounting once on all paths, no orphan child/worker/timer/socket. Handle stalled client and upstream with deterministic tests; no keepalive time extension, unbounded event buffer or new polling model.
6. Always apply established defined-location logprobs removal to recognized Responses representations, independently of repair, without deleting same-named user metadata. Do not expose answer text inside early lifecycle snapshots or reasoning event wrappers. Preserve annotations/citations/usage/IDs/status and headers; transformed streaming output must not retain stale Content-Length/digest/entity validators. Unknown usage remains unknown.
7. Primary executor owns runtime, docs, narrow OAP/CI reconciliation, integration, commits/one PR and sole final report. Secondary executor independently authors transport regression tests in its isolated private worktree and writes a private review/evidence receipt. Secondary may commit only assigned tests locally; never pushes, creates PR, alters runtime/docs/governance/active/report or sends canonical FIFO. Primary imports secondary test commit only after strategic checkpoint; do not edit the same file concurrently. Both stay in existing foreground panes; no new agents/CLI/provider. Only primary receives canonical control; secondary participates under the same consumed order via explicit strategic handoff. Strategy owns synchronization between scopes.
8. Reuse existing protection/metadata/patch-integrity/transport tests; no weakening. Secondary must prove early client receipt while fake upstream is explicitly blocked before completion (threading events/barriers, not latency guesses), answer withholding, correct approved edit, zero-review route, no prefix replay, later reasoning ordering, malformed input, split Unicode/frame boundaries, metadata fields, truncation/error, cancellation and backpressure/bounds. Exercise real normal launcher or actual transparent HTTP handler with fake upstream/reviewer outside that boundary. Run focused then relevant broader suites; inspect failures before expanding work. All four final-head required CI must pass. Separate stale known issues from new defects using base evidence.
9. Bounded live qualification after offline tests: use existing authorized external reviewer/main endpoint and key read privately, Responses API only, qwen3.8-27b-rtx3090 preferred. At most12 synthetic main requests plus12 optional review requests initially (within prior1M3090 allowance); no real user text or service configuration. Confirm actual reasoning event family and timing through a test proxy on an unused loopback port, record only event names/counts/timings/check outcomes. Raw events private0700/0600, never public. If actual upstream sends no exposed reasoning, state that accurately; do not invent live success. Busy endpoint ordinary prerequisite, continue offline work. No other protected host changes.
10. Provide strategy a PRE_REPORT checkpoint with diff, secondary test evidence, focused/broader outcomes, current PR/head, draft report and limitations before canonical report publication. Stop report publication pending strategic review; implementation may continue independent in-scope tests while review is pending. No deployment or release claim.

## Acceptance criteria

A1 initial reasoning received before main completion, answer withheld. A2 transformed stream exact text edits and coherent metadata/order with no duplicate prefix. A3 complete and truthful failures before/after emission plus explicit incomplete/failed status. A4 cancellation/backpressure/budgets and cleanup/accounting. A5 transparent PR43 functionality plus strict/JSON/protection/logprobs regressions. A6 independent secondary test contribution reconciled in one branch/PR. A7 source-faithful docs/architecture/projections and trusted authorization. A8 bounded actual-Qwen qualification accurately classified. A9 immutable report-only SELF publication, strategic final-head CI review and accepted development merge. Missing evidence remains explicit, never inferred from a polished report.

## Verification

Named focused tests: new tests/test_reasoning_streaming.py (secondary), any helper unit tests (primary distinct file), existing test_passthrough, test_responses_contract, test_request_cancellation, test_delivery_transport, test_upstream_setup, test_review_transport, test_pandoc_protection and patch/composed validation. Run baseline driver per scripts/verify_development_baseline.py and required CI after coherent integration. OAP candidate-review actual CLI with trusted base/exact order digest plus bootstrap/report-history/transcript/whitespace guards. All generated hashes/bytes must match. Evidence statuses and tested SHA explicit. Fake/live/client evidence distinct; no separate quality dataset.

## Local setup and constraints

Explicit OAP_ROLE=coding in helper environments. Only two supplied foreground panes, no subagent/background codex or provider/profile/auth relocation. Primary works at REPO_ROOT; secondary at strategy-selected private Git worktree, source shared but checkout isolated. Private notes/scratch under STRATEGIC_HOME strict0700/0600. Ephemeral test servers on unused loopback ports only. Reuse existing pinned Pandoc and external SQLite index read-only. Never read/print/commit keys; don't use shell tracing. Existing launch/auth/network/GPU/service constraints apply. Neither executor merges or enables auto-merge.

## Documentation

Explain early exposed reasoning versus held answer, buffering boundary, event-order conservative fallback, error-after-headers and actual current transparent/listener behavior. Retain mandatory Pandoc/exact original span/no-probabilities guarantees and no quality claims. Update relevant diagrams legibly; no project-wide editorial rewrite.

## Git and report publication

One branch oap/038-reasoning-streaming from exact base. Strategy commits activated order/active as prerequisite; primary commits implementation and imports reviewed secondary tests, opens one PR and handles scoped CI. Secondary never publishes canonical report. Preserve history/unrelated work. Update research RESEARCH-STATE own report-count bookkeeping only if required by existing test, do not touch scientific records. Before report: PRE_REPORT strategic review. Sole report oap/reports/038-a-reasoning-streaming.md, SELF final report-only commit with exact implementation parent, then remote verification and exactly one response OK. Follow-up strategic corrections use same-PR suffix. No rewriting report/history. Strategy independently reviews final head, effects, all required CI, rollback and merge. PR43 is accepted baseline; not authority for new deployment.

## Decision classification

D0 engineering implementation under explicit owner-authorized reasoning streaming and two foreground executors. Strongest counterargument: early emission removes the ability to return an HTTP502 after malformed upstream output, and reordering could corrupt SDK state. Mitigation is an explicit incomplete-stream failure contract, conservative one-way buffering boundary, independent real HTTP negative tests and bounded live compatibility checks. No unresolved human meaning or new D1 choice; return concrete irreconcilable protocol issues to strategy rather than weakening repair.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "038-a",
  "title": "Stream upstream reasoning before repaired answer delivery",
  "objective": "038",
  "status": "FINAL",
  "phase": "INITIAL_REASONING_STREAMING",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "8c1a24fe36a9e5e058fbca12ab1a0233ea134cf1",
  "branch": "oap/038-reasoning-streaming",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "037"
  ],
  "local_work": "Tracked clean accepted main; preserve untracked/ignored local data and both existing foreground agent sessions.",
  "prior_review": "037/PR40 and rename PR42 merged, PR43 deployed owner fixes accepted as current baseline.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner 2026-11-15 explicitly requests realtime thinking streaming, two supplied foreground Qwen agents and OAP implementation until done; observed whole-body SSE buffering in merged PR43."
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
  "governance_ref": "8c1a24fe36a9e5e058fbca12ab1a0233ea134cf1",
  "governance": {
    "AGENTS.md": "118be17b2e9e5b50d628c796b136ee1b8154ebf844af0007839f65848fd69f09",
    "ARCHITECTURE-for-agents.md": "53328fa21328108fa65d28424e0baccbd3b29280699144aa09c7e45dbff084a1",
    "ARCHITECTURE.md": "4df1664cec86aae1e3e07e19f61650919fa8dc5011acc67f411848eeee0a8ed9",
    "OAP-COMMUNICATION-coding-agent.md": "313d77900f9bbf1c974e76ea3f546083b117eeaac5f5fc2e964e77b3c19b9ccc",
    "PLAN.md": "d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0",
    "SECURITY.md": "1e3e9e59259ea6a57a3d8e368e896560c214f453c98792f7979e8d99b4154069",
    "TESTING.md": "7ce05ea2bf7e5b411e7a61ece0a0f577bb45bcc9bbd9f40b6480853f24a0ab27",
    "oap/coding-instructions/AGENTS.md": "db624bb41b34d8263bc5745083d86e7db5bb8907816cd8725871e7225eb6ce9d",
    "oap/governance/DISTILLATION-MAP.md": "d5f4c1fbb546f552dcd1a28b81725cc714b1c9820317d714fbe5c2dcbd2f9193",
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
    "source": "Owner 2026-11-15 explicitly authorizes initial reasoning streaming before answer completion and implementation; reconcile affected merged PR43 transport clauses.",
    "accepted_ref": "8c1a24fe36a9e5e058fbca12ab1a0233ea134cf1",
    "old_sha256": "4df1664cec86aae1e3e07e19f61650919fa8dc5011acc67f411848eeee0a8ed9",
    "scope": "Initial exposed reasoning may precede completed answer capture; answer repair/delivery, exact spans and strict acceptance unchanged. Reconcile affected PR43 transport/listener clauses and synchronized projections."
  }
}
```
