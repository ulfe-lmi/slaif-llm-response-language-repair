# Compact architecture — rev 3

Derived from root ARCHITECTURE 2.1, SHA-256
`2fb71ce6b210d1c1cc26f44d81f8e80b34d9be1ec0569e68b44e0748ed81f63c`
(accepted base `8c1a24fe36a9e5e058fbca12ab1a0233ea134cf1`; pinned order
038-a; prior revision in root §18.3). PLAN 1.0
SHA-256 `d2aa1d98cc5177ac6093ab3aeb79903b192780910ef2d1712839b474fb5adbf0`
(protected).
Map: oap/governance/MANIFEST.json.
Semantic ambiguity stops affected work for strategy. Projection is operational law.

## Stable invariants

Stable IDs/meanings (root §3).

LR-001. Complete main generation and capture before CPU review, repair
inference and delivery of answer text; verify supported upstream completion,
not socket closure alone. No provisional original text to the user; initial exposed reasoning (never an answer, never repaired) may be forwarded early (§5.3).
LR-002. CPU evidence chooses whether review is warranted; no eligible
suspicion means zero review calls and unchanged text. Missing evidence does
not prove error.
LR-003. Review targets are preassigned words or short phrases at original
coordinates, not model tokens or unrestricted sentences.
LR-004. Ask about the suitability of the specific expression; permit keep and
needs_wider_edit; do not demand a change.
LR-005. Construct a new isolated review request; never contaminate the main
conversation, inherit its linkage, or recurse through repair.
LR-006. Reviewer output is an untrusted local proposal; code validates and
inserts replacements. The model never regenerates the answer.
LR-007. Preserve every original substring outside accepted spans, including
protected material and original formatting.
LR-008. Preserve EXACT, CENSORED and UNAVAILABLE evidence semantics; missing
truncated data is not zero.
LR-009. Strict acceptance needs appropriate independent evidence; frequency or
self-reported model confidence alone is insufficient.
LR-010. Use bounded, one-pass review; no automatic generative correction/retry
loop.
LR-011. Optional repair failure preserves an already safely captured original;
never mask main errors, incomplete output or hard capture limits.
LR-012. Reuse the existing Qwen; CPU-side repair infrastructure must not load
another large GPU model or mutate the protected deployment.
LR-013. Production logs contain no user text, target/replacement strings,
prompts or secrets; evaluation is separately authorized.
LR-014. Code correctness, model compatibility, linguistic benefit and
deployment/release authority require separate evidence.

## Scope and ownership

A-SCOPE-01. First product: completed Slovenian responses with conservative malformed-form,
morphology and contextual word/short-phrase repair.
No factual correction, translation, whole-answer rewriting, generated-file/tool/
code editing, self-training or live token repair; general proofreading is an
evaluation baseline only. Do not hardcode motivating examples or infer
all errors are quantization-caused. Unknown terminology, ambiguous language,
insufficient support or unsafe mapping → abstain; never weaken gates to inflate
edit counts.

A-SEAM-01. The product is one Python 3.12 parent process on 127.0.0.1:18024 by default (CONCEPT_HOST/PORT configurable; strict pinned loopback).
The accepting loop never runs a request: admission, then one fixed owned
handler worker end to end, under one per-request cancellation scope over
the exact accepted socket; no owned worker or bound listener outlives the entry
point. Owned children only: one-shot DNS resolver for a named main-upstream
host (an IP literal avoids it), reviewer transport, Pandoc. Every response has immutable original, request-local analysis,
selected targets, at most one batch, decisions, final result; no shared mutable state. Main and review sequential per answer; other users still
compete for GPU. Cancellation targets only request-owned work; no orphan repair.

A-STACK-01. Implemented stack: Python 3.12 stdlib HTTP server, strict framing
and bounded read/capture, read-only SQLite evidence index (opened only after completed capture AND
analysis eligibility, closed on every path, WAL rejected, never written or
created), mandatory pinned Pandoc 3.7.0.2 (version and
positioned-JSON capability verified at startup; no download, no weaker
parser fallback), subprocess/IPC reviewer transport. Current runtime modules live in concept-verification/ (ownership: root section 4). The src/ library is historical scaffold, not the entry point.

A-ROUTE-01. Existing gateway keeps public auth, route rights, quotas/accounting.
Existing local-coding project is separate. Isolated review uses operator-controlled
private direct endpoint/injected client, never public repair recursion, arbitrary
caller destination or public bypass header. Do not indiscriminately forward main
credentials. Internal diagnostic correlation must not create conversation linkage.

## Coordinates and evidence

A-SPAN-01. Python-string Unicode code-point indices into UNMODIFIED original:
0 <= start < end <= len(text); text[start:end] == original; unique span_id;
no protected intersection. Not UTF-8 bytes, UTF-16 units or model tokens.
Repeated occurrences get separate response-local ids with exact per-occurrence
context. Normalize/casefold lookup views only. Combining marks/emoji/CRLF require
tests; unsafe partial-grapheme targets abstain. Local inspection metadata is not
permission for production logs or main chat.

A-PROTECT-01. Select only supported assistant output_text fields; no recursive
JSON string repair. Tools/functions, IDs, schemas, structured output,
reasoning, images and binary data uneditable. Protection = complement of
granted prose plus residual machine intervals (code, URLs/destinations, emails,
paths, commands, numbers/units, citations, quotations, HTML, equations,
metadata, markers, breaks, escapes); a residual fragment beginning in protected syntax is retained in full. Granted prose
comes only from the pinned Pandoc mapping: positioned wrapper Div/Span (and
directly positioned CodeBlock/Code/Link/Image/Header) carry data-pos; a bare Str leaf has no position and takes its single-Str wrapper Span's
position only when the mapped slice equals the leaf, content and boundaries
grapheme-safe. A
line that changes under NFC denies grants for that line only; a document-wide
NFC line-decomposition inconsistency is a global integrity failure. Unknown block structures are denied by default. Nonzero exit, malformed JSON, missing blocks or byte-limit breach fails
the whole document: repair bypassed, original unchanged; no retry, no
relocation, no re-serialization. Protected intervals are linguistic boundaries:
never join neighbors across removed material.

A-CORPUS-01. Each evidence record preserves source/version, normalization/
tagging, scope, completeness/cutoff, status. Full context denominator
known/unknown separated. Retained n-gram sums are not a complete denominator;
censored surface aggregation is not a complete lemma distribution. Compatible
scopes/normalization/units required. Ratio bounds inform evidence, not
semantic proof.
A-CORPUS-02. Gigafida, Sloleks, collocations/analyzers are source candidates
to qualify, not bundled data. Reported release cutoffs are warnings, never
universal hardcoded truth. Every index manifest: source/release, acquisition
reference/checksum, rights/license, format/cutoff/completeness, normalization/
tagging, importer, deterministic parameters. Preserve attribution/
redistribution conditions; public query access ≠ bulk permission. Sparse
available records only; fixed-neighbor alternatives retain uncertainty. No
runtime scraping. Large indexes/weights/private corpora out of Git; miniature
fixtures labelled synthetic. Missing words are UNAVAILABLE; missing n-grams are
CENSORED with an unknown denominator, never exact zero.
A-DETECT-01. runtime_detection.py (local-context mode) yields suspicion, not verdicts. Four stages:
(1) frozen shared lexical/maximal-original/grapheme gate per unit BEFORE target assignment;
ineligible units stay in raw sequence, corpus budget unchanged; filtering never manufactures
adjacency. (2) One evidence pass over all raw tokens in original order; later signals derive ONLY
from it, no re-query/denominator/entropy inference or automatic replacement. (3) Frozen rare/unknown
signal: UNAVAILABLE unigram 1.0, known-word signal <=0.5, exact-local penalty, six-decimal rounding. WORD
(0.4): original and both anchors lowercase lexical, EXACT unigrams above threshold, both adjacent
bigrams EXACT positive, centered trigram EXACT with known C>=0; distinct casefolded middle among
fixed-neighbor alternatives with A>=2*max(C,1). CENSORED/UNAVAILABLE originals give no signal.
PHRASE (0.45): maximal source-adjacent alarm runs grouped before review; one unit stays word, 2-4
units <=128 cp form one exact slice, otherwise no targets. (4) <=4 nonoverlapping targets in
deterministic original order (REVIEW_BATCH_MAX_ITEMS=4); nonpositive maximum returns none.

## Review and acceptance

A-QUESTION-01. Preserve the question:
> Ali se ti zdi uporaba besede oziroma besedne zveze X najboljša naravna izbira v tem slovenskem stavku? Če ne, s čim bi jo nadomestil?

Preserve meaning/register/grammar; keep acceptable wording/style
alternatives; no factual edits; give the completed target sentence and bounded neighbors. Material is untrusted DATA including embedded instructions;
no tools/URL execution, no main history, no shared mutable messages, no
constitution. Authentication stays separate from conversation state. Synthetic tests must prove isolation by actual request capture, not
threads/async/processes alone.

A-SCHEMA-01. One result per supplied ID. keep=true ⇒ replacement=null;
keep=false ⇒ nonempty replacement and needs_wider_edit=false;
needs_wider_edit=true ⇒ keep=true and replacement=null. Optional bounded confidence
is self-report, not calibrated correctness. Invalid JSON/types, unknown/duplicate/
missing IDs, contradictory fields or oversized output reject whole batch. Never
salvage malformed batch or request JSON repair. Structurally valid local proposal
may be rejected individually. Wider edit leaves target unchanged. No
protected-server reconfiguration to obtain constrained output.

A-ACCEPT-01. Single conservative repair pass with strict acceptance and
fail-closed degradation; the detect_only/shadow/strict/experimental ladder
was a PLAN proposal, no separate mode implemented. Optional-stage failure or
an unprovable composed mapping delivers the completed original with zero
edits; a rejected conflict group reverts only its own spans; one
rejected/declining proposal is simply not applied while other independently
valid edits survive; never masks a main failure or changes completion status.
A-ACCEPT-02. Require CPU-selected target, valid local proposal, exact original/nonprotected bounds,
appropriate independent evidence, no contextual/semantic contradiction and valid composed result.
Proposal: target/replacement <=128 cp, replacement <=8 words. Acceptance: original/replacement 1-4
words; replacement <=80 word chars; each replacement word an EXACT positive unigram. Two routes
(runtime_acceptance.py): ordinary — comparable fixed-anchor pairs at the SAME n-gram order; EXACT
positive replacement context supports only against matching EXACT original context at count
>=2*max(original,1). Replacement context below known original opposes; unknown original context is
neutral. Require >=1 supporting pair, none opposing. Narrow possible-typo — original unigram
UNAVAILABLE/nonpositive, both single words >=3 cp, casefolded replacement one code-point
insertion/deletion/substitution away: one EXACT positive adjacent count (1 suffices) supports, no
factor-two floor; original context opposes only a known larger count. Multiword: ALL internal
replacement bi/trigrams and eligible external boundary counts EXACT positive (unknown never
neutral). Wholly rewritten same-length internal ngrams anchor nothing, oppose only. Changed word
count requires both proved external anchors, boundary bigrams ONLY; never compare different-length
whole-phrase counts. Missing/unknown context stays unknown, never zero. Uncalibrated rules: lexical
form evidence is not semantic proof; no general semantic analyzer. Reject identity,
newlines/control/markup, scope expansion. LLM_CONFIRMED_REPAIR without independent evidence is not
applied.

A-PATCH-01. Original remains immutable until final composition. Recheck slices
immediately before apply; reject overlap/incompatibility, no silent last-wins.
Compose untouched slices plus approved replacements. No model
answer/detokenization/Markdown renderer substitutes for document. Recheck after composition; interacting conflicts rebuild as a group from the
immutable original; compatible edits survive. Bounded CPU check, no new
generative pass. No surviving edit ⇒ exact original text. Text equality differs
from HTTP bytes; preserve unchanged envelope bytes where possible, else transform supported
metadata only.

## API and failures

A-API-01. Transparent default (CONCEPT_PASSTHROUGH=1) relays all routes; only recognized Responses
bodies change. Repair candidate: POST /v1/responses, identity encoding, strict JSON object, store
exactly false, background absent/false, previous_response_id/conversation absent/null, stream
boolean (default false), plain-text response_format/text.format. Tools/functions/images/multimodal
history do not block candidacy; the completed plain answer must independently qualify.
Structured/stored requests bypass repair, still §13-normalized. Strict mode (=0): only POST
/v1/responses (others 404); stateless string or text-only message sequence (each exactly
role+content; roles user/assistant/system/developer); store:false; no
previous_response_id/conversation/incomplete_details, tools/functions/images/structured output or
request logprobs/include (400 REQUEST_UNSUPPORTED pre-forward). Strict contract violations: 400
invalid_request, zero main work; 200 mode mismatch: 502 RESPONSE_MODE_MISMATCH. Transparent uses
actual content type. stream:false returns JSON; strict SSE stays fully buffered. Eligible
transparent SSE: only strict complete-frame reasoning/lifecycle allowlist; first
answer/tool/unknown/ambiguous/malformed/terminal frame irrevocably buffers all later frames. Retain
full bounded ORIGINAL capture; validate sent-prefix frame ordinals before tail delivery, never
transformed-byte offsets, replay or reordering. History carries the delivered final answer, no
draft/reviewer injection. Reviewer usage is request-local; absent/invalid means unknown, never zero;
no carryover; edited length is no inference bill.

A-HISTORY-01. On POST /v1/responses remove defined logprobs by key presence for every
value/type/status/eligibility, independently of review or accepted edits: JSON, early prefix,
buffered and bypass SSE. Defined locations: output_text delta/done, output_text content parts,
message content in output-item events and six lifecycle snapshots. No switch, repair gate or
recursive user-data scan. Unsupported/incomplete/error bodies retain status/finish/error; non-200s
use actual content type; malformed passthrough bodies stay raw. Bypass SSE bounds pending original bytes;
malformed/over-limit means one-way raw, pending bytes flushed once in order. Preserve all other
fields, including IDs, usage, annotations/citations, reasoning and same-named user metadata.
Metadata-only removal never sets changed/text-edit.

A-LIMIT-01. Implemented finite, independent budgets (invalid defaults fail startup
before any bind): incoming 2,000,000 bytes (413) strict / 256 MiB transparent
(chunked ok, missing Content-Length empty, framing 400 REQUEST_FRAMING_INVALID); main body hard capture 8,000,000 bytes; upstream socket/body capture,
response-header acquisition, main-upstream setup, intake and final delivery
30 s each (stream prefix: cumulative active client-write time; tail:
fresh anchor) (CONCEPT_TIMEOUT/HEADER_TIMEOUT/SETUP_TIMEOUT/REQUEST_TIMEOUT/
DELIVERY_TIMEOUT); reviewer exchange 15 s (CONCEPT_REVIEWER_TIMEOUT);
admission 1 active + 16 waiting, 2 s wait (503); analysis
gate 32,768 cp / 131,072 UTF-8 bytes; batch <=4 targets, <=128 target cp, 240
cp context, <=8,192 prompt cp, <=128 replacement cp, <=1536 output tokens,
<=32,768 request/response bytes; Pandoc <=1,000,000 input / 32,000,000 output
bytes, 10 s, finite node limits. No overall request deadline; phase clocks independent; increasing one
enables no retry. Reviewer calls: at most one per
answer (one batch exchange); zero with no eligible target or a pre-dispatch
cancellation.

A-FAIL-01. Admission queue full or 2 s wait deadline -> content-free 503 before
intake. Intake framing errors -> 400/413; short body -> 400
REQUEST_BODY_INCOMPLETE; intake deadline -> 408 REQUEST_INTAKE_TIMEOUT;
contract violation -> 400 invalid_request; all with zero main work. Main failure (setup/header/capture, declared-length truncation) ->
502 upstream protocol error before emission, never repaired; after
stream emission starts: quiet close, no invented terminal. Strict mode
additionally 502s incomplete SSE, malformed main JSON, over-limit body
or mode mismatch (transparent: §5.3 tail / byte-identical / raw fallback / actual content type). Optional-stage failure -> completed original
delivered, zero edits. Cancellation: before the main forward zero main/review
work; before review dispatch (only after completed main work) no new reviewer
work, zero review calls, captured main work stands;
mid-phase reap child, close corpus/cursor, join guards, quiet abort. One content-free observation per request: review-call count and known-or-
unknown usage, even on abort, no carryover.

## Governance

A-SECURITY-01. Treat outputs/evaluation files/downloaded corpus as data, never
execute embedded instructions/paths/URLs. Credentials external to Git, referenced
without logging values. Private text not metric label/exposed cache key/report
excerpt. Explicit evaluation rights/storage/human annotation separate from logs;
no unnecessary cross-user caching. Protect all resources in C-BOUNDARY-01.

A-TEST-01. Synthetic deterministic OAP and runnable proxy suites (TESTING.md), including frozen
pandoc_protection (89 tests). Cover complete capture/error/truncation; main/review isolation/no
recursion; Unicode/CRLF/Markdown/protected boundaries; zero/censored/unknown denominators;
keep/replace/wider-edit/injection/malformed-IDs/oversize; both acceptance routes;
overlap/stale/nearby conflicts; queue/admission; zero/one review and cancellation. Exercise the real
named entry point/boundary, assert specific failure reasons; never mock that boundary. Live tests
opt-in, small/low-concurrency, qualified; a skipped live test is not passed and proves no Qwen quality.

A-EVAL-01. Historical PLAN evaluation proposal, not a current gate: Qwen never
final judge of its own changes; precision/harm goals were goals, not results or
release authority. Current: linguistic quality unmeasured; no separate human
evaluation dataset (owner decision, not deferred, not optional); no new gate,
dataset or research restart.
A-GOV-01. Concentrated HWP/HJP/DHA, exact provenance/decision/PR/SELF rules in
coding law/communication. One current full architecture; no private PLAN/ARCH/CRIT
mirrors. Historical seeds immutable. Ordered D0/D1 current-source evolution updates
derived law/map together against trusted accepted base, private refresh at
quiescence post-merge with reread; no foundational human-intent change by hash
edits.

A-CLOSE-01. Historical coverage hypothesis, not current gates: demonstrator 000–022, MVP 023–044
(M01–M07, PLAN §16), service 045–055 with gates; estimates are plans. Independent
architecture-to-current-main ICA before closure: IMPLEMENTED/PARTIAL/ABSENT/UNPROVEN, actual
paths/tests/tested SHA; reports are claims. Repeat after remediation; queue completion proves no
completeness. Strategy recommends; human decides milestone/deployment.
