# Work order 043-a — Official OpenAI SDK comparison client and live gateway qualification

Status: FINAL

## Identity

Objective043, round043-a, CREATE_NEW_PR. Repository ulfe-lmi/slaif-llm-response-language-repair, default main, verified accepted base709dc799950a7a7ed56314560fc033e1129b9e50. New branch oap/043-openai-sdk-client; create one actual PR after non-report implementation, never invent its number. Previous042-a/b/c and PR48 are merged and preserved. Only existing primary foreground coding agent; no new/background agent, codex exec or live-process restart.

## Provenance

H: Owner reports the terminal client fails even on unrepaired models; directs client fix against unchanged gateway, specifically use the OpenAI Python library, and explicitly authorizes needed installations. Preserve original client purpose: Codex TOML/profile/catalog, bare /model and /thinking numbered menus reading next line, /quit, plain scrollback, server thinking and delivered/original answer display.
A: Existing proxy/Pandoc/exact-span/API metadata/reviewer contracts; caller-only compatibility change. E: Exact localprofile TOML credential authenticates GET/models200. Repaired/unrepairedRTX3090 both return response.created/in_progress then response.failed with responses_stream_event_not_supported; no answer delivered. Root direct SDK3.60.0 reproduces for inputlist; stringinputHTTP400. I: Rejected event is gateway/provider-boundary observation, not evidence of badkey or proxy defect; no deployedgatewaymutation is authorized. SDK transport replacement alone is not a demonstrated fix.

## Current verified state

Local/remote/defaultmain and private acceptedref709dc799950a7a7ed56314560fc033e1129b9e50; PR48merged, no openPR observed at23:50UTC. All4final042 CI passed including13drivercommands/cleanup; rootclient60/fiveprobes/guidecopy and coding60actualCLIHTTP tests remain syntheticsoftware proof. Owner reported livefailure aftermerge; preserve those oldclaims/failedepochs, qualify them accurately, never rewrite reports. Trackedclean; preserve5152untrackedcorpus/scratchfiles, ignoredkey600, allbranches/worktrees/privatehistories. Originalprimary3341327/pane%23 alive/idle, sameownerargv/home/auth/model/profile. Doctorstructurevalid/errorsnone; accepted-runtimegovernancevalid/coding41327. Existingactivationflags remainunchanged.

## Governance

All16 accepted source/compact hashes below stay unchanged: external caller example, not proxy architecture evolution. Validator compatibility allowlist only:

- oap/governance/MANIFEST.json

No actual source/manifest/projection change authorized without a demonstrated necessary task-scoped need reported to strategy. Preserve PLAN/CRITICAL/seeds, source lock, oldorders/reports, six whitespace exceptions and all proxy/runtime source contracts.

## Goal and dependencies

Use the official OpenAI Python SDK as the actual Responses request/event boundary, simplify the hand-written transport, retain correct client UX/history/privacy, and demonstrate a usable request to the existing gateway without gateway/provider/model/service changes. Successful live qualification is distinct from software tests. Do not claim the owner issue fixed merely because SDK imports or fakes pass; establish actual healthy completion or report the concrete remaining compatibility boundary and smallest viable alternative without weakening requested behavior.

## Scope

examples/repair_chat.py; examples/README.md; tests/contract/test_repair_chat.py; pyproject.toml and uv.lock for a pinned SDK client/development dependency; INSTALL.md and TESTING.md only narrowly if invocation/install/testing instructions change; exact affected GENERATED-FILES records. CI candidate-review order/hash/base pins in application-baseline.yml and oap-bootstrap.yml only, retain current40-minute command/60-minutejob and all other guards/budgets. research/RESEARCH-STATE.md machine counter107 to108 for this one report; registry42/incidents2 and historicalprose unchanged. Ownorder/active/report only. Prefer no core runtime dependency change: a client dependency group included by dev/default tests is suitable (for example client=[openai==3.60.0] and dev include-group client); normal runtime dependency versions/closure must stay unchanged. No optional gate or research programme.

## Non-goals

No gateway or neighboring repository mutation, backend/provider/model/quantization/service/network/VPN/firewall/occupied-port/auth change. No second GPU model, new AI agent/Codex process/provider or changed session home. No OAuth/auth.json access, command execution from config, hosted/local tool execution, dummy/fake tool declaration to activate gateway modes, invented encrypted replay/session/principal identity, automatic protocol retries/fallback or silent thinking disable/nonstream downgrade. No raw credential/server message/customer/reviewer content in logs/reports/public fixtures. No separate human evaluation dataset or unrelated milestone.

## Files and boundaries

Read normal tiny router, compact coding sources, exact order and relevant localclient/tests/guide/dependency/workflow contracts. Existinggateway documentation/source may be inspected read-only for the actual request contract; this does not switch role or grant gateway implementation authority. Root supplies observedprofile facts and private SDK probe environment in handoff; do not cat credential-bearing files or expose keys to the model. Running the actual client/SDK against the owner's configured profile is permitted; secrets must stay inside the application process and never printed/captured in report evidence. Use synthetic prompts and content-free diagnostics/aggregate evidence only.

Official SDK primary reference: https://github.com/openai/openai-python . Root installed and inspected openai3.60.0 in owned privatePython3.12environment; this version uses httpx2 through openai.DefaultHttpxClient. Do not assume old httpx.Client is the SDK's required transport type; inspect the pinned installed API. Keep existingproxy httpx/pydantic locked versions when resolving the new dependency; no broad upgrade.

## Requirements

1. Replace urllib/manualrequest/SSEtransport as actual API boundary with openai.OpenAI and its Responses streaming API. Pinned SDK3.60.0 is the observed starting version; if a concrete incompatibility requires another official version, record exact evidence and pin choice without unrelated upgrades. Add reproducible client/development installation/run instructions, lock dependency group, keep service runtime closure unchanged. Every API call uses exact selected model/profile key/base URL, explicit max_retries=0, finite timeout and deterministic owned client/stream cleanup. Refuse redirects via SDK-supported transport; no raw credential/exception/body/header logging. Remove obsolete transport paths rather than keep a silent urllib fallback.
2. Preserve exactprofile resolution (table-overlay versusstandalone-complete, providerlookup), env_key precedence versusexperimental_bearer_token, modelcatalog and CLIoverrides; --profile defaults ~/.codex and does not inherit strategicCODEX_HOME. Bare/model and/thinking print numberedmenu and readnextline; directforms conveniences, /quit/EOF/CtrlC/invalidselection behavior preserved. Thinking affects MAIN requests only, never proxy reviewer. Plainappend-only stdout, no ANSI/cursor/alternate screen/spinner, unsafe endpointcontrols renderedvisibly. Stream actualserver reasoning deltas promptly and once; support SDK terminal/done/item representations, including unknownextension fields retained by SDK model_extra/to_dict equivalents. No answer regenerating, no invented thinking/original.
3. Preserve one main request per prompt, zero for menus; only completed successfuldeliveredtext enters in-memoryhistory (no reasoning/original/extension/failedpartialturns). JSONfallback if serveractuallyreturnsJSON remains honest; malformedAPIJSON/failed/incomplete/queued/in_progress/EOF/cancellation not accepted as completed. SDK owns standardSSE framing; exercise actualSDK parser/typed event boundary rather than fakecopies. Do not retain a custom weaker parser or fake completions. Keep known original extension version/index/text validation and exact missing message `Original response was not provided.`; approved final metadata may change only onserver. No default retries to hide failures.
4. Make errors useful while secret-free. Known observed gatewaycode responses_stream_event_not_supported must be reported as a fixed recognized diagnostic with a clear endpoint failure description, instead of only generic responsefailed. HTTPstatus and fixedknownerrorcategories are safe. Never print arbitrary exceptionstr, servermessages/errorcodes/headers/requestID that could contain a token or usertext; use narrow recognizedcodes/types/fixedmessages with safeunknownfallback. Preserve no-success/no-history semantics. Authentication401/403, transporttimeout, malformedstream and serverfailed are distinct.
5. Qualify against UNCHANGED livegateway using preferred RTX3090 repaired and unrepaired modelIDs. Start with smallest valid SDK text request and compare actual supported Codex request contract, one field at a time where necessary. No dummytool/falseidentity/encryptedblob to bypass validation, no authority weakening, no fixing gateway or substituting fake service as liveproof. Keep reasoning selection functional and streaming visible; no silent loss ofthinking. If a proposed valid text-only request cannot satisfy existinggatewaycontract, preserve exact synthetic request structure (no key/rawresponse) and concretefailurecategory, present smallest viable client alternative tostrategy before substituting/removing requested behavior. Explicitly correct overstrong diagnoses if new observations differ. Do not claim COMPLETE for livefailure merely because the SDKalsofails.
6. Meaningful actual CLI+SDK+ownedHTTP tests preserve established menu/profile/request/header/history/Unicode/original/terminal/error/redirect/cancellation/barrier coverage. Fakes go behind the SDK HTTP boundary, not replacementSDK/eventparser. Add focused actualSDK regressions for unknown terminalnamespace preservation and recognizedgatewayfailure surfacing; test errorfield carrying syntheticcredential neverechoed. Verify rawserver reasoning flushes while actualHTTPserver holds finalcompletion, valid completedoriginaltext/index binding, no extraAPIcall/history afterfailure. Reuse meaningful existingtests; update only transport-specific premises to actual SDKcontract with recordedreason. No new tests merely mirroring dependencyconstants. Ruff/strictmypy scopedclient/test files, actual candidate-review/sharedchecks and both literalwhitespacebases. No long local fullbaseline duplicate; CI completebaselinehandles broader regressions.
7. Guide/install/testing instructions use actualSDK dependency installation and workingprofile command, numberedmenus and responseexamples; removestdlib-only claim. Explain actualtestedgatewayrequestmode and limitations accurately, no hostedservice/fullCodexcompatibility or linguisticquality claim. Root personally reads guide and actualplainterminaloutput. Counter107to108 pairedwithone newreport; finalreportfiles109/machine108 afterknown007-oexclusion, registry42/incidents2 unchanged. Record expectedpre-reportcounter mismatch honestly, never weaken count validation.
8. Normal one-PR forward publication, actual nonreport I/head observation, truthful PRIVATE report and unpublishedpreflight while canonicalabsent; exactcopy, sole-report SELF parentliteralI, push, normalREMOTEverify andONEresponseOK thenidle. Normalpublication already authorized; no additional rootdraftapprovalpause. Requiredall4FINAL-head CI green plus independentstrategyreview beforeexpected-headdevelopmentmerge. No futurepush/CI/merge claim in prewrittenreport. If livecompatibility remains unresolved, resultmustremaintruthfullyPARTIAL/BLOCKED with exactremainingcriterion; publicationdoesnotestablishacceptance.

## Acceptance criteria

A1 officialSDK is actualtransport/parser, reproduciblyinstalled/pinned, no manualfallback/retries or core runtimeupgrade. A2 existingUX/config/menu/mainthinking/original/history/privacy/cancellation semantics pass actualCLI+SDK+HTTPboundary tests, includingearlythinkingbarrier andsafeerrorcodes. A3 actualRTX3090 repaired+unrepaired successfullivecompletion and expectedoriginal/fixedmissing behavior, sourceboundcontent-free evidence; any livegapclearlyUNPROVEN, notpassed. A4 accurateuserguide/plainterminalpersonalreview and scopedstatic/sharedchecks; all4finalCI includingfull13driver+cleanup pass beforeguardeddevelopmentmerge/main/privateacceptedrefresh byRoot. No deployment or formalMVP/humanqualitydataset.

## Verification

QualifiedPython3.12 with pinnedofficialSDK inclient/devgroup; ownnative0700tempdirs/0600files, finiteprocesses/HTTPservers, no realcredential fixtures. Relevant actualclientcontractmodule, focused SDKboundary tests and minimalmeaningfuldependencybaseline regression ifchangedmetadatarequiresit. Ruff/strictmypy client/newtests; source/runtimeclosure equality, uvlockcheck, actualcandidate-review, transcript/reporthistory, both check_whitespace authorizedbases2832fa1e51bdf3641aabbd81feab8ddb64a876da and82ea1e6f4173934fa47bb34ee6a6f78338d3603a. No full localbaseline replay for process/dependencymetadata alone. Existing finalCI checks: Application baseline; Research reproducibility; OAP bootstrap acceptance; OAP report history, unchanged40/60 budgets. Missing/pending/cancelled blocksmerge. Root separatelyvalidates actualSDKliveintegration; reportcannotreuse oldsyntheticpasses as liveproof.

## Local setup and constraints

Owner explicitly authorizes installingneededdependencies; isolatedPythonvenv/uvgroups/pinnedlock setup isallowedwithoutanotherquestion. Onlyexistingprimaryforeground session via normaltmux/FIFO, OAP_ROLE=coding explicit. Retain owner-launchedhome/auth/profile/model/effort/cwd; exactrecordedrestartargv onlyifactuallydead, no automaticrestart. No backgroundagent/subagent/codexexec. LiveAPI requests viaPythonSDK are not codingagents; use onlyRTX3090 asownerpreference, noA100/V100targetcalls. Standing1,000,000additionalRTX3090requests/tenpublicationrecoveries authorization persists; no repeatedpermission. Start small/content-free/finitecalls, record requestcounts and evidence; do not turn allowances into unnecessarytraffic. Protect realservices/corpus/key/privatehistory/untrackedwork. No rawcustomer/review or credentialdata in public/OAPartifacts. Synthetic mainprompts only, generatedoutputnotpersisted except explicitlycontrolledprivatevisualinspection; aggregatefacts forreports.

## Documentation

examples/README primarySDKhow-to; INSTALL/TESTING onlyfocused invocation/setup changes ifneeded, exactaffectedinventoryhashes. Showserverexposedthinking, delivered+originalorfixedabsence, numberedmenu nextline. Actuallivequalification and softwaretestproof explicitlyseparated. No stdlib-only, universalcompatibility, deployment or qualityoverclaim.

## Git and report publication

Rootpublishesexact043-a/activeastwo writesandactivation-onlycommitonnewbranchfrom709dc79; signalsseparatelynormalforegroundhandoff. CodingimplementsonecoherentSDKclientPR, preservesall042artifacts, pushesnonreportI/freshPRbeforecomposingreport, normalprivatepreflight/soleSELF/push/REMOTE/ONEOKthenidle. Nevermerge/automerge or rewritehistory. Rootindependentcode/test/docs/liveproof/finalheadCI/effects/riskreview andguardedexpectedSHAdevelopmentmerge, thenverifiedmain/localFF/privatebacked-upacceptedrefresh. No deployment.

## Decision classification

D0 explicitownerSDKclientfix. Strongestcounterargument: SDKmigrationmayadddependencieswithoutsolvinggatewayrequestcontract and maysilentlydropunknownoriginalfields or introduce retries/redirects. MitigateactualtypedSDKboundary tests, pinnedclientgroup, preservedruntimeclosure, explicitzero retries/refusedredirects/unknownfieldtests, actuallivequalification onunchangedgateway beforefixclaim. No unsupportedfakecapabilities or loweredprotection. Ordinaryfailedintegration/dependencyworkdoesnotcreateDHAdebt.

## Deferred human adjudication

- Decision: NONE

## Metadata

```oap-metadata
{
  "id": "043-a",
  "title": "Use the official OpenAI Python SDK for the comparison client and qualify the existing gateway",
  "objective": "043",
  "status": "FINAL",
  "phase": "SDK_CLIENT_COMPATIBILITY",
  "repository": "ulfe-lmi/slaif-llm-response-language-repair",
  "default_branch": "main",
  "base_sha": "709dc799950a7a7ed56314560fc033e1129b9e50",
  "branch": "oap/043-openai-sdk-client",
  "pr_mode": "CREATE_NEW_PR",
  "pr": null,
  "dependencies": [
    "042"
  ],
  "local_work": "Tracked main clean at709dc79; preserve5152untrackedfiles under three roots, ignoredkey600, oldbranches/worktrees/privatehistory and liveforegroundsessions.",
  "prior_review": "Objective042/PR48 merged at709dc799950a7a7ed56314560fc033e1129b9e50 after all4finalCI/source/docs/negative review, localmain/privateaccepted refreshed with backups. Currentowner livefailure reproduced; initial042 claims are software/syntheticHTTP proof, not livegatewayqualification. No openPR observed. Existingprimary foreground idle.",
  "provenance": [
    {
      "kind": "H",
      "reference": "Owner reports client failure even on unrepaired models, directs fix client while gateway unchanged, explicitly requires OpenAI Python library and authorizes installing whatever needed. Preserve requestedCodexprofiles/catalogs, numberedmodel/thinkingmenus, plainterminalscrollback/original+delivered display."
    },
    {
      "kind": "A",
      "reference": "Accepted Responses/Pandoc/proxy contracts unchanged; caller-only clientSDK migration. SameforegroundOAP publication/independentfinalheadreview/all4CI/developmentmerge. No gateway/provider/service mutation."
    },
    {
      "kind": "E",
      "reference": "Root verified exactlocalearly-beta TOML bearer resolution andGETmodelsHTTP200. Repaired andunrepairedRTX3090 request shapes getHTTP200 thenresponse.failed/responses_stream_event_not_supported beforeanswer; addedinclude/summary/context variants do notfix. OfficialOpenAI3.60.0 installed privately; SDK inputlist reproduces samefailure, SDKstringinputgets400. Thisprovesneedactualrequestqualification, notthatSDKalonefixesupstreamvalidation."
    },
    {
      "kind": "I",
      "reference": "Root read-onlygateway source associateserror withResponsesStreamEventValidator rejection; ordinarytext versusCodexstreaming profiles dependonrequestshape/key/serverpair. No rejected rawupstreamevent observed and deployedgatewayrevision notestablished; avoidoverstatingcause. Do not manufacturedummytools/replay/session toactivategates."
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
  "governance_ref": "709dc799950a7a7ed56314560fc033e1129b9e50",
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
