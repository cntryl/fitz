# Fitz UI/UX Scorecard Campaign

## Goal

Review each operator-facing Fitz UI page for usefulness and usability, then
improve the experience using source-backed findings. Jev provides typed scoring
signals to prioritize review; it does not establish actual usage, validate a
visual interaction, or prove a defect. Every finding needs corroboration from
the rendered UI, source, a test, or operator evidence before it becomes a
change.

## Scope

Review authenticated operator pages and their meaningful route states:

| Page group | Page types to score | Route/state coverage |
|---|---|---|
| Overview | Broker status, active issues, broker vitals | Selected Route Family; healthy, issue-present, partial/unavailable data |
| Sessions | Active session summary and table | Empty and populated results; loading/error where practical |
| Diagnostics | Operational diagnostics, search, topology and internal views | Empty and populated search; refresh; partial/stale/unavailable data |
| Metrics | Broker metrics and drill-down content | Empty/populated data; expanded/collapsed sections; refresh/error |
| Domain inventory | Stream, KV, Schedule, Queue, Lease, Notice, RPC | Family-wide, realm, area, resource scopes supported by each domain |
| Resource detail | Resource-specific history/state/work/ownership/live-flow views | Empty and populated; long values and pagination where supported |
| Operation detail | Schedule, Notice, and RPC operation views | Representative operation state, missing/empty results, and available actions |
| Navigation and shell | Route Family selector, sidebar, responsive menu, breadcrumbs/actions | Desktop/mobile, direct links, family switching, active-route indication |
| Authentication | Login and logout | Validation errors, pending state, unauthenticated redirect, return navigation |

Legacy aliases and selector-only routes are assessed as navigation behavior, not
counted as duplicate content pages. Do not infer the opaque `realm` from
RouteFamily; where both are visible, assess whether their independent meanings
are clear.

## Jev scorecard

Rubric version 2 is used by the initial scored run recorded in
[`ui-ux-scorecard-results-2026-09-30.json`](ui-ux-scorecard-results-2026-09-30.json).

Provide Jev a compact, named state for one page type at a time: page purpose,
intended operator/task, route scope, visible content/actions, relevant UI
states, and source excerpts or screenshots. Batch these independent `Score`
questions over the same state. Use the following ordered levels (0–3) for each
dimension; each question measures one dimension.

| Dimension | 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| Task usefulness | Does not support a recognizable operator task | Supports a rare or indirect task with substantial work elsewhere | Supports a clear task with some gaps or detours | Directly supports an important task end to end |
| Decision value | Provides no useful basis for an operator decision | Adds context but leaves the decision unclear | Supports a decision with limited ambiguity | Clearly guides a timely, appropriate operator decision |
| Actionability | No practical next step is available | Next step must be inferred or found elsewhere | Useful actions or links exist but have gaps | Findings lead directly to an appropriate next step |
| Distinct value | Duplicates other UI with no meaningful benefit | Mostly overlaps; only a small unique benefit | Adds a useful distinct view or workflow | Provides a uniquely valuable capability |
| Clarity | Purpose, scope, or signals are materially confusing | Several labels or relationships require inference | Purpose is clear with a few points of friction | Purpose, scope, and signals are immediately understandable |
| Operational trust | States can mislead about freshness, completeness, or availability | Important data caveats are easy to miss | Most caveats and data states are clear | Data freshness, completeness, and failure states are explicit and dependable |

Ask a separate yes/no judgment for whether critical scope confusion exists
(especially Route Family versus `realm`), and whether a plausible serious
accessibility barrier blocks a core task. These are escalation signals, not
weighted away by high scores in other dimensions.

Do not ask Jev to estimate traffic, operator frequency, completion rate,
accessibility conformance, screen-reader behavior, or actual usability from
source text alone. Capture those with analytics, browser inspection,
accessibility tooling, or operator feedback as appropriate.

## Per-page record

For every page type, record:

- Intended operator and task; why this page exists.
- Jev model, timestamp, page-state input summary, score, level probabilities,
  and confidence for each dimension.
- Critical-scope and accessibility-escalation judgments.
- Independent evidence: source location, route/state inspected, test result,
  screenshot or operator observation.
- Finding, impact, proposed change, and disposition: fix, keep, investigate,
  or remove candidate.
- Recheck evidence after a change. Keep observed facts distinct from Jev
  hypotheses and do not turn confidence into correctness.

## Campaign sequence

1. Establish the route/page inventory and working-tree baseline.
2. Review the shell and Overview first, then Sessions, Diagnostics, and Metrics.
3. Review each of the seven domain overview pages; compare like route scopes
   and preserve each domain's distinct guarantees and vocabulary.
4. Review resource and operation detail pages, including empty and error states.
5. Inspect responsive layouts and keyboard/accessibility behavior across the
   route matrix; record usability defects separately from usefulness scores.
6. Verify every proposed finding against code and rendered behavior. Implement
   focused fixes, add or update regression coverage, and repeat affected checks.
7. Re-score changed pages with the same rubric and compare old/new evidence.
   Close only when every in-scope page has a record and no confirmed blocker or
   unresolved high-impact finding remains.

## Jev request contract

- Model: `jev-latest`.
- One request per compact page state; independent dimension questions may be
  batched together.
- Keep each question atomic and use the same concrete, standalone scale on
  every page so scores can be compared.
- Preserve the full probability distribution and confidence; never report a
  composite as if Jev produced it. If a summary score is later useful, define
  normalization and weights explicitly in code and retain the raw scores.
- If the state is too large, split by page/route type. Do not omit relevant
  source behavior merely to fit a broad prompt.
- Do not claim Jev review completion without an observed API response.

## Completion gate

The campaign is complete when every in-scope page type has a scorecard and
independent evidence, confirmed high-impact defects have fixes and regression
coverage, and responsive/accessibility/browser review has been recorded. A
green unit or build suite alone does not establish visual or interaction
quality.

## Initial campaign run: 2026-09-30

Status: Jev scoring and implementation pass completed; operator research and
hands-on assistive-technology review remain open.

- Jev: 25 named page and flow states scored with model `jev-1.13.0` using
  rubric version 2. Raw per-dimension answers, probability distributions,
  confidence, and escalation judgments are retained in the linked results
  JSON. These scores are prioritization signals only.
- Source and UI corroboration: Overview already had a generated timestamp in
  its data model, but the page did not render it. The UI now shows the snapshot
  time with a relative label; a regression test covers the output. The mock
  API's fixed June timestamp was also changed to current generated times so
  its “healthy” state does not look months stale.
- Scope clarity: Jev marked Diagnostics as the strongest scope-confusion
  candidate. Inspection confirmed selected Route Family context was explained
  in-page, while breadcrumb values were unlabeled. Breadcrumbs now explicitly
  label `Route Family`, `Realm`, `Area`, and `Resource`, with a regression test
  checking those distinctions.
- Other low or uncertain dimension scores remain investigation leads, not
  confirmed defects. In particular, Stream resource actionability had very
  low confidence; it needs operator/task evidence before any workflow change.
- Accessibility: Jev did not establish an actual accessibility barrier.
  Automated route checks and responsive coverage pass, but keyboard and
  screen-reader review, accessibility tooling, and operator frequency/task
  evidence are still pending.
- Validation after changes: 184 unit/component tests passed; lint, type check,
  and production build passed; all 280 Chromium E2E cases passed. The E2E suite
  covers the route/theme/viewport matrix; manual screen-reader review is not
  represented by these results.
- This is a completed initial campaign pass, not a claim that every design
  decision is optimal or that all accessibility needs have been tested.
