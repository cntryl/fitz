# Fitz Admin Troubleshooting Roadmap

## Purpose

Build the admin portal around one job: help a user understand how messages are flowing, where they are stuck, and what likely explains the current state. The UI should be useful to operators first, and the same troubleshooting surface should later be exposed to MCP so AI agents can assist safely using the same bounded data.

This roadmap is intentionally troubleshooting-first, not dashboard-first.

## WIP Status

This document is now a working tracker. Use it to separate what is already in the codebase from what is still pending.

| Phase                                            | Status      | What that means right now                                                                                                           |
| ------------------------------------------------ | ----------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Phase 1: Troubleshooting Questions And Taxonomy  | Done        | Canonical diagnosis labels, severity vocabulary, and explanation-hint scaffolding already exist in the admin troubleshooting layer. |
| Phase 2: Shared Troubleshooting Read Model       | Done        | The read model already builds diagnostics, hotspots, incident summaries, comparison metrics, and bounded timelines.                 |
| Phase 3: Add Missing Diagnostic Signals          | In progress | Most domain-specific counters and age/trend fields are wired, and the remaining gaps are now concentrated in the last RPC, Lease, and notice edge cases. |
| Phase 4: Build The UI Around Troubleshooting     | Todo        | The portal pages and drilldowns still need to be built around the troubleshooting model.                                            |
| Phase 5: Explanation And Guidance Layer          | Todo        | Explanations are still limited to hints; the rule-based guidance layer is not complete yet.                                         |
| Phase 6: MCP Exposure                            | Todo        | MCP tooling has not been exposed yet.                                                                                               |
| Phase 7: Hardening, Semantics, And Test Coverage | Todo        | The troubleshooting flow still needs contract, parity, and regression coverage.                                                     |

### Current Done List

- Canonical diagnosis labels and diagnostic snapshots are implemented in the admin troubleshooting module.
- Incident summaries, top bottleneck selection, and last-significant-transition tracking are already built.
- Bounded resource timelines exist for KV, queue, stream, lease, notice, RPC, and schedule.
- Comparison helpers already surface age, failure, contention, and transition metrics for resource drilldowns.
- Queue now exposes redeliveries, DLQ transitions, complete-rejects, backlog age distribution, and notify-drop counters in stats and metrics.
- Queue delay-age buckets now flow through the read model, diagnostics, stats, and Prometheus metrics.
- Stream and schedule request-latency histograms are implemented and consumed by the troubleshooting layer.

### Current Todo List

- Finish the remaining RPC, Lease, and notice Phase 3 diagnostic counters, then move on to the next phase boundary.
- Add the troubleshooting-first UI pages, timeline views, compare view, and explanation card layout.
- Turn the explanation hints into a deterministic guidance layer with confidence and next-query suggestions.
- Expose the same bounded troubleshooting model through MCP.
- Add parity, regression, and semantics tests for durable versus ephemeral labeling.

## Current Baseline

The existing admin API already provides a strong starting point:

- global broker and domain stats
- per-realm, per-area, and per-resource drilldowns
- queue backlog, inflight, dead-letter, and age data
- stream offsets, watermarks, sizes, and live append-session counts
- notice subscriptions and routes
- RPC workers and pending requests
- lease counts
- schedule counts and live handoff state
- Prometheus metrics for the main broker and domain-level counters

The current gaps are mostly diagnostic, not structural:

- weak causal tracing across domains
- missing transition counters for why state changed
- missing contention and wait-depth signals in RPC and Lease
- missing age distributions and growth trends for backlog surfaces
- missing a unified explanation layer that turns raw data into a troubleshooting story

## Product Principle

The portal should answer five questions quickly:

1. What is healthy?
2. What is stalled?
3. Where is the backlog or contention?
4. What changed recently?
5. What is the most likely explanation?

If a screen cannot answer at least one of those questions, it probably belongs in a lower-priority admin surface.

## Target Experience

The troubleshooting experience should have three layers:

1. Summary layer
   - broker health
   - per-domain hotspots
   - top blocked resources
   - recent failures and transitions

2. Drilldown layer
   - realm -> area -> resource
   - resource -> session / worker / subscription / request / claim
   - current counts plus recent trend

3. Explanation layer
   - likely bottleneck
   - likely owner or holding stage
   - recent change that triggered the problem
   - recommended next action or next query

## Shared Data Model

Create a normalized troubleshooting model that every surface can use. It should be the source for the UI and for MCP.

### Core entity types

- broker snapshot
- realm snapshot
- area snapshot
- resource snapshot
- live session or worker snapshot
- recent event timeline entry
- contention snapshot
- failure snapshot
- explanation snapshot

### Required fields by default

The model should include, where applicable:

- identity: domain, realm, area, resource, family, session, correlation, operation
- state: active, pending, delayed, inflight, dead-lettered, delayed, claimed, failed, released
- counts: backlog, inflight, ready, delayed, dead letters, waiters, workers, subscriptions
- time: age, last change, last success, last failure, throughput window
- ownership: owner session, worker session, holder, claimant, current route family
- trend: growing, shrinking, steady, stalled
- explanation hints: likely bottleneck, likely cause, recent transition

### Important rule

Do not expose only raw counters. Every useful count should be paired with at least one time or trend signal when the domain can support it.

## Roadmap

### Phase 1: Troubleshooting Questions And Taxonomy

Goal: define the exact diagnostic questions the product will answer.

Deliverables:

- a portal troubleshooting taxonomy
- a canonical diagnosis label set used by `current_stage`
- domain-by-domain question list
- severity and priority definitions
- the first set of explanation templates

Suggested questions:

- Is this a throughput problem, a contention problem, or a data-loss problem?
- Which domain is the bottleneck?
- Is the backlog growing or stable?
- Is a single resource hot or is pressure spread across a realm?
- Is work waiting for a worker, a lease, a subscription, or a durable handoff?
- Did this begin after a restart, disconnect, or route change?

Implementation tasks:

1. Define a canonical troubleshooting vocabulary for all domains.
2. Map each domain to the questions it can answer today versus after enhancements.
3. Define severity levels for flow issues, contention, backlog growth, and stale state.
4. Define the portal labels for ephemeral, durable, and mixed-state views.
5. Write the explanation templates that the UI and MCP will reuse, including the bounded next query and durability wording.

### Phase 2: Shared Troubleshooting Read Model

Goal: normalize the existing admin snapshot into a reusable diagnostic model.

Deliverables:

- a troubleshooting snapshot service built on the admin read model
- resource timeline aggregation
- trend calculation for backlog and throughput
- top-offender and hotspot computation
- domain-specific explanation hooks

Implementation notes:

- keep this read-only
- keep it bounded and cheap
- do not turn it into a query engine
- prefer rolling windows and coalesced snapshots over deep history scans

Implementation tasks:

1. Add a troubleshooting snapshot builder on top of the existing admin read model.
2. Normalize each domain into a shared resource summary shape.
3. Add rolling-window trend computation for counts and rates.
4. Add hotspot ranking so the UI can surface the likely problem first.
5. Add recent-transition aggregation for each resource and realm.
6. Add a summary layer for broker-wide state and per-realm state.
7. Add a bounded timeline model that can be reused by the UI and MCP.

### Phase 3: Add Missing Diagnostic Signals

Goal: fill the gaps that prevent the UI from explaining flow.

Priority additions:

- queue: redelivery count, delay age buckets, dead-letter transition count, backlog age distribution, complete/reject counters
- rpc: timeout count, backpressure count, wrong-worker count, wrong-correlation count, late-response drop count, wait-depth or pending-by-route details
- lease: waiter depth, timeout count, invalid-token count, forced-release count, ownership churn count
- stream: lag distribution, append-session churn, replay/read latency indicators, conflict or rejection counters if missing
- schedule: overdue normalization count, persistence failure count, claim retry count, fire latency or age breakdown
- notice: unsubscribe churn, delivery failure count, route concentration, notification drop count if applicable

Implementation tasks:

1. Inventory every missing signal against the current admin read model and metrics collector.
2. Decide whether each missing signal belongs in JSON admin API, Prometheus, or both.
3. Add backlog-age and retry-related counters to Queue.
4. Add timeout, backpressure, late-response, and wrong-correlation counters to RPC.
5. Add waiter-depth and ownership-failure counters to Lease.
6. Add lag, conflict, and churn counters to Stream.
7. Add overdue normalization and persistence-failure counters to Schedule.
8. Add unsubscribe and delivery-failure counters to Notice.
9. Add labels or dimensions only where they help a user isolate a problem without exploding cardinality.

### API Enhancements Needed For Phase 3

Add or extend the following endpoints and fields before the troubleshooting UI is considered complete:

- `GET /api/v1/stats`
  - add a top-level `diagnostics` section with current incident summary, top bottleneck, and last significant transition time
  - add per-domain trend fields such as `trend`, `delta_5m`, or `delta_1h` where cheap

- `GET /api/v1/{domain}/stats`
  - add domain-specific failure and contention counters
  - add backlog or ownership breakdowns that explain why the counts are high

- `GET /api/v1/{domain}/realms/{realm}/areas/{area}/resources/{resource}`
  - add recent change metadata, last activity timestamps, and trend indicators
  - add a `diagnostics` block with the likely bottleneck and explanation hints

- `GET /api/v1/{domain}/realms/{realm}/areas/{area}/resources/{resource}/events`
  - return recent transitions, failures, retries, ownership changes, and state flips
  - keep the window bounded, such as the last N events or last M minutes

- `GET /metrics`
  - add missing counters and histograms that support the same troubleshooting story
  - include age histograms where current counts alone are not enough

Suggested diagnostic fields:

- `last_changed_at`
- `last_success_at`
- `last_failure_at`
- `current_stage`
- `likely_bottleneck`
- `trend`
- `age_seconds`
- `backlog_age_buckets`
- `recent_transition_count`
- `failure_count`
- `contention_count`
- `waiter_count`

API design rules:

- expose enough detail to explain the problem, but not unbounded history
- keep ephemeral and durable state explicitly separate
- prefer coalesced read-model snapshots over live scans of actors
- do not add API shapes that imply durable recovery unless the backend truly supports it

### Phase 4: Build The UI Around Troubleshooting

Goal: make the portal answer diagnostic questions before it becomes a reporting tool.

Recommended pages:

- overview: broker health, hot domains, active incidents, recent regressions
- realm page: flow summary, dominant domain, hot resources, live contention
- resource page: current state, backlog, inflight, age, recent changes, live actors
- event timeline: recent notable transitions and failures for one resource or realm
- comparison view: before/after snapshots for a time window or release change

Recommended UI behaviors:

- highlight the most likely bottleneck first
- show trend arrows and age indicators alongside counts
- distinguish durable state from current-process state visually
- surface a short explanation before the raw table
- let the user jump from a summary card directly to the most relevant drilldown

Implementation tasks:

1. Create an overview page that leads with broker health and active incidents.
2. Create realm and resource drilldown pages with consistent layout and shared navigation.
3. Add a timeline panel for recent state changes, retries, failures, and ownership flips.
4. Add trend badges and age indicators next to every count that can move over time.
5. Add a compare view for before/after snapshots so users can link a regression to a deployment or traffic shift.
6. Add filters for realm, area, resource, route family, and domain.
7. Add an explanation card that summarizes the likely cause before the raw data table.
8. Make ephemeral-versus-durable state visually explicit in the component hierarchy.

### Phase 5: Explanation And Guidance Layer

Goal: turn snapshots into operator guidance.

Deliverables:

- derived diagnosis labels such as backlog growth, worker starvation, lease contention, dead-letter pressure, or stale handoff
- rule-based explanation templates
- suggested next drilldowns
- optional remediation hints where the system is confident

Guardrails:

- explanations must be derived from data already exposed in the control plane
- explanations must never change system behavior
- if the model is unsure, say so explicitly

Implementation tasks:

1. Define the explanation schema returned by the troubleshooting service.
2. Implement rule-based diagnosis for the first set of known bottlenecks.
3. Add confidence or certainty fields so the UI can avoid overclaiming.
4. Add recommended next queries or drilldowns.
5. Add a domain-specific explanation table for queue, rpc, lease, stream, schedule, notice, and kv.
6. Add tests that prove the explanation layer stays read-only and bounded.

### Phase 6: MCP Exposure

Goal: expose the same troubleshooting surface to AI agents.

Tool family proposal:

- summarize broker health
- inspect realm or resource flow
- inspect recent transitions for a resource
- inspect contention for a resource or operation
- inspect dead letters or pending requests
- explain likely cause of a stall
- compare two snapshots or time windows

MCP rules:

- use the same underlying read model as the UI
- keep tools bounded and paged
- require the same realm and resource scoping as REST
- make `explain` a thin interpretation layer over facts, not a freeform planner
- do not expose unbounded scans or arbitrary analytics

This should stay aligned with the existing MCP safety model in [docs/roadmap/mcp-tools.md](mcp-tools.md).

Implementation tasks:

1. Define the MCP tool contract for the troubleshooting model.
2. Expose read-only summary tools first.
3. Expose resource drilldown tools second.
4. Expose bounded event and timeline lookup tools third.
5. Expose explanation tools last, and keep them deterministic.
6. Reuse the same authorization and scoping checks as the REST admin API.
7. Add audit events for every tool call.
8. Add per-tool cost limits and page limits.
9. Add tests that compare MCP output to REST output for the same scope.

### API Enhancements Needed For Phase 6

To support MCP safely, the admin API should expose stable, bounded, tool-friendly views:

- a broker summary endpoint with a compact troubleshooting payload
- a resource diagnostics endpoint with counts, age, trend, and explanation hints
- a resource events endpoint with bounded recent transitions
- a resource comparison endpoint for two snapshots or time windows
- a consistent pagination model for any list-style diagnostic output

These should be the same underlying read models used by the UI, not separate AI-only data sources.

## Sprint Breakdown

Assume 2-week sprints. The order matters: first define the troubleshooting contract, then build the shared read model, then extend the API, then wire the UI, then expose MCP, then harden everything with tests and semantics checks.

### Sprint 1: Troubleshooting Contract And Vocabulary

Goal: define the language of troubleshooting before implementing more surface area.

Work items:

1. Write the canonical troubleshooting vocabulary for every domain.
2. Define flow states, contention states, durable states, ephemeral states, and mixed-state labels.
3. Write the initial explanation templates and diagnosis labels.
4. Map each domain to the questions it can answer now versus after API changes.
5. Define the severity model for backlog growth, starvation, dead-letter pressure, lease contention, and stale handoff.
6. Decide the first cut of portal pages and the user questions each page must answer.

Dependencies:

- existing admin read model
- existing stats endpoints
- existing domain docs and caveats about durable versus live state

Exit criteria:

- there is one shared vocabulary for the whole product
- the team can explain what counts as a bottleneck, a stall, or a transient live-state artifact
- the troubleshooting model has enough shape to drive implementation

### Sprint 2: Shared Troubleshooting Read Model

Goal: build the reusable diagnostic snapshot layer the UI and MCP will both consume.

Work items:

1. Add the troubleshooting snapshot service on top of the existing admin read model.
2. Normalize broker, realm, area, resource, session/worker, and event data into one diagnostic shape.
3. Add rolling-window trend computation for counts and rates.
4. Add hotspot ranking so the UI can highlight the most likely problem first.
5. Add recent-transition aggregation for retries, failures, ownership changes, and state flips.
6. Add a bounded timeline model that can be reused by the portal and MCP.
7. Add synthetic fixtures for healthy, stalled, and degraded states.

Dependencies:

- Sprint 1 vocabulary and explanation schema
- admin read model snapshots
- stable resource identity across domain views

Exit criteria:

- one read-only service can summarize broker, realm, and resource scope
- the snapshot includes trend, age, ownership, and recent-change data
- timeline data is bounded and safe to reuse in higher layers

### Sprint 3: API Enhancements And Diagnostic Signals

Goal: expose the missing signals needed to explain message flow.

Work items:

1. Extend `/api/v1/stats` with diagnostics such as top bottleneck, incident summary, and last significant transition.
2. Extend per-domain stats with failure, contention, and growth signals.
3. Extend per-resource endpoints with last-change metadata, trend indicators, and explanation hints.
4. Add bounded resource event endpoints for recent transitions, retries, ownership changes, and failures.
5. Add snapshot comparison endpoints for before/after troubleshooting.
6. Add the missing Prometheus counters and histograms for age, retry, timeout, contention, and stall signals.
7. Update OpenAPI so the portal and MCP can bind against one stable contract.

API fields to add or standardize:

- `last_changed_at`
- `last_success_at`
- `last_failure_at`
- `current_stage`
- `likely_bottleneck`
- `trend`
- `age_seconds`
- `backlog_age_buckets`
- `recent_transition_count`
- `failure_count`
- `contention_count`
- `waiter_count`

Domain-specific signal priorities:

- queue: redelivery count, delay age buckets, dead-letter transition count, backlog age distribution, complete/reject counters
- rpc: timeout count, backpressure count, wrong-worker count, wrong-correlation count, late-response drop count, wait-depth
- lease: waiter depth, timeout count, invalid-token count, forced-release count, ownership churn count
- stream: lag distribution, append-session churn, conflict/rejection counters, replay/read latency indicators
- schedule: overdue normalization count, persistence failure count, claim retry count, fire latency or age breakdown
- notice: unsubscribe churn, delivery failure count, route concentration, notification drop count

Dependencies:

- Sprint 2 read model
- domain-specific sink and metrics hooks
- API contract review against durability boundaries

Exit criteria:

- the API can explain what changed and what is stuck, not just how many objects exist
- the portal has enough data to drive an explanation-first UI
- the MCP layer has bounded facts to consume later

### Sprint 4: Troubleshooting UI Foundation

Goal: make the portal answer the first diagnostic questions quickly.

Work items:

1. Build the overview page with broker health, active incidents, hot domains, and top blocked resources.
2. Build realm drilldown pages with consistent flow summaries.
3. Build resource drilldown pages with current state, backlog, inflight, dead letters, age, and trend.
4. Add filters for realm, area, resource, route family, and domain.
5. Distinguish durable state from current-process state in the visual hierarchy.
6. Surface explanation cards before raw data tables.
7. Make the likely bottleneck visually obvious on every summary surface.

Dependencies:

- Sprint 2 read model
- Sprint 3 API fields and endpoints
- UI layout decisions from the troubleshooting vocabulary

Exit criteria:

- a user can open the portal and immediately see where pressure is concentrated
- the portal clearly separates durable vs live state
- the portal can guide a user from overview to the correct resource

### Sprint 5: Timeline, Comparison, And Explanation Layer

Goal: turn snapshots into actual troubleshooting guidance.

Work items:

1. Add the recent-event timeline panel for state changes, retries, failures, and ownership flips.
2. Add compare mode for before/after incident inspection.
3. Add trend badges and age indicators beside every moving count.
4. Add domain-specific explanation views for queue, rpc, lease, stream, schedule, notice, and kv.
5. Add suggested next drilldowns so the UI can guide the operator.
6. Implement rule-based diagnosis for known bottlenecks and contention patterns.
7. Add confidence or certainty fields so the UI does not overclaim.

Dependencies:

- Sprint 3 event and comparison endpoints
- Sprint 4 UI foundation
- explanation schema from Sprint 1

Exit criteria:

- the portal can explain a likely cause, not just show counts
- users can see recent transitions and compare two points in time
- the UI can describe whether the issue is growing, stable, or recovering

### Sprint 6: MCP Surface And Safety Controls

Goal: expose the same troubleshooting surface to AI agents without widening access.

Work items:

1. Define the MCP tool contract for the troubleshooting model.
2. Expose read-only summary tools first.
3. Expose resource drilldown tools second.
4. Expose bounded event and timeline lookup tools third.
5. Expose explanation tools last and keep them deterministic.
6. Reuse the same authorization and scoping checks as REST.
7. Add audit events for every tool call.
8. Add per-tool cost limits and page limits.
9. Add parity tests comparing MCP output to REST output for the same scope.

Tool families:

- summarize broker health
- inspect realm or resource flow
- inspect recent transitions for a resource
- inspect contention for a resource or operation
- inspect dead letters or pending requests
- explain likely cause of a stall
- compare two snapshots or time windows

Dependencies:

- Sprint 2 read model
- Sprint 3 API contract
- Sprint 5 explanation layer
- MCP policy model already documented in [docs/roadmap/mcp-tools.md](mcp-tools.md)

Exit criteria:

- an AI agent can troubleshoot with the same bounded facts as the UI
- MCP remains strictly scoped and cost-bounded
- MCP output matches REST semantics for the same resource scope

### Sprint 7: Hardening, Semantics, And Test Coverage

Goal: make the troubleshooting stack production-safe and semantically disciplined.

Work items:

1. Add API contract tests for the new diagnostic endpoints.
2. Add UI tests for overview, drilldown, timeline, and compare surfaces.
3. Add MCP parity tests against REST.
4. Add regression tests for durable versus ephemeral labeling.
5. Add fixtures for backlog growth, contention, dead-letter spikes, restart artifacts, and recovery confusion cases.
6. Verify no API shape implies durable recovery where the backend does not support it.
7. Tune hotspot and trend calculations so they stay low-cardinality and bounded.

Dependencies:

- Sprint 3 API work
- Sprint 4 and 5 UI work
- Sprint 6 MCP work

Exit criteria:

- the full troubleshooting flow is stable and test-covered
- semantics are consistent across REST, UI, and MCP
- the roadmap is ready for general operator use

## Proposed Deliverables By Milestone

### Milestone A: Diagnostic Core

- troubleshooting taxonomy
- shared snapshot model
- trend and hotspot computation
- top-level broker summary
- explanation schema
- initial timeline model
- test fixtures for healthy, stalled, and degraded cases

### Milestone B: Domain Signal Completion

- queue diagnostics completed
- rpc contention and failure signals completed
- lease contention and failure signals completed
- schedule and stream explanation gaps closed
- missing Prometheus counters added where needed
- JSON stats payloads extended with diagnostic fields
- all current admin resources expose last-change and trend where possible

### Milestone C: UI v1

- troubleshooting overview
- resource drilldown pages
- event timeline
- bottleneck explanation cards
- compare view
- realm and resource filters
- durable versus ephemeral state cues

### Milestone D: MCP v1

- read-only troubleshooting tools
- scoped inspection tools
- bounded explanation tools
- audit and policy checks
- shared troubleshooting contract with REST
- pagination and cost limits
- parity tests against REST read models

## Task Breakdown By Workstream

### Backend And Read Model

1. Extend the admin read model with recent event timelines and trend metadata.
2. Add a troubleshooting snapshot service that aggregates the read model into a single diagnostic shape.
3. Add per-domain diagnostic counters where missing.
4. Add bounded event retrieval and comparison helpers.
5. Add explanation synthesis based on the troubleshooting snapshot.

### API Layer

1. Extend `/api/v1/stats` with an incident summary and trend data.
2. Extend per-domain stats with failure, contention, and growth signals.
3. Extend per-resource endpoints with last-change metadata and diagnostic hints.
4. Add bounded event/timeline endpoints for resource troubleshooting.
5. Add snapshot comparison endpoints for regression analysis.
6. Update OpenAPI so the portal and MCP can bind against a stable contract.

### UI Layer

1. Build a troubleshooting overview landing page.
2. Build domain drilldowns with consistent table and card layouts.
3. Build resource pages with timeline and explanation sections.
4. Build a compare mode for pre/post incident inspection.
5. Add filters and breadcrumbs for realm, area, resource, and family.

### MCP Layer

1. Define the minimal tool set for safe diagnostics.
2. Bind each tool to an existing REST read model.
3. Add audit logging, scoping checks, and page limits.
4. Keep explanation tools deterministic and bounded.

### Test And Quality Work

1. Add fixture coverage for backlog growth, contention, dead-letter spikes, restart artifacts, and recovery confusion cases.
2. Add API contract tests for the new diagnostic endpoints.
3. Add UI snapshot or component tests for the troubleshooting pages.
4. Add MCP parity tests so AI tooling cannot drift from REST semantics.
5. Add regression tests for durable versus ephemeral labeling.

## Success Criteria

The work is successful when a user can answer the following without leaving the portal:

- what is currently broken or slow?
- which resource is responsible?
- what changed right before the issue?
- is this a live-process artifact or durable state?
- what should I inspect next?

For MCP, success means an agent can answer the same questions safely using the same control-plane facts.

For implementation, the roadmap is complete when the portal can show:

- the top bottleneck for a realm or resource
- the recent sequence of transitions that led there
- whether the issue is growing or receding
- whether the problem is durable state, live state, or both
- the next best drilldown or remediation step

And the same information is available through MCP without expanding authorization or query scope.

## Risks And Constraints

- Do not blur durable and ephemeral state.
- Do not imply replay, recovery, or ownership continuity unless the storage model actually supports it.
- Do not let observability become behavior.
- Do not introduce expensive scans just to support nicer explanations.
- Do not split the UI and MCP onto different data sources.

## Recommended Next Step

Start with Phase 1 and Phase 2 together: define the diagnostic questions, then implement the shared troubleshooting snapshot service. Everything else should consume that shape.
