# Fitz Cross-Language Conformance Runner Contract

This document defines the harness contract used by fitz-go, fitz-ts, and fitz-py to execute the shared scenario suite in cross-language-conformance-suite.yaml.

## Goals

- Run the same behavioral scenarios in all client SDKs.
- Produce machine-comparable JSON results.
- Enforce strict reconnect parity and first-class timeout/cancellation/cleanup semantics.

## Required Runner Inputs

Each runner must accept these inputs:

- suite_path: path to cross-language-conformance-suite.yaml
- client_name: one of fitz-go, fitz-ts, fitz-py
- transport: websocket or tcp
- auth_mode: anonymous, valid_jwt, invalid_jwt
- broker_addr: endpoint for selected transport/auth mode
- output_path: file path for JSON result

Recommended optional inputs:

- seed: deterministic random seed for generated routes/payloads
- timeout_scale: multiplier for slower CI environments
- reconnect_enabled_override: force reconnect on/off to test contract boundaries

## Required Runner Behavior

1. Parse the suite file and execute every listed scenario in order.
2. For each scenario, isolate state using unique route prefixes.
  Scenario setups may include optional load-shaping knobs such as `concurrency_limit` and `burst_size`; runners should honor them when present and ignore unknown fields.
3. Capture verdict using the result schema from the suite.
4. Attach evidence in a language-native but normalized format:
- operation traces
- error type/code
- state transitions
- timing fields
5. Continue execution after failures and record all results.
6. Exit non-zero if any P0 scenario is not pass.

## Per-Scenario Result Shape

Each emitted scenario record must include:

- scenario_id
- client
- transport
- auth_mode
- verdict
- latency_ms
- evidence
- notes

Example:

```json
{
  "scenario_id": "CS-008",
  "client": "fitz-ts",
  "transport": "websocket",
  "auth_mode": "valid_jwt",
  "verdict": "pass",
  "latency_ms": 47,
  "evidence": {
    "error_type": "AbortError",
    "post_cancel_request_succeeds": true
  },
  "notes": "rpc call canceled via AbortSignal"
}
```

## Aggregate Result Shape

Top-level output JSON should contain:

- client
- suite_version
- run_started_at
- run_finished_at
- scenarios: array of scenario records
- summary: aggregate pass/fail counts and p0/p1 rates

## Suggested Runner Commands

These are recommended command shapes (exact implementation may vary):

- fitz-go: go test ./conformance -run TestConformance -args -suite <path> -transport <t> -auth <a> -out <json>
- fitz-ts: npm run test:conformance -- --suite <path> --transport <t> --auth <a> --out <json>
- fitz-py: pytest tests/conformance -q --suite <path> --transport <t> --auth <a> --out <json>

## CI Matrix Recommendation

Run each client against:

- transport: websocket, tcp
- auth_mode: anonymous, valid_jwt

Run auth_mode=invalid_jwt for CS-002 and connection-focused scenarios.

Minimal matrix:

1. fitz-go + websocket + anonymous
2. fitz-go + websocket + valid_jwt
3. fitz-go + tcp + anonymous
4. fitz-go + tcp + valid_jwt
5. fitz-ts + websocket + anonymous
6. fitz-ts + websocket + valid_jwt
7. fitz-ts + tcp + anonymous
8. fitz-ts + tcp + valid_jwt
9. fitz-py + websocket + anonymous
10. fitz-py + websocket + valid_jwt
11. fitz-py + tcp + anonymous
12. fitz-py + tcp + valid_jwt

## Mapping Suite Scenarios to Existing Acceptance Criteria

- CS-001..CS-003 map directly to connection and basic operation criteria.
- CS-004..CS-006 map to error handling criteria.
- CS-007..CS-010 enforce timeout/cancel/reconnect behavior parity.
- CS-011..CS-013 enforce stream semantics.
- CS-016 enforces filtered stream replay and optional stream metadata.
- CS-014..CS-017 enforce concurrency, bounded-load, and lifecycle cleanup semantics.

## Adoption Steps

1. Add a conformance test target in each client repo.
2. Implement scenario adapters that invoke each language's public client API.
3. Emit normalized JSON output.
4. Add CI gate: fail build if any P0 scenario is not pass.
5. Add trend reporting for P1 scenarios to prevent drift.
