# Fitz Client Perfection Scoreboard

This document is the execution board for strict all-client completion.

## Frozen Gate

- Scope: fitz-go, fitz-ts, fitz-py, fitz-rs, fitz-dotnet.
- Contract sources:
  - `client-spec.md`
  - `client-acceptance-criteria.md`
  - `client-requirements.md`
  - `cross-language-conformance-suite.yaml`
  - `cross-language-conformance-runner.md`
- Done definition:
  - Every repo in scope must report 100% P0 and 100% P1 on required conformance matrix legs.
  - Any uncovered or divergent contract behavior is a must-fix.
  - New items not required by contract are backlog and do not reopen done criteria.

## Matrix Legs

Required for each repo unless the contract explicitly exempts the client:

1. websocket + anonymous
2. websocket + valid_jwt
3. tcp + anonymous
4. tcp + valid_jwt

Auth-failure leg for CS-002:

1. websocket + invalid_jwt
2. tcp + invalid_jwt

## Repo Commands

Use these command shapes as the baseline runbook. Keep env vars aligned with each repo CI config.

### fitz-go

- Full tests:
  - `go test ./...`
- Conformance:
  - `go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite`
- Matrix leg override:
  - `CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=valid_jwt go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite`
- Invalid JWT focused check:
  - `CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=anonymous go test -v -timeout 120s ./test/conformance/... -run TestConformanceSuite/CS-002_auth_failure`
- Conformance artifact default:
  - `./conformance-results.json`

### fitz-ts

- Full verification:
  - `npm run verify`
- Conformance:
  - `npm run test:conformance`
- Matrix leg override:
  - `CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=valid_jwt npm run test:conformance`
- Conformance artifact default:
  - `./artifacts/conformance-results.json`

### fitz-py

- Full verification:
  - `pytest tests/unit && pytest tests/integration -v && pytest tests/conformance -v`
- Conformance:
  - `pytest tests/conformance -v`
- Matrix leg override:
  - `CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=valid_jwt pytest tests/conformance -v`
- Invalid JWT focused check:
  - `CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=anonymous pytest tests/conformance/test_conformance.py -k cs002 -v`
- Conformance artifact default:
  - `./artifacts/conformance-results.json`

### fitz-rs

- Library tests:
  - `cargo test --lib`
- Conformance:
  - `cargo test --test conformance -- --ignored --nocapture`
- Matrix leg override:
  - `CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=valid_jwt cargo test --test conformance -- --ignored --nocapture`
- Conformance artifact default:
  - `./artifacts/conformance-results.json`

### fitz-dotnet

- Core tests:
  - `dotnet test tests/Core/Core.Tests.csproj -c Release`
- Conformance:
  - `dotnet test tests/Core/Core.Tests.csproj -c Release --filter FullyQualifiedName~Conformance`
- Matrix leg override:
  - `CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=valid_jwt dotnet test tests/Core/Core.Tests.csproj -c Release --filter FullyQualifiedName~Conformance`
- Invalid JWT focused check:
  - `dotnet test tests/Core/Core.Tests.csproj -c Release --filter FullyQualifiedName~should_fail_invalid_jwt_auth_when_connecting`
- Conformance artifact default:
  - `./artifacts/conformance-results.json`

## Execution Ledger

Update this table after each matrix run.

| Repo        | Transport | Auth Mode            | P0 Pass Rate | P1 Pass Rate | Overall | Artifact Path                                                                    | Must-Fix? | Notes                                                                          |
| ----------- | --------- | -------------------- | ------------ | ------------ | ------- | -------------------------------------------------------------------------------- | --------- | ------------------------------------------------------------------------------ |
| fitz-go     | websocket | anonymous            | 100%         | 100%         | pass    | `fitz-go/artifacts/conformance-ws-anonymous.json`                                | no        | No non-pass scenarios                                                          |
| fitz-go     | websocket | valid_jwt            | 100%         | 100%         | pass    | `fitz-go/artifacts/conformance-ws-valid_jwt.json`                                | no        | No non-pass scenarios                                                          |
| fitz-go     | tcp       | anonymous            | 100%         | 100%         | pass    | `fitz-go/artifacts/conformance-tcp-anonymous.json`                               | no        | No non-pass scenarios                                                          |
| fitz-go     | tcp       | valid_jwt            | 100%         | 100%         | pass    | `fitz-go/artifacts/conformance-tcp-valid_jwt.json`                               | no        | No non-pass scenarios                                                          |
| fitz-go     | websocket | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `go test ./test/conformance -run TestConformanceSuite/CS-002_auth_failure (ws)`  | no        | Focused auth-failure check passed                                              |
| fitz-go     | tcp       | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `go test ./test/conformance -run TestConformanceSuite/CS-002_auth_failure (tcp)` | no        | Focused auth-failure check passed                                              |
| fitz-ts     | websocket | anonymous            | 100%         | 100%         | pass    | `fitz-ts/artifacts/conformance-ws-anonymous.json`                                | no        | No non-pass scenarios                                                          |
| fitz-ts     | websocket | valid_jwt            | 100%         | 100%         | pass    | `fitz-ts/artifacts/conformance-ws-valid_jwt.json`                                | no        | No non-pass scenarios                                                          |
| fitz-ts     | tcp       | anonymous            | 100%         | 100%         | pass    | `fitz-ts/artifacts/conformance-tcp-anonymous.json`                               | no        | No non-pass scenarios                                                          |
| fitz-ts     | tcp       | valid_jwt            | 100%         | 100%         | pass    | `fitz-ts/artifacts/conformance-tcp-valid_jwt.json`                               | no        | No non-pass scenarios                                                          |
| fitz-ts     | websocket | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `fitz-ts/artifacts/conformance-ws-anonymous.json`                                | no        | CS-002 passes in full suite; standalone invalid_jwt mode unsupported           |
| fitz-ts     | tcp       | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `fitz-ts/artifacts/conformance-tcp-anonymous.json`                               | no        | CS-002 passes in full suite; standalone invalid_jwt mode unsupported           |
| fitz-py     | websocket | anonymous            | 100%         | 100%         | pass    | `fitz-py/artifacts/conformance-ws-anonymous.json`                                | no        | No non-pass scenarios                                                          |
| fitz-py     | websocket | valid_jwt            | 100%         | 100%         | pass    | `fitz-py/artifacts/conformance-ws-valid_jwt.json`                                | no        | No non-pass scenarios                                                          |
| fitz-py     | tcp       | anonymous            | 100%         | 100%         | pass    | `fitz-py/artifacts/conformance-tcp-anonymous.json`                               | no        | No non-pass scenarios                                                          |
| fitz-py     | tcp       | valid_jwt            | 100%         | 100%         | pass    | `fitz-py/artifacts/conformance-tcp-valid_jwt.json`                               | no        | No non-pass scenarios                                                          |
| fitz-py     | websocket | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `pytest tests/conformance -k cs002 (ws)`                                         | no        | Focused auth-failure check passed                                              |
| fitz-py     | tcp       | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `pytest tests/conformance -k cs002 (tcp)`                                        | no        | Focused auth-failure check passed                                              |
| fitz-rs     | websocket | anonymous            | 12.5%        | 0%           | fail    | `fitz-rs/artifacts/conformance-ws-anonymous.json`                                | yes       | P0/P1 blocked by route-shape mismatch errors and CS-008/CS-010 not implemented |
| fitz-rs     | websocket | valid_jwt            | 12.5%        | 0%           | fail    | `fitz-rs/artifacts/conformance-ws-valid_jwt.json`                                | yes       | P0/P1 blocked by route-shape mismatch errors and CS-008/CS-010 not implemented |
| fitz-rs     | tcp       | anonymous            | 12.5%        | 0%           | fail    | `fitz-rs/artifacts/conformance-tcp-anonymous.json`                               | yes       | Includes stub-server panic in timeout/disconnect scenarios                     |
| fitz-rs     | tcp       | valid_jwt            | 12.5%        | 0%           | fail    | `fitz-rs/artifacts/conformance-tcp-valid_jwt.json`                               | yes       | Includes stub-server panic in timeout/disconnect scenarios                     |
| fitz-rs     | websocket | invalid_jwt (CS-002) | n/a          | n/a          | fail    | `fitz-rs/artifacts/conformance-ws-anonymous.json`                                | yes       | CS-002 verdict is partial in full-suite artifact                               |
| fitz-rs     | tcp       | invalid_jwt (CS-002) | n/a          | n/a          | fail    | `fitz-rs/artifacts/conformance-tcp-anonymous.json`                               | yes       | CS-002 verdict is partial in full-suite artifact                               |
| fitz-dotnet | websocket | anonymous            | 100%         | 100%         | pass    | `fitz-dotnet/artifacts/conformance-ws-anonymous.json`                            | no        | No non-pass scenarios                                                          |
| fitz-dotnet | websocket | valid_jwt            | 100%         | 100%         | pass    | `fitz-dotnet/artifacts/conformance-ws-valid_jwt.json`                            | no        | No non-pass scenarios                                                          |
| fitz-dotnet | tcp       | anonymous            | 100%         | 100%         | pass    | `fitz-dotnet/artifacts/conformance-tcp-anonymous.json`                           | no        | No non-pass scenarios                                                          |
| fitz-dotnet | tcp       | valid_jwt            | 100%         | 100%         | pass    | `fitz-dotnet/artifacts/conformance-tcp-valid_jwt.json`                           | no        | No non-pass scenarios                                                          |
| fitz-dotnet | websocket | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `fitz-dotnet/artifacts/conformance-ws-anonymous.json`                            | no        | CS-002 verdict is pass (`auth_mode=invalid_jwt`) in artifact                   |
| fitz-dotnet | tcp       | invalid_jwt (CS-002) | n/a          | n/a          | pass    | `fitz-dotnet/artifacts/conformance-tcp-anonymous.json`                           | no        | CS-002 verdict is pass (`auth_mode=invalid_jwt`) in artifact                   |

## Must-Fix Tracker

Add one row per open defect that blocks strict gate completion.

| ID     | Repo    | Scenario/Req                           | Severity | Owner      | Status | Root Cause                                                                                                                                                                        | Fix PR | Verification Evidence                  |
| ------ | ------- | -------------------------------------- | -------- | ---------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | -------------------------------------- |
| MF-001 | fitz-rs | CS-001..CS-005, CS-007, CS-011..CS-015 | P0       | unassigned | open   | Conformance route generator uses non-canonical route depth (`.../res`) causing domain validation failures; plus missing cancellation/reconnect semantics and TCP stub panic paths |        | `fitz-rs/artifacts/conformance-*.json` |

## Backlog Tracker

Use this only for non-gating work that should not expand scope.

| ID     | Repo | Topic | Why Not Gating | Owner | Target Milestone |
| ------ | ---- | ----- | -------------- | ----- | ---------------- |
| BL-001 |      |       |                |       |                  |

## Release Decision Checklist

- All matrix rows for all scoped repos are populated with evidence.
- Every scoped repo shows 100% P0 and 100% P1 on required legs.
- Invalid JWT behavior for CS-002 is explicitly proven.
- Reconnect/cancellation/correlation scenarios are proven without partial verdicts.
- Must-fix tracker is empty.
- Final ledger has links to artifacts for independent re-check.
