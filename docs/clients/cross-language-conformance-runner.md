# Cross-Language Conformance Runner

The shared [scenario suite](cross-language-conformance-suite.yaml) defines
observable client behavior. Each SDK repository adapts those scenarios to its
native test runner; this document defines the common inputs and results.

## Inputs

Each runner accepts the suite path, client name, transport (`websocket` or
`tcp`), authentication mode (`anonymous`, `valid_jwt`, or `invalid_jwt`),
broker address, and JSON output path. Scenarios use unique route prefixes so
their state does not overlap.

## Execution

- Run every scenario that applies to the selected transport and authentication
  mode.
- Continue after a scenario fails and record its evidence.
- Exercise reconnect with the same client instance and a new broker session.
- Exit unsuccessfully if any P0 scenario does not pass.

## Results

Each scenario result records `scenario_id`, `client`, `transport`, `auth_mode`,
`verdict`, `evidence`, and `correlation_enabled`. Verdicts are `pass`,
`partial`, `fail`, `not_implemented`, or `unclear`.

The summary reports pass and total counts for P0 and P1 scenarios, plus an
overall status. Results establish only the scenarios and broker/client builds
that were run; they are not a general compatibility or deployment claim.
