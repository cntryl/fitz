# Stability Policy

This document defines pre-1.0 stability expectations.

## Current Contract

- APIs and formats may evolve.
- Breaking changes are allowed only with explicit migration guidance.
- Runtime correctness and durability claims must remain documented and test-backed.

## What We Try To Keep Stable

1. Core route and dispatch model.
2. Error categories and operational semantics.
3. Observability signals required for safe operation.

## What May Change More Frequently

1. Experimental features.
2. Performance internals.
3. Optional client ergonomics in companion SDKs.
