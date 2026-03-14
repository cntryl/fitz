# Fitz Overview

Fitz is a multi-domain broker designed around async network edges and a deterministic sync runtime.

## What Fitz Provides

- Key-value operations
- Queue semantics with leasing flows
- Pub/sub notice fanout
- Request/response RPC
- Stream append and read patterns
- Lease and schedule control domains

## Core Concepts

- Route address: domain-oriented path for operations
- Route family: numeric partition and isolation boundary
- Realm: logical isolation scope for resources and permissions
- Session: authenticated connection context

Read [development/the-big-idea.md](../development/the-big-idea.md) for architecture intent.
