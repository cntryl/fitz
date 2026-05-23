---
name: askr-forms-tables-crud
description: Use when building askr CRUD screens, forms, validation, filters, tables, pagination, selection, bulk actions, confirmation dialogs, row actions, resource/query data flow, and feature folder boundaries.
---

# Askr Forms Tables CRUD

Use this for create, read, update, archive, filter, paginate, and edit workflows. The goal is a predictable split between route composition, feature workflow state, and transport boundaries.

## Inspect First

- The nearest page that owns a table or form.
- The nearest feature folder for the same domain.
- The adapter or service that owns transport.
- Existing tests that cover filtering, submission, or destructive actions.

## Use This When

- You are adding a list, detail, create, edit, archive, or bulk-action flow.
- You need filters, pagination, selection, or row actions.
- You need field validation and server validation errors.
- You need a safe destructive action flow.

## Pick The Owner

- The route or page composes the screen.
- The feature folder owns filters, form state, table columns, queries, and mutations.
- The adapter owns fetch, update, archive, and DTO mapping.
- Shared helpers own cross-cutting formatting and validation helpers when reused.

## Copy This Shape

```text
src/pages/app/users.tsx
src/features/users/user-table.tsx
src/features/users/user-filters.tsx
src/features/users/user-form.tsx
src/features/users/users.query.ts
src/features/users/users.mutation.ts
src/adapters/users-client.ts
src/shared/format.ts
```

Routes compose the screen. Features own domain UI and workflow state. Adapters own transport. Shared helpers own cross-cutting formatting or validation utilities.

## Do This In Order

1. Choose the async owner first: `resource()` for route-owned reads, `createQuery()` for shared keyed reads, and `createMutation()` for writes.
2. Keep filters, pagination, dialog state, and selected IDs in local `state()` pairs, and read or write each one through its `[getter, setter]` pair.
3. Keep field validation and submit handling explicit in the feature layer.
4. Confirm destructive actions before archive or delete.
5. Refresh or invalidate after create, update, archive, or delete.

## Table Rules

- Use stable row keys, never index keys for records.
- Put feature-specific columns in the feature folder.
- Use `selector()` for keyed row selection fanout when the table is large or hot.
- Provide loading, empty, error, and disabled action states.
- Keep formatting in `src/shared/format.ts` or a domain feature helper.

## Form Rules

- Keep form state local unless multiple routes need it.
- Put validation rules in `src/features/<feature>` when domain-specific, or `src/shared` when cross-cutting.
- Use `@askrjs/ui` form controls for behavior and accessibility.
- Disable submit while pending and surface field or form errors explicitly.

- API clients in table, form, or generic UI components.
- Bound live or frequently updating rows so tables and timelines do not churn the whole DOM.
- Keep filtering, sorting, and pagination state together in the feature workflow instead of scattering it across route files.
- Business rules in reusable primitives.
- Hidden destructive actions without confirmation.
- Duplicated pagination/filter logic across route files.
- A table component that imports a raw API client and mutates records directly from row actions.
- A form that shows only a toast on submit failure and never maps field errors back to inputs.
- Pagination, sorting, and filter state duplicated in multiple route pages.

## Validate

- CRUD ownership boundaries are obvious.
- Every async path has loading, error, retry, or disabled feedback.
- Selection, bulk actions, and destructive actions are keyboard reachable.
- Submit handlers preserve pending state and field or form errors.
- Record rows use stable keys.

## Done When

- Form, table, mutation, and adapter ownership are separated.
- Field errors, form errors, pending state, and success state are all visible.
- Selection, filters, sorting, and destructive actions are keyboard reachable.
- Rows keep stable identity and large updates do not force whole-list churn.

## Handoff

- Use `askr-query-mutation` when the form or table state becomes shared keyed state across screens.
- Use `askr-accessibility` when validation, focus, or destructive-action UX is the hard part.
- Use `askr-testing-determinism` to validate submit, retry, selection, and list identity behavior.
