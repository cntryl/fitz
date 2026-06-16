# Fitz UI Clarity Charter

This charter keeps the Fitz admin UI legible, truthful, and easy to scan.

## Rules

- Every screen must say what it is, what state it is in, and what to do next.
- Every header must include a clear title, one-sentence explanation, freshness or status, and a primary action when one exists.
- Every chart must explain the time window or scope and must not pretend to be more precise or historical than the data really is.
- Exact values belong in tables or metric blocks unless a visual comparison truly helps the user.
- Loading, empty, refreshing, and error states must be distinct and truthful.
- Copy must use Fitz terms precisely: `realm` is the application namespace, `route family` is the broker routing key, and ephemeral state must never be described as durable.
- Dense operational pages should favor clarity over decoration: fewer ornaments, stronger labels, tighter spacing, and obvious drill-down paths.

## Visual Standard

- Use the shared shell and page templates instead of inventing page-specific chrome.
- Prefer token-driven spacing, radii, and colors.
- Use charts for comparison and structure, tables for precision, and status badges for freshness.
- Keep mobile and dark mode behavior intentional, not incidental.
