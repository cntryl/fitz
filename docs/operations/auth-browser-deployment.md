# Auth and Browser Deployment

This guide describes Fitz authentication and the browser-facing controls
implemented by the broker. The repository Compose files bind published ports to
loopback and use development credentials; they are examples for local use.

## Authenticated Deployment Baseline

Use this baseline whenever `FITZ_AUTH_REQUIRED=true` or when the admin surface
is exposed beyond loopback:

- Runtime JWT verification uses `FITZ_JWT_JWKS_MAP`.
- Admin uses `FITZ_ADMIN_AUTH_MODE=protected`.
- Browser traffic arrives through external TLS termination.
- `FITZ_WS_ALLOWED_ORIGINS` and `FITZ_ADMIN_PUBLIC_ORIGIN` are exact public
  origins, not wildcard or loopback placeholders.

## Runtime Auth

Set at least these environment values:

```sh
FITZ_AUTH_REQUIRED=true
FITZ_JWT_JWKS_MAP=https://idp.example=https://idp.example/.well-known/jwks.json
FITZ_JWT_AUDIENCES=fitz
FITZ_ROUTE_FAMILIES=1
FITZ_ROUTE_FAMILY_MAP=acme=1
```

Add claim-source overrides only when your identity provider requires them, for
example `FITZ_AUTH_ORG_CLAIM` or `FITZ_ROUTE_FAMILY_CLAIM`.

Configuration constraints:

- Use HTTPS JWKS URLs only.
- Keep `FITZ_JWT_ALLOW_INSECURE_HTTP=false`.
- Do not use `FITZ_JWT_HMAC_SECRET` outside testing or local prototyping.
- Issue short-lived tokens and reconnect with a fresh token on expiry.

When Fitz rejects a runtime JWT, the CONNECT failure log includes bounded
diagnostics for the algorithm, key ID, issuer, audience, time bounds, configured
permission-source names, and values found in those permission sources. The log
uses a short SHA-256 token fingerprint for correlation. It never includes the
compact JWT or signature. The added diagnostic fields exclude the subject,
identity values, and unrelated claims. Treat the reported header and payload
fields as untrusted troubleshooting input; they are decoded only after the
normal verification path has rejected the token.

## Admin and Browser Perimeter

Set at least these environment values:

```sh
FITZ_ADMIN_AUTH_MODE=protected
FITZ_ROOT_PASSWORD=<secret>
FITZ_ADMIN_PUBLIC_ORIGIN=https://admin.example.com
FITZ_ADMIN_COOKIE_SECURE=true
FITZ_WS_ALLOWED_ORIGINS=https://app.example.com
FITZ_ASSUME_EXTERNAL_TLS=true
```

Operational expectations:

- Put Fitz behind a TLS-terminating load balancer, reverse proxy, sidecar, or
  other trusted network boundary.
- Fitz rejects startup when runtime auth or protected admin is configured on a
  non-loopback bind without `FITZ_ASSUME_EXTERNAL_TLS=true`. This is an explicit
  deployment assertion; Fitz does not terminate TLS itself.
- Keep the backend listener reachable only from that trusted edge.
- Keep raw TCP disabled unless you explicitly need it, or protect it with a
  trusted TLS/private-network path.
- Protected admin session cookies expire across broker restarts because the
  signing key is process-ephemeral.
- Admin login bodies are small and bounded, password verification runs off the
  transport executor, and repeated failures from one client address are
  rate-limited. Keep an edge rate limit as an additional distributed control.
- JWKS downloads use HTTPS, strict time and size limits, a shared cache, and
  coalesced refreshes. A missing key ID does not trigger an unrestricted
  identity-provider request for every connection.

## What Not To Reuse From Local Examples

Do not carry these local-development conveniences into an externally reachable
deployment:

- `FITZ_ADMIN_AUTH_MODE=open`
- `FITZ_JWT_HMAC_SECRET`
- `FITZ_JWT_ALLOW_INSECURE_HTTP=true`
- `FITZ_ASSUME_LOCAL_LOOPBACK_EDGE=true`
- loopback-only or placeholder origins
- repository Compose files without reviewing their loopback bindings,
  credentials, and storage settings
