# FAQ

## Is Fitz production-ready?

Not yet. Review [../README.md](../README.md) and the readiness documentation before a production rollout.

## How do I choose route family values?

Route families are a deployment-level isolation allowlist, not a client routing
API. Configure `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous list starting at `1`
and issue authenticated JWTs with one provisioned `fitz.route_family`.

For authenticated mode, the JWT must also include valid signing, audience, and
expiration claims. A missing, zero, or unprovisioned `fitz.route_family` causes
authentication failure and broker connection close.

Keep route strings stable for application semantics.

## What JWT claims does Fitz require?

Authenticated JWTs should carry the following:

- `sub`
- `aud`
- `exp`
- `fitz.route_family` (non-zero, provisioned)
- `permissions` or `scopes`
- `iss` when using JWKS (issuer-based verification)

## Where is the full wire protocol?

See [../clients/client-spec.md](../clients/client-spec.md).

## Where do I start if requests fail intermittently?

Start with [troubleshooting.md](troubleshooting.md), then review [../operations/observability.md](../operations/observability.md).
