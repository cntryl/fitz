# FAQ

## Is Fitz production-ready?

Not yet. Review [../README.md](../README.md) and the readiness documentation before a production rollout.

## How do I choose route family values?

Route families are a deployment-level isolation allowlist, not a client routing
API. Configure `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous list starting at `1`
and map verified identity values with `FITZ_ROUTE_FAMILY_MAP`, for example
`FITZ_ROUTE_FAMILY_MAP=abc=1,xyz=2`.

For authenticated mode, the JWT must also include valid signing, audience, and
expiration claims. A missing or unmapped identity claim causes authentication
failure and broker connection close. Use `FITZ_ROUTE_FAMILY_CLAIM=org_id` when
using Auth0 Organizations; the default claim is `tid`, which fits Microsoft
Entra ID. Cognito and Okta can use `sub`, a provider custom claim, or an exact
namespaced claim key.

Keep route strings stable for application semantics.

## What JWT claims does Fitz require?

Authenticated JWTs should carry the following:

- `sub`
- `aud`
- `exp`
- the configured route-family identity claim (`tid` by default)
- `permissions` or `scopes`
- `iss` when using JWKS (issuer-based verification)

Fitz reads permissions from the configured namespaced custom claim first, then
top-level `permissions`, then `scope` or `scp`. `roles` claims are identity
metadata only and are not treated as permissions unless your issuer maps them
into one of the supported permission sources.

## Where is the full wire protocol?

See [../clients/client-spec.md](../clients/client-spec.md).

## Where do I start if requests fail intermittently?

Start with [troubleshooting.md](troubleshooting.md), then review [../operations/observability.md](../operations/observability.md).
