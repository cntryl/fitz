# FAQ

## Is Fitz production-ready?

Yes, for Fitz's stated single-node model. Review [../README.md](../README.md), [../operations/production-runbook.md](../operations/production-runbook.md), and [durability.md](durability.md) before a production rollout.

## How do I choose route family values?

Route families are a deployment-level isolation allowlist, not a client routing
API. Configure `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous list starting at `1`
and map verified identity values with `FITZ_ROUTE_FAMILY_MAP`, for example
`FITZ_ROUTE_FAMILY_MAP=abc=1,xyz=2`.

For authenticated mode, the JWT must also include valid signing, audience, and
expiration claims. A missing or unmapped identity claim causes authentication
failure and broker connection close. Configure `FITZ_ROUTE_FAMILY_CLAIM` to the
claim that identifies your application partition, and configure
`FITZ_AUTH_ORG_CLAIM` when a namespaced override should be checked first.

Keep route strings stable for application semantics.

## What JWT claims does Fitz require?

Authenticated JWTs should carry the following:

- `sub`
- `aud`
- `exp`
- the configured route-family identity claim (`tid` by default), optionally overridden by `FITZ_AUTH_ORG_CLAIM`
- one supported permission source: configured custom permissions claim, top-level `permissions`, configured `FITZ_AUTH_PERMISSIONS_CLAIM` array, configured role claim array, `scp`, or `scope`
- `iss` when using JWKS (issuer-based verification)

Fitz reads permissions in this order: configured namespaced custom claim,
top-level `permissions`, configured `FITZ_AUTH_PERMISSIONS_CLAIM` array,
configured role claim array, `scp`, then `scope`. If you configure
`FITZ_AUTH_ROLE_CLAIM`, every role value must itself be a Fitz permission
string or recognized coarse scope.

## How should I configure Auth0?

Use Auth0 Organizations, set `FITZ_ROUTE_FAMILY_CLAIM=org_id`, and enable API
RBAC with **Add Permissions in the Access Token** so Auth0 emits top-level
`permissions`. Use an Auth0 API access token whose `aud` includes the Fitz API
Identifier. See [auth0.md](auth0.md) for the full setup.

## Where is the full wire protocol?

See [../clients/client-spec.md](../clients/client-spec.md).

## Where do I start if requests fail intermittently?

Start with [troubleshooting.md](troubleshooting.md), then review [../operations/observability.md](../operations/observability.md).
