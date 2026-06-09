# Quick Start

This guide provides the fastest path to a successful first Fitz request.

## 1. Start Fitz

Use the repository compose or local runtime command and confirm readiness via `/readyz`.

## 2. Connect a Client

Use any client that follows [clients/client-spec.md](../clients/client-spec.md).

## 3. Authenticate

Send CONNECT with a valid JWT as the first frame. In authenticated mode, the
JWT must be signed and must include:

- `sub` — subject identity
- `aud` — one of the broker audiences configured by `FITZ_JWT_AUDIENCES`
- `exp` — token expiration time
- the configured identity claim — `tid` by default, or `org_id` for Auth0 Organizations
- `permissions` or `scopes` — authorization rules for subsequent domain requests
- `iss` when using JWKS mode; it must match a configured issuer

Configure `FITZ_ROUTE_FAMILY_MAP`, such as `FITZ_ROUTE_FAMILY_MAP=xyz=2`,
to translate the verified identity claim value to a provisioned route family.
Realm access is authorized by route-shaped permission patterns, not by a JWT
realm claim.

Common identity-provider setups:

| Provider | Identity claim | Permission source | Example Fitz env |
| --- | --- | --- | --- |
| Auth0 Organizations | `org_id` | top-level `permissions`, or `FITZ_AUTH_CUSTOM_CLAIM` | `FITZ_ROUTE_FAMILY_CLAIM=org_id` and `FITZ_ROUTE_FAMILY_MAP=org_acme=1` |
| Microsoft Entra ID | `tid` | `scp`, or `FITZ_AUTH_CUSTOM_CLAIM` | `FITZ_ROUTE_FAMILY_CLAIM=tid` and `FITZ_ROUTE_FAMILY_MAP=<tenant-guid>=1` |
| Amazon Cognito | `custom:tenant_id` or `sub` | `scope`, or `FITZ_AUTH_CUSTOM_CLAIM` | `FITZ_ROUTE_FAMILY_CLAIM=custom:tenant_id` and `FITZ_ROUTE_FAMILY_MAP=acme=1` |
| Okta | exact custom or namespaced claim | `scope`, or `FITZ_AUTH_CUSTOM_CLAIM` | `FITZ_ROUTE_FAMILY_CLAIM=https://example.com/identity` and `FITZ_ROUTE_FAMILY_MAP=acme=1` |

`roles` claims are retained as identity-provider metadata but are not treated
as Fitz permissions by default. Issue route-shaped permissions, coarse scopes
such as `notice.read`, or a configured namespaced Fitz permissions object.

If `FITZ_AUTH_REQUIRED=false`, anonymous mode is allowed and the broker uses
route family `1`.

## 4. Execute a Simple Operation

1. Choose an opaque route string.
2. Send a single operation, such as KV get or put.
3. Confirm both the response and latency metrics.

## 5. Continue

- [api-guide.md](api-guide.md)
- [durability.md](durability.md)
- [troubleshooting.md](troubleshooting.md)
