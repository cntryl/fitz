# Auth0 Setup

This is the recommended first-party Auth0 shape for Fitz.

Use an Auth0 API access token, not an ID token. Fitz validates the token at
CONNECT time, resolves the broker-internal route family from `org_id`, and then
authorizes every request from route-shaped permissions in the token.

## Recommended Token Shape

```json
{
  "iss": "https://tenant.auth0.com/",
  "aud": ["https://fitz.example.com/api", "https://tenant.auth0.com/userinfo"],
  "sub": "auth0|user-1",
  "exp": 1234567890,
  "org_id": "org_acme",
  "permissions": ["notice://prod/orders/**#read"]
}
```

Important details:

- `iss` must exactly match the configured Auth0 issuer, including the trailing slash when Auth0 emits one.
- `aud` must contain the Fitz API identifier configured in Auth0. Auth0 may emit `aud` as either a string or an array; Fitz accepts both.
- `org_id` is the external identity value Fitz maps to a route family.
- `permissions` must contain Fitz route-shaped permissions or recognized coarse scopes such as `notice.read`.
- Do not include `fitz.route_family`, `fitz.permissions`, JWT `realm`, JWT `areas`, or JWT `scopes`.

## Auth0 Dashboard

1. Create or select the Auth0 API that represents Fitz.
2. Set the API Identifier to the same value you configure in Fitz as a JWT audience, for example `https://fitz.example.com/api`.
3. Use RS256 signing for the API.
4. Enable RBAC for the API.
5. Enable **Add Permissions in the Access Token** for the API.
6. Add API permissions using Fitz permission strings, for example `notice://prod/orders/**#read` or `kv://prod/**#write`.
7. Assign those permissions to users, roles, or machine-to-machine clients through Auth0.
8. Use Auth0 Organizations and send the login or token request with an organization so Auth0 emits `org_id`.

## Token Requests

For browser or user login flows, request an access token for the Fitz API
Identifier and include the Auth0 organization context. For machine-to-machine
flows, grant the client access to the Fitz API permissions and request the
token in the organization context when the token should carry `org_id`.

When constructing raw authorization URLs by hand, remember that Fitz permission
strings are OAuth scope values and must be URL-encoded. Auth0 SDKs normally
handle that encoding for you.

Auth0 docs for these steps:

- [Work with Tokens and Organizations](https://auth0.com/docs/manage-users/organizations/using-tokens)
- [Enable Role-Based Access Control for APIs](https://auth0.com/docs/manage-users/access-control/configure-core-rbac/enable-role-based-access-control-for-apis)
- [Add API Permissions](https://auth0.com/docs/get-started/apis/add-api-permissions)
- [Access Tokens](https://auth0.com/docs/tokens/concepts/access-token)
- [Get Access Tokens](https://auth0.com/docs/security/tokens/access-tokens/get-access-tokens)

## Fitz Environment

```sh
FITZ_AUTH_REQUIRED=true
FITZ_ROUTE_FAMILIES=1,2
FITZ_ROUTE_FAMILY_CLAIM=org_id
FITZ_ROUTE_FAMILY_MAP=org_acme=1,org_beta=2
FITZ_JWT_AUDIENCES=https://fitz.example.com/api
FITZ_JWT_JWKS_MAP=https://tenant.auth0.com/=https://tenant.auth0.com/.well-known/jwks.json
```

The JWKS URL value must be an absolute HTTPS URL without credentials or a
fragment.

The keys in `FITZ_ROUTE_FAMILY_MAP` are Auth0 organization IDs. The values are
the numeric route families provisioned on this Fitz node. A token with
`org_id=org_acme` resolves to route family `1`; a token with `org_id=org_beta`
resolves to route family `2`.

## Namespaced Claim Overrides

If your Auth0 tenant emits namespaced custom claims instead of plain `org_id`
or top-level `permissions`, configure override env vars:

```sh
FITZ_AUTH_ORG_CLAIM=fitz://org_id
FITZ_AUTH_PERMISSIONS_CLAIM=fitz://permissions
```

Identity resolution checks `FITZ_AUTH_ORG_CLAIM` first and falls back to
`FITZ_ROUTE_FAMILY_CLAIM` when the override claim is missing. Permission
normalization order remains fixed: `FITZ_AUTH_CUSTOM_CLAIM`, top-level
`permissions`, `FITZ_AUTH_PERMISSIONS_CLAIM`, `FITZ_AUTH_ROLE_CLAIM`, `scp`,
then `scope`.

## Custom Permission Claim

The recommended Auth0 path is top-level `permissions`. If you need a namespaced
custom claim instead, configure `FITZ_AUTH_CUSTOM_CLAIM` and emit only a
permissions object:

```sh
FITZ_AUTH_CUSTOM_CLAIM=https://example.com/fitz
```

```json
{
  "https://example.com/fitz": {
    "permissions": ["notice://prod/orders/**#read"]
  }
}
```

Do not set `FITZ_AUTH_CUSTOM_CLAIM=fitz`; that legacy shape is rejected at
startup.

## Troubleshooting

- Missing `org_id`: make sure the user or client authenticated in the context of an Auth0 Organization.
- Missing `permissions`: make sure API RBAC is enabled, **Add Permissions in the Access Token** is enabled, and the user or client has the expected grants.
- Audience mismatch: make sure the application requests an access token for the Fitz API Identifier, not only an ID token or `/userinfo` token.
- Issuer mismatch: make sure `FITZ_JWT_JWKS_MAP` uses the exact issuer string from the token `iss` claim.
- CONNECT closes after token validation: check that `org_id` exists in `FITZ_ROUTE_FAMILY_MAP` and maps to a family in `FITZ_ROUTE_FAMILIES`.
- Permission denied after CONNECT succeeds: check that the permission route includes the requested route realm and access level.
