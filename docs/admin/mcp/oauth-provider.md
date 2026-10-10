# Pinned OAuth provider setup

The supplied [Keycloak realm import](keycloak/realm.json) configures Keycloak
26.8.0 with an authorization-code client requiring S256 PKCE and a separate
client-credentials identity. The verified image digest is
`quay.io/keycloak/keycloak:26.8.0@sha256:b0f60d489d51c5d113390bdf5461d4c06e6051be026c05549f2e1e10ec352bcc`.
Fitz validates access tokens against a pinned RSA public key. It does not fetch
discovery/JWKS data on requests or operate an authorization server.

## Import and provision

1. Serve Keycloak at an HTTPS issuer such as
   `https://identity.example.test/realms/fitz-mcp-example`. Import `realm.json`
   using `--import-realm`. Follow the provider's
   [container deployment guide](https://www.keycloak.org/server/containers)
   for database, TLS, proxy and bootstrap administrator configuration.
2. Serve Fitz at `https://fitz.example.test/mcp`. Replace the audience mapper's
   `included.custom.audience` if this resource URL differs. Set Fitz's
   `FITZ_MCP_OAUTH_ISSUER` and `FITZ_MCP_OAUTH_AUDIENCE` to these exact values.
3. Review the supplied claim mappers before admitting users. They grant family
   `1`, `queue://example-realm/operations/**#read`, the `inspect` capability and
   `fitz.mcp.read` scope. Provision family `1` in Fitz. The realm
   `example-realm` is independent of that routing family.
4. Provision approved users in this realm. Public registration and password
   grants are disabled. All users admitted to this example client receive the
   same narrow grant. For individual grants, configure trusted role/group
   mappers and prohibit user editing of authority attributes.
5. Retrieve the generated `fitz-mcp-machine` client secret from Keycloak's
   Credentials tab into a file with mode `0600`. No secret is in the import.
   Keep secrets and tokens out of command arguments, repository files, logs and
   MCP arguments.
6. Obtain the active RS256 signing public key from the provider's Keys view or
   `<issuer>/protocol/openid-connect/certs`. Convert the selected RSA JWK to a
   PEM SubjectPublicKeyInfo key and install it at
   `FITZ_MCP_OAUTH_PUBLIC_KEY_FILE` before starting Fitz.

The discovery document at `<issuer>/.well-known/openid-configuration` supplies
the authorization, token and JWKS endpoints. Review it over trusted HTTPS during
setup. Fitz's `/.well-known/oauth-protected-resource/mcp` document points clients
to this issuer and lists supported scopes.

## Human authorization code with PKCE

The public client `fitz-mcp-human` accepts only
`http://127.0.0.1:15202/callback` in this example. Register the actual client's
exact redirect URI before use; do not add wildcard redirects.

Generate a fresh random verifier, its base64url SHA-256 S256 challenge and a
fresh `state`. Open `<issuer>/protocol/openid-connect/auth` with:

| Parameter | Value |
| --- | --- |
| `client_id` | `fitz-mcp-human` |
| `response_type` | `code` |
| `redirect_uri` | Exact registered callback |
| `scope` | `fitz.mcp.read` |
| `state` | Fresh client-generated value |
| `code_challenge` | Base64url SHA-256 of verifier without padding |
| `code_challenge_method` | `S256` |

After the user authenticates with Keycloak, verify callback `state`. Exchange
the single-use code at `<issuer>/protocol/openid-connect/token` using
`grant_type=authorization_code`, `client_id`, the exact `redirect_uri`, `code`
and `code_verifier`. Public clients send no secret. Use the access token as the
Fitz bearer credential; do not use an ID token. Tokens expire after five minutes.

## Machine grant

Read the protected secret file and send an HTTPS form POST to
`<issuer>/protocol/openid-connect/token` with:

```text
grant_type=client_credentials
client_id=fitz-mcp-machine
client_secret=<read from protected file>
scope=fitz.mcp.read
```

Write `access_token` directly to a protected token file for `fitz-mcp-stdio`, or
hold it in memory for the HTTP client. Request a new token before expiry.
Fitz checks signature, issuer, resource audience, time bounds, subject, role,
family grants, route permissions and capabilities. Scope alone grants no Fitz
authority. A route permission with an empty path segment (for example
`kv://#read` or `kv://prod//orders#read`) invalidates the whole token, not
just that grant. The example deliberately cannot read other families or realms, run
global summaries, or mutate resources.

## Pinned-key rotation

1. Disable MCP mutations and pause callers. Reconcile actions already
   dispatched; changing keys does not roll them back.
2. Activate the provider's new RS256 key and stop issuing old-key tokens.
3. Install the selected new public PEM and restart Fitz with it. Editing the
   file does not reload a running process's key.
4. Obtain fresh tokens and verify a scoped read. Old-key tokens must fail
   closed. Reconnect MCP clients; compatibility sessions bind to their tokens.
5. Resume callers and enable mutations after the read succeeds. Archive audit
   evidence before any retention rotation.

Fitz accepts one signing pin. Rotation uses this controlled transition and has
no automatic overlap window. Unknown, rotated, expired and wrongly scoped
credentials fail closed. No provider metadata cache exists in Fitz; its loaded
key lives for the process lifetime.

## Provider validation

On 2026-10-04 the import loaded into the pinned image. A real client-credentials
grant produced an RS256 access token containing the exact issuer, resource
audience, read scope and narrow claims above. Its signature verified against the
active provider JWK. The imported public client required S256: a request missing
the PKCE method returned `invalid_request`; an S256 request reached the login
form. Fitz's provider acceptance test separately uses the resulting credential
through the production MCP HTTP authentication wrapper.

See the [Keycloak OIDC guide](https://www.keycloak.org/securing-apps/oidc-layers)
and [26.8 administration guide](https://www.keycloak.org/docs/26.8.0/server_admin/)
for endpoint behavior and provider configuration.
