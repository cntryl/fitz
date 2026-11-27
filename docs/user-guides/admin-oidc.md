# OpenID Connect sign-in for Fitz Admin

Fitz can send an unauthenticated Admin UI visitor to one configured
OpenID Connect provider. Fitz completes the authorization code exchange on
the server with S256 PKCE, a one-time browser-bound state, and an ID-token
nonce. It checks the issuer, signatures, token lifetimes, client ID, and
admin API audience. The browser receives an opaque Fitz session cookie.
The Fitz session expires when either provider token expires or the local
session limit is reached. Signing out revokes the Fitz session. The provider
may still have its own session.

## Admin permissions

The verified admin API access token must contain a string-array
`permissions` claim. A different claim name can be configured.

| Permission | Fitz behavior |
| --- | --- |
| `fitz:admin` | Required to create an OIDC admin session |
| `route_family:1` | Grants access to the provisioned route family numbered 1 |
| `route_family:*` | Grants access to all provisioned route families and eligible global admin operations |

Grant at least one route family alongside `fitz:admin`. Numeric grants
use the canonical non-zero decimal identifier. Fitz applies the same
authorization to the UI picker and protected admin endpoints. The broker
CONNECT authentication configuration still governs broker traffic. The admin
API has its own audience, and the OIDC grant uses route-family identifiers
independently of realm labels.

## Configure Auth0

1. Under **Applications → Applications**, create a first-party **Regular Web
   Application** for Fitz Admin. Enable the connection your operators use.
   In **Settings → Allowed Callback URLs**, register the Fitz public admin
   origin plus `/api/v1/session/oidc/callback`. Enable **Authorization Code**
   and RS256 ID-token signing under **Advanced Settings**. Under
   **Credentials**, select **Client Secret (Post)**.
   [Auth0 application settings](https://auth0.com/docs/get-started/applications/application-settings)
2. Under **Applications → APIs**, create a dedicated Fitz Admin API with
   RS256 signing, for example with the identifier
   `https://fitz-admin.example.com/api`. In its **Settings → RBAC Settings**,
   turn on **Enable RBAC** and **Add Permissions in the Access Token**. If the
   API uses per-app authorization, grant the Fitz Admin application user
   access with the required permissions and request those permissions with
   `FITZ_ADMIN_OIDC_SCOPES`.
   [Auth0 RBAC settings](https://auth0.com/docs/get-started/apis/enable-role-based-access-control-for-apis) ·
   [Auth0 application access](https://auth0.com/docs/get-started/apis/api-access-policies-for-applications)
3. In the API's **Permissions** tab add `fitz:admin` and a route-family
   permission such as `route_family:1`. Create a role under
   **User Management → Roles**, attach these permissions from the Fitz Admin
   API, and assign the role under **User Management → Users → your user →
   Roles**. Use the literal `route_family:*` for operators who should have
   wildcard access.
   [Auth0 role permissions](https://auth0.com/docs/manage-users/access-control/configure-core-rbac/roles/add-permissions-to-roles) ·
   [Auth0 role assignment](https://auth0.com/docs/manage-users/access-control/configure-core-rbac/rbac-users/assign-roles-to-users)

4. Set the Fitz configuration and deploy. Supply
   `FITZ_ADMIN_OIDC_CLIENT_SECRET` through your deployment secret store.
   `FITZ_ADMIN_PUBLIC_ORIGIN` determines the callback URL and must match
   the browser's HTTPS origin. Use the exact issuer URL from discovery,
   including the trailing slash where applicable.
   [Fitz environment settings](vars.md#admin-api-authentication)

   ```sh
   export FITZ_ADMIN_AUTH_MODE=protected
   export FITZ_ADMIN_PUBLIC_ORIGIN=https://fitz-admin.example.com
   export FITZ_ASSUME_EXTERNAL_TLS=true
   export FITZ_ADMIN_OIDC_ISSUER=https://example.us.auth0.com/
   export FITZ_ADMIN_OIDC_CLIENT_ID=your-auth0-client-id
   export FITZ_ADMIN_OIDC_API_AUDIENCE=https://fitz-admin.example.com/api
   export FITZ_ADMIN_OIDC_CLIENT_AUTH_METHOD=client_secret_post
   # For a provider that requires explicit API scopes:
   # export FITZ_ADMIN_OIDC_SCOPES='fitz:admin route_family:1'
   ```

Open the Fitz UI in a fresh browser. With OIDC configured, it starts a
full-page navigation through `GET /api/v1/session/oidc/start` to the
provider. The callback returns the browser to its local destination.
A user granted `route_family:1` should see that family in the picker. If
the callback fails, confirm the application's callback URL, the API
identifier, and the user's role permissions. Sign in again after a role
change to get a fresh Fitz session.

The existing root password remains available to the protected admin session
API if configured. OIDC browser sessions are process-local and expire on
broker restart. Use the [browser deployment checklist](../operations/auth-browser-deployment.md)
for the trusted HTTPS edge.
