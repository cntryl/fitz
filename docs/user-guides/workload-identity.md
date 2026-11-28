# Worker authentication with a cloud identity

Workers obtain a short-lived token from their cloud and supply it through their
Fitz SDK token provider when opening a connection. The existing Wire 3 JWT
field carries the token over TCP or WebSocket. The broker verifies it and
looks up the permissions of the bound identity. SDK clients can keep their
existing CONNECT and reconnect code.

## Configure the broker

Set `FITZ_AUTH_REQUIRED=true`, `FITZ_ASSUME_EXTERNAL_TLS=true`, and
`FITZ_WORKLOAD_IDENTITY_TRUST_FILE` to the path of a JSON file. Use a trusted
TLS edge and restrict direct access to the backing Fitz listeners. Provision
each binding's `route_family` using `FITZ_ROUTE_FAMILIES`. The optional
`realm` is independent of the route-family number.

The three providers can coexist in the same file. Use the top-level
`audience` and `accounts` for AWS. The older
`FITZ_AWS_IAM_TRUST_FILE` setting accepts the AWS configuration. Set one
trust-file variable. Each Entra tenant and Google section specifies its own
audience. Workload audiences must be distinct from the broker's generic JWT
audiences.

```json
{
  "audience": "https://fitz.example/workloads/aws",
  "accounts": [{
    "account_id": "123456789012",
    "issuer": "https://example-account.tokens.sts.global.api.aws",
    "algorithms": ["RS256", "ES384"],
    "bindings": [{
      "role_arn": "arn:aws:iam::123456789012:role/workers/Orders",
      "route_family": 1,
      "permissions": ["notice://prod/orders/**#read"]
    }]
  }],
  "azure": [{
    "tenant_id": "b91c183b-e5a6-4993-a2be-717a8e609882",
    "token_version": "2.0",
    "audience": "721f24b4-62eb-44ff-9f9b-23366a63d10b",
    "bindings": [{
      "subject": "2368085c-4367-4e30-a143-e89b2c981b8e",
      "client_id": "39f67f1e-9308-4620-8911-d791436b149f",
      "route_family": 1,
      "permissions": ["notice://prod/orders/**#read"]
    }]
  }],
  "google": {
    "audience": "https://fitz.example/workloads/google",
    "bindings": [{
      "subject": "112010400000000710080",
      "route_family": 1,
      "permissions": ["notice://prod/orders/**#read"]
    }]
  }
}
```

## Single-provider environment setup

For one provider and one shared route family and permission set, use environment
variables in place of the trust file. The same authentication and TLS settings
apply. This AWS example admits any IAM role in the configured account that
presents a valid workload token for this audience.

```sh
export FITZ_AUTH_REQUIRED=true
export FITZ_ASSUME_EXTERNAL_TLS=true
export FITZ_WORKLOAD_PROVIDER=aws
export FITZ_WORKLOAD_AUDIENCE=https://fitz.example/workloads/aws
export FITZ_WORKLOAD_AWS_ACCOUNT_ID=123456789012
export FITZ_WORKLOAD_AWS_ISSUER=https://example-account.tokens.sts.global.api.aws
export FITZ_WORKLOAD_SUBJECT='*'
export FITZ_WORKLOAD_PERMISSIONS='notice://prod/orders/**#read'
```

`FITZ_WORKLOAD_ROUTE_FAMILY` defaults to `1`. Separate permissions with commas.
Use an exact role ARN as the subject to admit only that AWS role. Flat AWS
configuration uses RS256.

For `FITZ_WORKLOAD_PROVIDER=azure`, set `FITZ_WORKLOAD_AZURE_TENANT_ID`.
`FITZ_WORKLOAD_AZURE_TOKEN_VERSION` defaults to `2.0`. Use a managed
identity object ID as the subject and set `FITZ_WORKLOAD_AZURE_CLIENT_ID` to
its client ID. For `FITZ_WORKLOAD_SUBJECT='*'`, omit the client ID. This
grants the shared permissions to every valid app-only workload identity in
the configured tenant that can obtain a token for the configured audience.

For `FITZ_WORKLOAD_PROVIDER=google`, set the subject to the numeric service
account ID. Google uses a global issuer, so an exact subject is required.
Use a trust file for multiple providers or distinct bindings.

### AWS

Enable outbound identity federation, scope `sts:GetWebIdentityToken` to the
Fitz audience, and use regional STS with the attached role. Fitz checks the
account and exact role ARN. The default algorithm is RS256. Select ES384 in the
token request if the account's configured algorithm list permits it. See
[AWS IAM authentication](aws-iam.md) and the
[AWS STS API reference](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetWebIdentityToken.html).

### Azure

Register a broker API in the trusted Microsoft Entra tenant and configure it
to emit the optional `idtyp` access-token claim. The worker uses its managed
identity to request an access token for that API. Bind the service-principal
object ID as `subject` and the managed identity's client ID as `client_id`.
Fitz requires an app-only token. With v2 use the broker API client ID as
`audience`. With v1 use the audience emitted for the registered API, commonly
its application ID URI. Fitz derives the tenant-specific issuer and signing-key
URL from the configured tenant and version.
[Microsoft Entra claims validation](https://learn.microsoft.com/en-us/entra/identity-platform/claims-validation) ·
[Optional claims](https://learn.microsoft.com/en-us/entra/identity-platform/optional-claims-reference)

### Google Cloud

Use a Google-signed ID token for the configured audience from the attached
service account, for example via the metadata server. The binding uses the
numeric service-account ID shown in `sub` and `azp`. Fitz uses Google's
OIDC issuer and signing keys. [Google Cloud ID tokens](https://cloud.google.com/docs/authentication/get-id-token)

## Reconnect and qualification

On reconnect, use the SDK callback to get a fresh token. Fitz rejects expired
tokens on CONNECT and when authorizing new domain requests. Changing the trust
file takes effect after the broker restarts.

For deployment qualification, run a real worker from each provider. Verify
CONNECT over the TLS TCP and WebSocket endpoints you expose, a permitted
operation and a denied operation, denial for a different workload identity
or audience, and a successful reconnect with a newly issued token. Record
the build, provider and outcome without logging tokens.
