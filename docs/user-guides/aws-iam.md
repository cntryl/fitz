# AWS IAM authentication for broker clients

Fitz can authenticate a worker using a short-lived AWS STS outbound identity
token in its Wire 3 CONNECT JWT field. The worker can use its existing ECS task
role, EC2 instance role, or another attached role that can request an identity
token. The broker maps an exact role ARN to a local route family and permission
set.

For a broker that also accepts Azure or Google Cloud identities, see the
[cloud workload identity guide](workload-identity.md).

## AWS setup

Enable outbound identity federation in each trusted AWS account. Record the
account's issuer URL and choose an audience dedicated to this Fitz deployment.
Give the worker role `sts:GetWebIdentityToken` permission, constrained to that
audience, the ordinary AWS identity subject, and the needed token lifetime.
Request the token from regional STS with `SigningAlgorithm=RS256` and a lifetime
of 60–300 seconds. Use the task role for an ECS worker. The task execution role
supplies permissions to the container platform.

[AWS setup](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_outbound_getting_started.html) ·
[STS API](https://docs.aws.amazon.com/STS/latest/APIReference/API_GetWebIdentityToken.html) ·
[Token claims](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_providers_outbound_token_claims.html)

## Broker setup

Set `FITZ_AUTH_REQUIRED=true`, `FITZ_ASSUME_EXTERNAL_TLS=true`, and
`FITZ_AWS_IAM_TRUST_FILE` to a readable JSON file. Terminate TLS at a trusted
edge for both WebSocket and TCP and restrict the backend listeners to that
edge. Clients verify the TLS certificate and hostname. Leave the TCP listener
disabled when only WebSocket access is needed.

```json
{
  "audience": "https://orders.fitz.example/workload",
  "accounts": [
    {
      "account_id": "123456789012",
      "issuer": "https://example-account.tokens.sts.global.api.aws",
      "bindings": [
        {
          "role_arn": "arn:aws:iam::123456789012:role/workers/Orders",
          "route_family": 2,
          "realm": "prod",
          "permissions": ["notice://prod/orders/**#read"]
        }
      ]
    }
  ]
}
```

Provision the bound family in `FITZ_ROUTE_FAMILIES`. The optional realm and the
route-family number are independent. Grant access using explicit route
permissions. Fitz checks the account, issuer, exact role ARN, signed subject
and requesting principal, audience, and time claims. It uses the issuer's
HTTPS JWKS endpoint and an exact key ID. Token claims such as scopes and tags
supply no Fitz permissions.

An IAM-only deployment needs no generic JWT route-family map. For a deployment
with both AWS workers and external JWT clients, retain the existing JWKS and
route-family map for external clients and choose a distinct workload audience.
Use the Admin UI's own auth configuration for administrator access.

## Connecting and refreshing

Use the existing SDK token provider to call regional STS whenever the SDK
opens a new session. Pass only the returned `WebIdentityToken` to the provider
callback. Keep tokens out of logs. The standard callback entry points are
`TokenProvider` in .NET, the provider argument to `Dial` in Go,
`token_provider` in Python and Rust, and `tokenProvider` in TypeScript.

Fitz verifies the token on each CONNECT and fixes the permission snapshot for
that connection. An expired token cannot open a new session or authorize new
domain operations on an established session. Expiration does not itself close
the socket or remove existing subscriptions. Restart the broker to apply
changes to the trust file, then reconnect with a fresh token.
