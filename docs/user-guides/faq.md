# FAQ

## Is Fitz production-ready?

Not yet. Review [../README.md](../README.md) and the readiness documentation before a production rollout.

## How do I choose route family values?

Route families are a deployment-level isolation allowlist, not a client routing
API. Configure `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous list starting at `1`
and issue authenticated JWTs with one provisioned `fitz.route_family`. Keep route
strings stable for application semantics.

## Where is the full wire protocol?

See [../clients/client-spec.md](../clients/client-spec.md).

## Where do I start if requests fail intermittently?

Start with [troubleshooting.md](troubleshooting.md), then review [../operations/observability.md](../operations/observability.md).
