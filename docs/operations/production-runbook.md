# Production Runbook

This runbook describes standard operating procedures for Fitz in production.

## Startup Procedure

1. Verify config and secrets are present. Use [production-auth.md](production-auth.md) for the auth/browser perimeter baseline and [cloud-setup.md](cloud-setup.md) for storage configuration.
2. Start Fitz and wait for `/targetz` success only when using an orchestrator handoff pattern, then `/startupz` and `/healthz` or `/readyz` success for data-plane readiness. `/targetz` only proves the HTTP target is available and not draining; `/healthz` stays unhealthy until Fitz has initialized storage, acquired the active single-writer lease, validated auth configuration, completed startup, initialized durable domains, and begun accepting traffic.
3. Confirm metrics ingestion from `/metrics`.
4. Validate one authenticated client round trip on `/ws`.

## Steady-State SLO Signals

- Message latency histogram stable within target.
- No sustained growth in delivery failures.
- Mailbox depth and pending queue metrics remain bounded.
- Authentication failures remain near expected baseline.

See [operations/observability.md](observability.md) for instrumentation details.

## Incident Response

1. Classify issue: auth, routing, storage, or transport.
2. Check probes and key counters first.
3. If impact is isolated to one realm, apply realm-scoped mitigation.
4. If impact is global, reduce load and start controlled rollback.
5. Record timeline and root cause data for postmortem.

## Planned Maintenance

### ECS/Fargate Rolling Handoff

1. For ECS rolling deploys, configure the ALB target group health check to `/targetz`, set `minimumHealthyPercent=100` and `maximumPercent=200`, set target deregistration delay near `FITZ_DRAIN_GRACE_SECONDS` (for example 30 seconds), and set ECS task `stopTimeout` higher than the Fitz drain grace (for example 45 seconds).
2. Let ECS start the replacement task. The replacement can pass `/targetz` before it has the Midge writer lease; it must still reject WebSocket upgrades and TCP sessions until `/healthz` or `/readyz` is healthy.
3. Let ECS send `SIGTERM` to the old task, or explicitly call `POST /api/v1/runtime/drain` from an authenticated same-origin admin client before stopping the task.
4. Confirm `/targetz`, `/healthz`, and `/readyz` return `503` with `accepting_target_traffic` or `accepting_traffic` as `"draining"` while `/livez` remains `200`.
5. Allow Fitz to reject new TCP/WebSocket sessions, wait the configured drain grace, close active ephemeral sessions, and release the storage writer cleanly.
6. Confirm the replacement acquires the writer lease and `/healthz` or `/readyz` returns `200`, then run smoke checks.

AWS references: [ECS rolling deployments](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-ecs.html), [ECS service deployment and health parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service_definition_parameters.html), [Fargate `stopTimeout`](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html), and [ALB target group attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-target-group-attributes.html).

### Kubernetes Rolling Handoff

For standard Kubernetes Service routing, use `/readyz` or `/healthz` as the Pod readiness probe. That is the idiomatic data-plane readiness signal: the Pod should not receive client traffic until storage, auth, durable domain initialization, startup completion, and traffic acceptance have all passed.

For a single-active Fitz Pod with rolling replacement and `maxUnavailable=0`, `/readyz` can create a handoff deadlock because the replacement cannot acquire the writer lease while the old Pod remains active, and the Deployment will not remove the old Pod until the replacement is Ready. In that specific pattern, use `/targetz` as the rollout readiness gate and keep `/readyz` as the strict data-plane readiness check for load balancers, smoke checks, and client-facing gates. This accepts a short retry window: the replacement Pod may be Kubernetes Ready before it accepts WebSocket or TCP data-plane sessions.

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      terminationGracePeriodSeconds: 45
      containers:
        - name: fitz
          ports:
            - name: http
              containerPort: 4090
            - name: tcp
              containerPort: 4091
          env:
            - name: FITZ_DRAIN_GRACE_SECONDS
              value: "25"
          startupProbe:
            httpGet:
              path: /livez
              port: http
            periodSeconds: 2
            failureThreshold: 30
          livenessProbe:
            httpGet:
              path: /livez
              port: http
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /targetz
              port: http
            periodSeconds: 2
            failureThreshold: 2
```

During rollout, the replacement Pod may become Ready through `/targetz` before it owns the writer lease. That is intentional only for the handoff pattern above: WebSocket upgrades and TCP sessions still return `503` or close until `/readyz` is healthy. This avoids the single-writer rollout deadlock at the cost of a short client retry window. If that retry window is unacceptable, use `/readyz` for readiness and choose a deployment strategy that can tolerate stopping the old Pod before the replacement becomes Ready.

Kubernetes references: [liveness, readiness, and startup probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/), [Deployment rolling update settings](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), and [Pod termination flow](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/).

## Emergency Rollback

1. Stop rollout immediately.
2. Revert to last known-good image and configuration.
3. Replay validation checklist from startup procedure.
4. Keep elevated monitoring for one full traffic cycle.
