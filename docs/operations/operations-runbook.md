# Operations Runbook

This runbook covers Fitz startup, observation, replacement, drain, backup,
and rollback. Operators validate these procedures in their own environment.

## Startup Procedure

1. Verify config and secrets are present. Use [auth-browser-deployment.md](auth-browser-deployment.md) for the auth/browser perimeter baseline and [cloud-setup.md](cloud-setup.md) for storage configuration.
2. Start Fitz and wait for `/startupz` plus `/healthz` or `/readyz` before sending data-plane traffic.
3. Use `/targetz` only through a scheduling/orchestration path that can observe a lease-waiting standby without routing customer traffic to it. Never use it for the customer-facing ALB target-group health check.
4. Confirm Prometheus ingestion from the dedicated `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics` listener; use `/api/v1/{family}/metrics` for authenticated operator JSON.
5. Validate one authenticated client round trip on `/ws`.

## Steady-State Signals

- No sustained growth in delivery failures.
- Mailbox depth and pending queue metrics remain bounded.
- Authentication failures remain near expected baseline.
- Queue persistence is always best effort; producers can regenerate lost work and consumers tolerate redelivery after lost ACKs.
- `fitz_queue_fast_flush_failures_total` stays flat. Any sustained increase means Queue background persistence is failing; check disk space and storage health. The flush interval does not guarantee a maximum loss window.
- `fitz_queue_l0_admission_retries_total` stays low. It counts Queue ENQUEUE/ACK commits that Midge rejected before WAL submission for a missing L0 slot and that the actor restaged inside the same 30-second admission budget. A sustained rate means Queue writes are encountering L0 admission pressure; check storage health and compaction progress.
- `fitz_queue_admission_budget_exhausted_total` stays flat. It counts Queue ENQUEUE/ACK commits that failed because the shared 30-second storage-admission budget ran out while Midge was still stalled or still rejecting the write for a missing L0 slot. Clients received an error for those operations; treat any increase as storage saturation.
- `fitz_kv_inventory_estimate_update_failures_total` stays flat. Increases mean admin KV estimates are being repaired by rescan; user data is unaffected.
- Active Midge writer-lease health remains healthy. Fitz withdraws orchestration health and readiness and terminates if lease renewal health is lost; investigate the provider and replacement event rather than expecting in-process reacquisition.

See [observability.md](observability.md) for instrumentation details.

## Incident Response

1. Classify issue: auth, routing, storage, transport, or domain behavior.
2. Check probes and key counters first.
3. If impact is isolated to one realm, apply realm-scoped mitigation.
4. If impact is global, reduce load and start controlled rollback.
5. Record timeline and root cause data for postmortem.

## Planned Maintenance

For single-active replacement, first choose a deployment topology. A standard
one-target-group ECS rolling deployment cannot provide zero-downtime handoff:
`/healthz` keeps the standby out of traffic but cannot succeed while the old
writer holds the lease, while `/targetz` would make ALB route traffic to a task
whose data plane is closed.

For a zero-downtime-capable custom handoff:

1. Start the replacement outside the customer-facing target group, or in a blue-green target group that is not yet on the production route.
2. Let a separate controller observe `/targetz`. Expected writer-lease contention retries indefinitely with capped exponential backoff.
3. Deregister and drain the old writer with `POST /api/v1/runtime/drain` from an authenticated same-origin admin client, or have the orchestrator send SIGTERM.
4. Confirm `/targetz`, `/healthz`, and `/readyz` on the old process return `503` while `/livez` remains `200`.
5. Allow the configured drain grace to complete and the old process to release the Midge writer lease.
6. Wait for `/healthz` or `/readyz` on the replacement to return `200`, then register it or shift the production route and run smoke checks.

Use `/healthz`, never `/targetz`, as the customer-facing ALB target-group health
check. Set the external deregistration delay close to
`FITZ_DRAIN_GRACE_SECONDS`. Use 90 seconds as the process-termination-grace
baseline with the default 25-second drain grace, then increase it for the
measured peak session cleanup, domain teardown, and configured storage backend
and provider.

If separate standby orchestration or blue-green/custom lifecycle control is not
available, use a stop-first ECS deployment: stop and drain the old task, accept
the resulting downtime, and start or admit the replacement only after the old
writer exits.

A standby waiting for the writer lease remains live until it acquires ownership
or receives Ctrl-C, SIGTERM, an authenticated runtime drain request, or a fatal
shutdown request. Shutdown while waiting cancels the backoff and closes the
early HTTP and metrics listeners without waiting the active-broker drain grace.
An in-flight Midge open or recovery attempt is always joined before exit so it
cannot acquire the lease later from a detached blocking task. Midge does not
cooperatively cancel that operation, so shutdown may take the backend or
provider's worst-case time and can exceed the 90-second baseline.

Shutdown modes are intentionally different:

- SIGTERM and `POST /api/v1/runtime/drain` gracefully withdraw traffic, wait `FITZ_DRAIN_GRACE_SECONDS` after full startup, and then clean up sessions, listeners, domains, and Midge.
- Ctrl-C and fatal actor or active writer-lease-health failures skip the drain delay and begin the same explicit cleanup immediately.

During active service, when Midge reports a lease-renewal health failure, Fitz
withdraws `/targetz`, `/healthz`, and `/readyz` and terminates through the fatal
path. Do not wait for a second signal and do not expect that process to reacquire
the lease; verify that the orchestrator starts or promotes a healthy replacement.

Provider-backed cloud renewal has an independent monotonic deadline in Midge,
and malformed lease expiration fails closed as indeterminate. The storage
writer lease TTL defaults to 30 seconds and is configured with
`FITZ_STORAGE_LEASE_TTL_SECS`; Midge renews at one third of that value. A longer
TTL reduces provider coordination requests but delays takeover after a crash or
failed release. Successful graceful shutdown conditionally expires the lease
immediately.

## S3 Backup and Point-in-Time Recovery

For a Fitz deployment on AWS S3, use AWS Backup to protect the whole bucket
containing the Midge database. Qualify this procedure with the deployed Fitz
image and its embedded Midge version. AWS Backup defines a recovery point for
each bucket. For applications that span multiple Fitz deployments or other
datastores, coordinate and verify the application recovery point across them.
See the [Midge backup guide](https://github.com/cntryl/midge/blob/main/docs/user-guides/backup-and-restore.md)
for the database storage requirements.

### Configure backup

1. Enable S3 Versioning and apply noncurrent-version expiration. Start with
   seven days for retired WAL and SST data and a shorter window for old
   `midge_primary_lease.json` versions. Keep every current Midge object out of
   age-based expiration. When enabling versioning for the first time, stop the
   bucket's writers and allow [15 minutes for propagation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/manage-versioning-examples.html)
   before resuming writes or deletes.
2. Assign the bucket to one AWS Backup continuous backup rule with **7 days**
   of retention. Enable the bucket's Amazon EventBridge notifications. Add
   daily snapshots in the same vault if you need dated recovery points beyond
   that window. Source lifecycle and vault retention are separate settings.
   [AWS S3 backup types](https://docs.aws.amazon.com/aws-backup/latest/devguide/s3-backups.html#s3-backup-types-comparison)
3. Configure [backup alerts](#backup-alerts) when you enable the plan.

### Backup alerts

Send vault notifications to a standard SNS topic subscribed by the on-call team.
Use a topic in the vault's account and Region, allow `backup.amazonaws.com`
to publish to it, and confirm the subscription. For an encrypted topic,
also configure the KMS permissions required for AWS Backup to publish.
In the vault's account and Region, set `BACKUP_VAULT` and
`BACKUP_ALERT_TOPIC_ARN`. Check the existing configuration and preserve any
events other operators already use. [AWS Backup notifications](https://docs.aws.amazon.com/aws-backup/latest/devguide/backup-notifications.html)

```sh
aws backup get-backup-vault-notifications --backup-vault-name "$BACKUP_VAULT"

aws backup put-backup-vault-notifications \
  --backup-vault-name "$BACKUP_VAULT" \
  --sns-topic-arn "$BACKUP_ALERT_TOPIC_ARN" \
  --backup-vault-events BACKUP_JOB_FAILED BACKUP_JOB_EXPIRED \
    CONTINUOUS_BACKUP_INTERRUPTED RESTORE_JOB_FAILED \
    S3_BACKUP_OBJECT_FAILED S3_RESTORE_OBJECT_FAILED
```

Test delivery to the on-call subscriber, then verify AWS Backup event delivery
with an isolated test job. Group object-level failures by job ID so one
incomplete restore produces one incident. Review jobs that finish with issues
as well as failed jobs. In the AWS Backup API, a backup shown as **Completed with
issues** has state `COMPLETED` with a `MessageCategory` other than `SUCCESS`.
In a shared vault, use bucket-scoped job queries so another bucket's success
cannot conceal missing Fitz backups. [AWS Backup job API](https://docs.aws.amazon.com/aws-backup/latest/APIReference/API_ListBackupJobs.html)

Check freshness independently of event delivery. Use the protected S3
resource in AWS Backup. Open the continuous recovery point's restore
dialog to check its available time range without submitting a restore.
Use the following thresholds as examples and set alerts based on your
recovery objectives. [Reading the restore window](https://docs.aws.amazon.com/aws-backup/latest/devguide/point-in-time-recovery-restoring.html)

| Signal | Example trigger and response |
| --- | --- |
| Continuous coverage | Page if the continuous point is missing or stopped after initial setup. Warn if the latest restorable time falls more than 30 minutes behind and page at 60 minutes. Check the bucket's EventBridge notifications, permissions, and whether AWS has fallen back to snapshots. |
| Daily snapshot freshness | Alert when the last successful snapshot is older than the daily interval plus your expected scheduling and completion delay, for example 26 hours with two hours of slack. Use the recorded recovery point time and check for expired or failed jobs. |
| Restore verification | Run the [recovery drill](#verify-a-recovery-point) before relying on the plan, after changes to the deployed image or backup configuration, and on a regular schedule such as monthly. Alert on a missed drill, failed data checks, or recovery that exceeds the time objective. A completed AWS restore job still needs Fitz data checks. |

Review the oldest restorable time as the seven-day window fills. Record the
bucket, recovery point ARN, latest restorable time, and job error in any
backup incident. AWS documents about 15 minutes of lag at the newest end
of the S3 continuous restore window. [AWS point-in-time recovery](https://docs.aws.amazon.com/aws-backup/latest/devguide/point-in-time-recovery.html)

### Verify a recovery point

1. For a drill, commit a known KV value with sync durability. Record a UTC
   restore time after Fitz acknowledges the commit. Submit a second sync
   commit for a different key after that time. Wait for the selected time to
   enter the available recovery window.
2. Restore the selected point into a new, empty, versioned bucket. Use the
   entire bucket and the latest object version at that point. Inspect the job
   details for skipped or failed objects. Provision the target's access,
   encryption, and lifecycle settings before opening it. [AWS S3 restore](https://docs.aws.amazon.com/aws-backup/latest/devguide/restoring-s3.html)
3. Run an isolated Fitz task with the same image and route-family configuration.
   Set `FITZ_STORAGE_BUCKET` to the target bucket, keep the original
   `FITZ_STORAGE_PREFIX`, and set `FITZ_STORAGE_CACHE_PATH` to an empty
   directory. Keep production traffic and workers disconnected.
4. Wait for `/readyz` or `/healthz`. Read both test keys through Fitz. The first
   value must be present and the second absent. Check representative state in
   each persistent domain you use. Readiness alone can succeed against an
   empty prefix. Record the recovery point and selected timestamp, the
   recovered values, and the elapsed time to verified readiness.

### Resume service

Drain and stop the original writer before routing production traffic to the
restored task. The two buckets have independent writer leases. Reconnect
clients and resume workers after checking the effects of rollback.

| Domain | Operator check |
| --- | --- |
| KV | Verify the expected values and have clients reopen transactions and watches. |
| Stream | Reconcile client-owned checkpoints with the restored history before resuming readers. A checkpoint can refer to events beyond the restored tail. |
| Queue | Reconcile accepted work and acknowledgements with the producer or consumer record. Queue uses best-effort persistence, and restored messages can redeliver. |
| Schedule | Reconcile pending claims with actions already performed outside Fitz before reconnecting subscribers. |

See [recovery internals](../development/recovery-internals.md) for live session
ownership and each domain's startup behavior. After the restored broker accepts
writes, returning to the original bucket requires reconciling those writes.

## Emergency Rollback

1. Stop rollout immediately.
2. Revert to the last known-good image and configuration.
3. Replay validation from the startup procedure.
4. Keep elevated monitoring for one full traffic cycle.
