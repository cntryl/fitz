import type { Page } from "@playwright/test";
import {
  adminFeatures,
  domainAreasByRealm,
  domainOverviewData,
  domainResourcesByArea,
  makeDiagnosticSnapshot,
  mockAdminFeatures,
  normalizedAdminApiSegments,
  topologyApiPayload,
} from "./api-fixtures";
import {
  type ResourceDomain,
  type ResourceScope,
  leaseSearchRowsFixture,
  parseResourceScope,
  resourceDetailFixture,
  resourceTimelineFixture,
  rpcCallsFixture,
  scheduleExecutionObservationsFixture,
  scheduleMissedHandoffsFixture,
  streamRecordsFixture,
} from "./resource-fixtures";

export async function mockResourceDetailApis(
  page: Page,
  domain: ResourceDomain,
  routeScope: ResourceScope,
) {
  const diagnostics = makeDiagnosticSnapshot();

  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = normalizedAdminApiSegments(parsed.pathname);
    if (segments.length === 3 && segments[2] === "topology") {
      await route.fulfill({
        json: topologyApiPayload,
      });
      return;
    }

    if (segments[2] === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    if (segments.length < 3 || segments[2] !== domain) {
      await route.continue();
      return;
    }

    const baseFixture = domainOverviewData[domain as keyof typeof domainOverviewData];
    if (segments.length === 4 && segments[3] === "stats") {
      await route.fulfill({
        json: baseFixture.stats,
      });
      return;
    }

    if (segments.length === 4 && segments[3] === "realms") {
      await route.fulfill({
        json: { realms: baseFixture.realms },
      });
      return;
    }

    if (segments.length === 6 && segments[3] === "realms" && segments[5] === "areas") {
      const realm = decodeURIComponent(segments[4] ?? routeScope.realm);
      await route.fulfill({
        json: {
          areas: domainAreasByRealm(domain, realm).map((area) => ({ area })),
          realm,
        },
      });
      return;
    }

    if (segments.length === 8 && segments[3] === "realms" && segments[5] === "areas") {
      const realm = decodeURIComponent(segments[4] ?? routeScope.realm);
      const area = decodeURIComponent(segments[6] ?? routeScope.area);
      await route.fulfill({
        json: {
          area,
          realm,
          resources: domainResourcesByArea(domain, realm, area),
        },
      });
      return;
    }

    if (domain === "stream" && segments.length === 8) {
      if (segments[3] === "realms" && segments[5] === "areas" && segments[7] === "watermarks") {
        const realm = decodeURIComponent(segments[4] ?? "");
        const area = decodeURIComponent(segments[6] ?? "");

        if (realm === routeScope.realm && area === routeScope.area) {
          await route.fulfill({
            json: {
              area,
              family_watermarks: [{ family: 1, watermark: 20 }],
              realm,
              resource_count: 2,
            },
          });
          return;
        }
      }
    }

    if (domain === "rpc" && segments.length === 4 && segments[3] === "pending") {
      await route.fulfill({
        json: {
          requests: [
            {
              age_seconds: 7,
              correlation_id: "corr-1",
              route: "GetStatus",
              submitted_at: "2026-05-21T13:00:00.000Z",
              worker_session_id: "worker-1",
            },
          ],
        },
      });
      return;
    }

    if (domain === "rpc" && segments.length === 4 && segments[3] === "calls") {
      await route.fulfill({
        json: rpcCallsFixture({
          area: parsed.searchParams.get("area") ?? routeScope.area,
          limit: Number(parsed.searchParams.get("limit") || 200),
          operation: parsed.searchParams.get("operation"),
          realm: parsed.searchParams.get("realm") ?? routeScope.realm,
          resource: parsed.searchParams.get("resource") ?? routeScope.resource,
        }),
      });
      return;
    }

    if (domain === "lease" && segments.length === 4 && segments[3] === "search") {
      await route.fulfill({
        json: leaseSearchRowsFixture(routeScope),
      });
      return;
    }

    const scope = parseResourceScope(segments);

    if (!scope) {
      await route.continue();
      return;
    }

    if (
      scope.area !== routeScope.area ||
      scope.realm !== routeScope.realm ||
      scope.resource !== routeScope.resource
    ) {
      await route.continue();
      return;
    }

    if (segments.length === 9) {
      await route.fulfill({
        json: resourceDetailFixture(domain, scope, diagnostics),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "events") {
      await route.fulfill({
        json: resourceTimelineFixture(domain, scope),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "records" && domain === "stream") {
      await route.fulfill({
        json: streamRecordsFixture({
          area: scope.area,
          limit: Number(parsed.searchParams.get("limit") || 50),
          realm: scope.realm,
          resource: scope.resource,
        }),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "transactions" && domain === "kv") {
      await route.fulfill({
        json: {
          transactions: [
            {
              area: scope.area,
              idle_seconds: 11,
              mode: "write",
              operations_count: 4,
              realm: scope.realm,
              resource: scope.resource,
              started_at: "2026-05-21T13:00:00.000Z",
              tx_id: 101,
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "subscriptions" && domain === "notice") {
      await route.fulfill({
        json: {
          subscriptions: [
            {
              created_at: "2026-05-21T13:00:00.000Z",
              notifications_received: 8,
              pattern: "notifications/**",
              realm: scope.realm,
              session_id: "session-1",
              subscription_id: 11,
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "operations" && domain === "rpc") {
      await route.fulfill({
        json: {
          operations: [{ operation: "GetStatus" }, { operation: "SetState" }],
        },
      });
      return;
    }

    if (segments.length === 12 && domain === "rpc" && segments[9] === "operations") {
      await route.fulfill({
        json: {
          workers: [
            {
              average_latency_ms: 12,
              realm: scope.realm,
              registered_at: "2026-05-21T13:00:00.000Z",
              requests_handled: 7,
              route: decodeURIComponent(segments[10] ?? "GetStatus"),
              session_id: "worker-1",
            },
          ],
        },
      });
      return;
    }

    await route.continue();
  });
}

export async function mockScheduleResourceApis(page: Page, routeScope: ResourceScope) {
  const diagnostics = makeDiagnosticSnapshot();

  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = normalizedAdminApiSegments(parsed.pathname);
    if (segments.length === 3 && segments[2] === "topology") {
      await route.fulfill({
        json: topologyApiPayload,
      });
      return;
    }

    if (segments[2] === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    if (segments.length < 3 || segments[2] !== "schedule") {
      await route.continue();
      return;
    }

    if (segments.length === 4 && segments[3] === "stats") {
      await route.fulfill({
        json: domainOverviewData.schedule.stats,
      });
      return;
    }

    if (segments.length === 4 && segments[3] === "realms") {
      await route.fulfill({
        json: { realms: domainOverviewData.schedule.realms },
      });
      return;
    }

    if (segments.length === 4 && segments[3] === "missed") {
      await route.fulfill({
        json: scheduleMissedHandoffsFixture({
          area: parsed.searchParams.get("area") ?? routeScope.area,
          realm: parsed.searchParams.get("realm") ?? routeScope.realm,
          resource: parsed.searchParams.get("resource") ?? routeScope.resource,
        }),
      });
      return;
    }

    if (segments.length === 6 && segments[3] === "realms" && segments[5] === "areas") {
      const realm = decodeURIComponent(segments[4] ?? routeScope.realm);
      await route.fulfill({
        json: {
          areas: domainAreasByRealm("schedule", realm).map((area) => ({ area })),
          realm,
        },
      });
      return;
    }

    if (segments.length === 8 && segments[3] === "realms" && segments[5] === "areas") {
      const realm = decodeURIComponent(segments[4] ?? routeScope.realm);
      const area = decodeURIComponent(segments[6] ?? routeScope.area);
      await route.fulfill({
        json: {
          area,
          realm,
          resources: domainResourcesByArea("schedule", realm, area),
        },
      });
      return;
    }

    const scope = parseResourceScope(segments);

    if (!scope) {
      await route.continue();
      return;
    }

    if (
      scope.area !== routeScope.area ||
      scope.realm !== routeScope.realm ||
      scope.resource !== routeScope.resource
    ) {
      await route.continue();
      return;
    }

    if (segments.length === 9) {
      await route.fulfill({
        json: resourceDetailFixture("schedule", scope, diagnostics),
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "executions") {
      await route.fulfill({
        json: scheduleExecutionObservationsFixture(scope),
      });
      return;
    }

    await route.continue();
  });
}

export function queueTimelineFixture(scope: ResourceScope) {
  return {
    area: scope.area,
    derived: false,
    domain: "queue",
    events: [
      {
        age_seconds: 2,
        area: scope.area,
        attempts: 1,
        correlation_id: "corr-queue",
        domain: "queue",
        kind: "transition",
        message_id: 200,
        observed_at: "2026-05-21T13:00:00.000Z",
        operation: "Peek",
        owner_session: "session-queue-1",
        realm: scope.realm,
        resource: scope.resource,
        summary: "Queue worker activity sample.",
        worker_session: "worker-queue-1",
      },
    ],
    family: 1,
    limit: 8,
    realm: scope.realm,
    resource: scope.resource,
  };
}

export async function mockQueueResourceApis(page: Page, routeScope: ResourceScope) {
  const diagnostics = makeDiagnosticSnapshot();

  await mockAdminFeatures(page);

  await page.route("**/api/v1/**", async (route) => {
    const parsed = new URL(route.request().url());
    const segments = normalizedAdminApiSegments(parsed.pathname);
    if (segments.length === 3 && segments[2] === "topology") {
      await route.fulfill({
        json: topologyApiPayload,
      });
      return;
    }

    if (segments[2] === "features") {
      await route.fulfill({
        json: adminFeatures,
      });
      return;
    }

    if (segments.length < 9 || segments[2] !== "queue") {
      await route.continue();
      return;
    }

    const scope = parseResourceScope(segments);

    if (!scope) {
      await route.continue();
      return;
    }

    if (
      scope.area !== routeScope.area ||
      scope.realm !== routeScope.realm ||
      scope.resource !== routeScope.resource
    ) {
      await route.continue();
      return;
    }

    if (segments.length === 9) {
      await route.fulfill({
        json: {
          area: scope.area,
          backlog_age_buckets: {
            over_15m: 0,
            under_1m: 2,
            under_5m: 1,
            under_15m: 0,
          },
          complete_success_total: 12,
          delay_age_buckets: {
            over_15m: 0,
            under_1m: 0,
            under_5m: 0,
            under_15m: 0,
          },
          diagnostics,
          enqueue_success_total: 24,
          in_rate_per_second: 1.5,
          messages_dead_lettered: 0,
          messages_delayed: 1,
          messages_inflight: 2,
          messages_ready: 6,
          messages_total: 9,
          oldest_backlog_age_seconds: 28,
          oldest_message_age_seconds: 42,
          out_rate_per_second: 0.75,
          realm: scope.realm,
          resource: scope.resource,
          status: "falling_behind",
          subscriptions_active: 2,
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "inflight") {
      await route.fulfill({
        json: {
          inflight: [
            {
              area: scope.area,
              attempts: 1,
              expires_at: "2026-05-21T13:05:00.000Z",
              family: 1,
              inflight_token: "token-1",
              message_id: 101,
              realm: scope.realm,
              resource: scope.resource,
              session_id: "session-queue-1",
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "dead-letters") {
      await route.fulfill({
        json: {
          messages: [
            {
              area: scope.area,
              attempts: 2,
              dead_lettered_at: "2026-05-21T12:59:00.000Z",
              family: 1,
              message_id: 88,
              realm: scope.realm,
              reason: "Transient failure",
              resource: scope.resource,
              session_id: "session-queue-2",
            },
          ],
        },
      });
      return;
    }

    if (segments.length === 10 && segments[9] === "events") {
      await route.fulfill({
        json: queueTimelineFixture(scope),
      });
      return;
    }

    await route.continue();
  });
}

