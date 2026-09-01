import { defineApi, createClient, del, empty, get, json, post } from "@askrjs/fetch";
import type { ClientOptions } from "@askrjs/fetch";
import type { AdminFeaturesResponse, AdminSearchResponse, AreaCollection, AreaDetail, Error, GlobalStats, GlobalTroubleshootingDiagnostics, KvCommittedValueResponse, KvPrefixScanResponse, KvResourceDetail, KvRowsResponse, KvStats, KvTransactionsList, LeaseResourceCollection, LeaseResourceDetail, LeaseSearchResponse, LeaseStats, LoginRequest, MessagingTopology, NoticeDeliveryObservationList, NoticeResourceCollection, NoticeResourceDetail, NoticeStats, NoticeSubscriptionsList, OperationCollection, QueueAreaCollection, QueueAreaDetail, QueueDeadLettersList, QueueInflightList, QueueRealmCollection, QueueRealmDetail, QueueResourceCollection, QueueResourceDetail, QueueStats, RealmCollection, RealmDetail, ResourceCollection, ResourceComparison, ResourceTimeline, RpcCallObservationList, RpcOperationDetail, RpcPendingList, RpcResourceCollection, RpcStats, RpcWorkersList, RuntimeDrainResponse, ScheduleExecutionObservationList, ScheduleMissedObservationList, ScheduleResourceCollection, ScheduleResourceDetail, ScheduleStats, SessionResponse, SessionsList, StreamAreaWatermarkDetail, StreamRealmWatermarkDetail, StreamRecordsResponse, StreamResourceCollection, StreamResourceDetail, StreamStats, StructuredMetricsResponse } from "./schemas";
import type { BrowseKvCommittedRowsPath, BrowseKvCommittedRowsQuery, CompareKvResourceSnapshotsPath, CompareKvResourceSnapshotsQuery, CompareLeaseResourceSnapshotsPath, CompareLeaseResourceSnapshotsQuery, CompareNoticeResourceSnapshotsPath, CompareNoticeResourceSnapshotsQuery, CompareQueueResourceSnapshotsPath, CompareQueueResourceSnapshotsQuery, CompareRpcResourceSnapshotsPath, CompareRpcResourceSnapshotsQuery, CompareScheduleResourceSnapshotsPath, CompareScheduleResourceSnapshotsQuery, CompareStreamResourceSnapshotsPath, CompareStreamResourceSnapshotsQuery, GetFamilyMetricsPath, GetFamilyStatsPath, GetFamilyTopologyPath, GetFamilyTroubleshootingGuidancePath, GetKvAreaPath, GetKvCommittedValuePath, GetKvCommittedValueQuery, GetKvRealmPath, GetKvResourcePath, GetKvStatsPath, GetLeaseAreaPath, GetLeaseRealmPath, GetLeaseResourcePath, GetLeaseStatsPath, GetNoticeAreaPath, GetNoticeRealmPath, GetNoticeResourcePath, GetNoticeStatsPath, GetQueueAreaPath, GetQueueRealmPath, GetQueueResourcePath, GetQueueStatsPath, GetRpcAreaPath, GetRpcOperationPath, GetRpcRealmPath, GetRpcResourcePath, GetRpcStatsPath, GetScheduleAreaPath, GetScheduleRealmPath, GetScheduleResourcePath, GetScheduleStatsPath, GetStreamAreaPath, GetStreamAreaWatermarksPath, GetStreamRealmPath, GetStreamRealmWatermarksPath, GetStreamResourcePath, GetStreamStatsPath, ListFamilySessionsPath, ListKvAreasPath, ListKvRealmsPath, ListKvResourceEventsPath, ListKvResourceEventsQuery, ListKvResourcesPath, ListKvTransactionsPath, ListLeaseAreasPath, ListLeaseRealmsPath, ListLeaseResourceEventsPath, ListLeaseResourceEventsQuery, ListLeaseResourcesPath, ListNoticeAreasPath, ListNoticeRealmsPath, ListNoticeResourceEventsPath, ListNoticeResourceEventsQuery, ListNoticeResourcesPath, ListNoticeSubscriptionsPath, ListQueueAreasPath, ListQueueDeadLettersPath, ListQueueInflightEntriesPath, ListQueueRealmsPath, ListQueueResourceEventsPath, ListQueueResourceEventsQuery, ListQueueResourcesPath, ListRpcAreasPath, ListRpcOperationWorkersPath, ListRpcOperationsPath, ListRpcPendingRequestsPath, ListRpcPendingRequestsQuery, ListRpcRealmsPath, ListRpcResourceEventsPath, ListRpcResourceEventsQuery, ListRpcResourcesPath, ListScheduleAreasPath, ListScheduleExecutionObservationsPath, ListScheduleExecutionObservationsQuery, ListScheduleRealmsPath, ListScheduleResourceEventsPath, ListScheduleResourceEventsQuery, ListScheduleResourcesPath, ListStreamAreasPath, ListStreamRealmsPath, ListStreamResourceEventsPath, ListStreamResourceEventsQuery, ListStreamResourcesPath, PurgeQueueDeadLetterPath, ReadStreamResourceRecordsPath, ReadStreamResourceRecordsQuery, ReplayQueueDeadLetterPath, ScanKvCommittedPrefixPath, ScanKvCommittedPrefixQuery, SearchAdminStateQuery, SearchLeaseOwnershipPath, SearchLeaseOwnershipQuery, SearchNoticeDeliveriesPath, SearchNoticeDeliveriesQuery, SearchRpcCallsPath, SearchRpcCallsQuery, SearchScheduleMissedHandoffsPath, SearchScheduleMissedHandoffsQuery, SearchStreamRecordsPath, SearchStreamRecordsQuery } from "./operations";

export const api = defineApi({
  getAllMetrics: get("/api/v1/all/metrics")
    .returns(json<StructuredMetricsResponse>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getAdminFeatures: get("/api/v1/features")
    .returns(json<AdminFeaturesResponse>())
    .security([]),
  beginRuntimeDrain: post("/api/v1/runtime/drain")
    .returns(json<RuntimeDrainResponse>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchAdminState: get("/api/v1/search")
    .query<SearchAdminStateQuery>({ "area": { style: "form", explode: true }, "domain": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "operation": { style: "form", explode: true }, "q": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true }, "route_family": { style: "form", explode: true } })
    .returns(json<AdminSearchResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getAdminSession: get("/api/v1/session")
    .returns(json<SessionResponse>())
    .errors({ "401": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  createAdminSession: post("/api/v1/session")
    .body(json<LoginRequest>())
    .returns(204, empty())
    .errors({ "400": json<Error>(), "401": json<Error>(), "503": json<Error>() })
    .security([]),
  deleteAdminSession: del("/api/v1/session")
    .returns(204, empty())
    .security([]),
  listActiveSessions: get("/api/v1/sessions")
    .returns(json<SessionsList>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getGlobalStats: get("/api/v1/stats")
    .returns(json<GlobalStats>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getMessagingTopology: get("/api/v1/topology")
    .returns(json<MessagingTopology>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getGlobalTroubleshootingGuidance: get("/api/v1/troubleshooting")
    .returns(json<GlobalTroubleshootingDiagnostics>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listKvRealms: get("/api/v1/{family}/kv/realms")
    .params<ListKvRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getKvRealm: get("/api/v1/{family}/kv/realms/{realm}")
    .params<GetKvRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listKvAreas: get("/api/v1/{family}/kv/realms/{realm}/areas")
    .params<ListKvAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getKvArea: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}")
    .params<GetKvAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listKvResources: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources")
    .params<ListKvResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<ResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getKvResource: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetKvResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<KvResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareKvResourceSnapshots: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareKvResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareKvResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listKvResourceEvents: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListKvResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListKvResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  scanKvCommittedPrefix: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/prefix")
    .params<ScanKvCommittedPrefixPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ScanKvCommittedPrefixQuery>({ "key_encoding": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "prefix": { style: "form", explode: true } })
    .returns(json<KvPrefixScanResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  browseKvCommittedRows: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/rows")
    .params<BrowseKvCommittedRowsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<BrowseKvCommittedRowsQuery>({ "cursor": { style: "form", explode: true }, "key_encoding": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "starts_with": { style: "form", explode: true } })
    .returns(json<KvRowsResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listKvTransactions: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/transactions")
    .params<ListKvTransactionsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<KvTransactionsList>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getKvCommittedValue: get("/api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/value")
    .params<GetKvCommittedValuePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<GetKvCommittedValueQuery>({ "key": { style: "form", explode: true }, "key_encoding": { style: "form", explode: true } })
    .returns(json<KvCommittedValueResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getKvStats: get("/api/v1/{family}/kv/stats")
    .params<GetKvStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<KvStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listLeaseRealms: get("/api/v1/{family}/lease/realms")
    .params<ListLeaseRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getLeaseRealm: get("/api/v1/{family}/lease/realms/{realm}")
    .params<GetLeaseRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listLeaseAreas: get("/api/v1/{family}/lease/realms/{realm}/areas")
    .params<ListLeaseAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getLeaseArea: get("/api/v1/{family}/lease/realms/{realm}/areas/{area}")
    .params<GetLeaseAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listLeaseResources: get("/api/v1/{family}/lease/realms/{realm}/areas/{area}/resources")
    .params<ListLeaseResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<LeaseResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getLeaseResource: get("/api/v1/{family}/lease/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetLeaseResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<LeaseResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareLeaseResourceSnapshots: get("/api/v1/{family}/lease/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareLeaseResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareLeaseResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listLeaseResourceEvents: get("/api/v1/{family}/lease/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListLeaseResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListLeaseResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchLeaseOwnership: get("/api/v1/{family}/lease/search")
    .params<SearchLeaseOwnershipPath>({ "family": { style: "simple", explode: false } })
    .query<SearchLeaseOwnershipQuery>({ "area": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "owner": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true }, "state": { style: "form", explode: true } })
    .returns(json<LeaseSearchResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getLeaseStats: get("/api/v1/{family}/lease/stats")
    .params<GetLeaseStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<LeaseStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getFamilyMetrics: get("/api/v1/{family}/metrics")
    .params<GetFamilyMetricsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<StructuredMetricsResponse>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchNoticeDeliveries: get("/api/v1/{family}/notice/deliveries")
    .params<SearchNoticeDeliveriesPath>({ "family": { style: "simple", explode: false } })
    .query<SearchNoticeDeliveriesQuery>({ "area": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "q": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true } })
    .returns(json<NoticeDeliveryObservationList>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listNoticeRealms: get("/api/v1/{family}/notice/realms")
    .params<ListNoticeRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getNoticeRealm: get("/api/v1/{family}/notice/realms/{realm}")
    .params<GetNoticeRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listNoticeAreas: get("/api/v1/{family}/notice/realms/{realm}/areas")
    .params<ListNoticeAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getNoticeArea: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}")
    .params<GetNoticeAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listNoticeResources: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}/resources")
    .params<ListNoticeResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<NoticeResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getNoticeResource: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetNoticeResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<NoticeResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareNoticeResourceSnapshots: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareNoticeResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareNoticeResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listNoticeResourceEvents: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListNoticeResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListNoticeResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listNoticeSubscriptions: get("/api/v1/{family}/notice/realms/{realm}/areas/{area}/resources/{resource}/subscriptions")
    .params<ListNoticeSubscriptionsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<NoticeSubscriptionsList>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getNoticeStats: get("/api/v1/{family}/notice/stats")
    .params<GetNoticeStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<NoticeStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueRealms: get("/api/v1/{family}/queue/realms")
    .params<ListQueueRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<QueueRealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getQueueRealm: get("/api/v1/{family}/queue/realms/{realm}")
    .params<GetQueueRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<QueueRealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueAreas: get("/api/v1/{family}/queue/realms/{realm}/areas")
    .params<ListQueueAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<QueueAreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getQueueArea: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}")
    .params<GetQueueAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<QueueAreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueResources: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources")
    .params<ListQueueResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<QueueResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getQueueResource: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetQueueResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<QueueResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareQueueResourceSnapshots: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareQueueResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareQueueResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_family": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueDeadLetters: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters")
    .params<ListQueueDeadLettersPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<QueueDeadLettersList>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  purgeQueueDeadLetter: del("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}")
    .params<PurgeQueueDeadLetterPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "message_id": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(204, empty())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": empty(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  replayQueueDeadLetter: post("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}/replay")
    .params<ReplayQueueDeadLetterPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "message_id": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(204, empty())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": empty(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueResourceEvents: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListQueueResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListQueueResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listQueueInflightEntries: get("/api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/inflight")
    .params<ListQueueInflightEntriesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<QueueInflightList>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getQueueStats: get("/api/v1/{family}/queue/stats")
    .params<GetQueueStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<QueueStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchRpcCalls: get("/api/v1/{family}/rpc/calls")
    .params<SearchRpcCallsPath>({ "family": { style: "simple", explode: false } })
    .query<SearchRpcCallsQuery>({ "area": { style: "form", explode: true }, "correlation_id": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "operation": { style: "form", explode: true }, "q": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true } })
    .returns(json<RpcCallObservationList>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcPendingRequests: get("/api/v1/{family}/rpc/pending")
    .params<ListRpcPendingRequestsPath>({ "family": { style: "simple", explode: false } })
    .query<ListRpcPendingRequestsQuery>({ "realm": { style: "form", explode: true } })
    .returns(json<RpcPendingList>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcRealms: get("/api/v1/{family}/rpc/realms")
    .params<ListRpcRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getRpcRealm: get("/api/v1/{family}/rpc/realms/{realm}")
    .params<GetRpcRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcAreas: get("/api/v1/{family}/rpc/realms/{realm}/areas")
    .params<ListRpcAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getRpcArea: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}")
    .params<GetRpcAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcResources: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources")
    .params<ListRpcResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RpcResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getRpcResource: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetRpcResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<OperationCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareRpcResourceSnapshots: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareRpcResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareRpcResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcResourceEvents: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListRpcResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListRpcResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcOperations: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations")
    .params<ListRpcOperationsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<OperationCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getRpcOperation: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}")
    .params<GetRpcOperationPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "operation": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<RpcOperationDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listRpcOperationWorkers: get("/api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}/workers")
    .params<ListRpcOperationWorkersPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "operation": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<RpcWorkersList>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getRpcStats: get("/api/v1/{family}/rpc/stats")
    .params<GetRpcStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RpcStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchScheduleMissedHandoffs: get("/api/v1/{family}/schedule/missed")
    .params<SearchScheduleMissedHandoffsPath>({ "family": { style: "simple", explode: false } })
    .query<SearchScheduleMissedHandoffsQuery>({ "area": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "operation": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true } })
    .returns(json<ScheduleMissedObservationList>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listScheduleRealms: get("/api/v1/{family}/schedule/realms")
    .params<ListScheduleRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getScheduleRealm: get("/api/v1/{family}/schedule/realms/{realm}")
    .params<GetScheduleRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listScheduleAreas: get("/api/v1/{family}/schedule/realms/{realm}/areas")
    .params<ListScheduleAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getScheduleArea: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}")
    .params<GetScheduleAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listScheduleResources: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources")
    .params<ListScheduleResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<ScheduleResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getScheduleResource: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetScheduleResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<ScheduleResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareScheduleResourceSnapshots: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareScheduleResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareScheduleResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listScheduleResourceEvents: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListScheduleResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListScheduleResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listScheduleExecutionObservations: get("/api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}/executions")
    .params<ListScheduleExecutionObservationsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListScheduleExecutionObservationsQuery>({ "limit": { style: "form", explode: true }, "offset": { style: "form", explode: true }, "operation": { style: "form", explode: true } })
    .returns(json<ScheduleExecutionObservationList>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getScheduleStats: get("/api/v1/{family}/schedule/stats")
    .params<GetScheduleStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<ScheduleStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listFamilySessions: get("/api/v1/{family}/sessions")
    .params<ListFamilySessionsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<SessionsList>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getFamilyStats: get("/api/v1/{family}/stats")
    .params<GetFamilyStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<GlobalStats>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listStreamRealms: get("/api/v1/{family}/stream/realms")
    .params<ListStreamRealmsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<RealmCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamRealm: get("/api/v1/{family}/stream/realms/{realm}")
    .params<GetStreamRealmPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<RealmDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listStreamAreas: get("/api/v1/{family}/stream/realms/{realm}/areas")
    .params<ListStreamAreasPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamArea: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}")
    .params<GetStreamAreaPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<AreaDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listStreamResources: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/resources")
    .params<ListStreamResourcesPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<StreamResourceCollection>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamResource: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/resources/{resource}")
    .params<GetStreamResourcePath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .returns(json<StreamResourceDetail>())
    .errors({ "404": json<Error>() })
    .security([{"sessionCookie":[]}]),
  compareStreamResourceSnapshots: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/resources/{resource}/compare")
    .params<CompareStreamResourceSnapshotsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<CompareStreamResourceSnapshotsQuery>({ "against_area": { style: "form", explode: true }, "against_realm": { style: "form", explode: true }, "against_resource": { style: "form", explode: true } })
    .returns(json<ResourceComparison>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  listStreamResourceEvents: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/resources/{resource}/events")
    .params<ListStreamResourceEventsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ListStreamResourceEventsQuery>({ "limit": { style: "form", explode: true } })
    .returns(json<ResourceTimeline>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  readStreamResourceRecords: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/resources/{resource}/records")
    .params<ReadStreamResourceRecordsPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false }, "resource": { style: "simple", explode: false } })
    .query<ReadStreamResourceRecordsQuery>({ "discriminator": { style: "form", explode: true }, "from_offset": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "q": { style: "form", explode: true } })
    .returns(json<StreamRecordsResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamAreaWatermarks: get("/api/v1/{family}/stream/realms/{realm}/areas/{area}/watermarks")
    .params<GetStreamAreaWatermarksPath>({ "area": { style: "simple", explode: false }, "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<StreamAreaWatermarkDetail>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamRealmWatermarks: get("/api/v1/{family}/stream/realms/{realm}/watermarks")
    .params<GetStreamRealmWatermarksPath>({ "family": { style: "simple", explode: false }, "realm": { style: "simple", explode: false } })
    .returns(json<StreamRealmWatermarkDetail>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  searchStreamRecords: get("/api/v1/{family}/stream/search")
    .params<SearchStreamRecordsPath>({ "family": { style: "simple", explode: false } })
    .query<SearchStreamRecordsQuery>({ "area": { style: "form", explode: true }, "discriminator": { style: "form", explode: true }, "from_offset": { style: "form", explode: true }, "limit": { style: "form", explode: true }, "q": { style: "form", explode: true }, "realm": { style: "form", explode: true }, "resource": { style: "form", explode: true } })
    .returns(json<StreamRecordsResponse>())
    .errors({ "400": json<Error>(), "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getStreamStats: get("/api/v1/{family}/stream/stats")
    .params<GetStreamStatsPath>({ "family": { style: "simple", explode: false } })
    .returns(json<StreamStats>())
    .errors({ "401": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getFamilyTopology: get("/api/v1/{family}/topology")
    .params<GetFamilyTopologyPath>({ "family": { style: "simple", explode: false } })
    .returns(json<MessagingTopology>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
  getFamilyTroubleshootingGuidance: get("/api/v1/{family}/troubleshooting")
    .params<GetFamilyTroubleshootingGuidancePath>({ "family": { style: "simple", explode: false } })
    .returns(json<GlobalTroubleshootingDiagnostics>())
    .errors({ "401": json<Error>(), "403": json<Error>(), "404": json<Error>(), "503": json<Error>() })
    .security([{"sessionCookie":[]}]),
}, {
  "servers": [
    "/"
  ],
  "securitySchemes": {
    "sessionCookie": {
      "type": "apiKey",
      "in": "cookie",
      "name": "fitz_admin_session",
      "description": "Admin session JWT cookie issued by `POST /api/v1/session`."
    }
  }
});

export const createApiClient = (options?: ClientOptions) => createClient(api, options);
