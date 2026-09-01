import type { AdminFeaturesResponse, AdminSearchResponse, AreaCollection, AreaDetail, Error, GlobalStats, GlobalTroubleshootingDiagnostics, KvCommittedValueResponse, KvPrefixScanResponse, KvResourceDetail, KvRowsResponse, KvStats, KvTransactionsList, LeaseResourceCollection, LeaseResourceDetail, LeaseSearchResponse, LeaseStats, LoginRequest, MessagingTopology, NoticeDeliveryObservationList, NoticeResourceCollection, NoticeResourceDetail, NoticeStats, NoticeSubscriptionsList, OperationCollection, QueueAreaCollection, QueueAreaDetail, QueueDeadLettersList, QueueInflightList, QueueRealmCollection, QueueRealmDetail, QueueResourceCollection, QueueResourceDetail, QueueStats, RealmCollection, RealmDetail, ResourceCollection, ResourceComparison, ResourceTimeline, RpcCallObservationList, RpcOperationDetail, RpcPendingList, RpcResourceCollection, RpcStats, RpcWorkersList, RuntimeDrainResponse, ScheduleExecutionObservationList, ScheduleMissedObservationList, ScheduleResourceCollection, ScheduleResourceDetail, ScheduleStats, SessionResponse, SessionsList, StreamAreaWatermarkDetail, StreamRealmWatermarkDetail, StreamRecordsResponse, StreamResourceCollection, StreamResourceDetail, StreamStats, StructuredMetricsResponse } from "./schemas";

export type GetAllMetricsResponse200 = StructuredMetricsResponse;

export type GetAllMetricsError_401 = Error;

export type GetAllMetricsError_403 = Error;

export type GetAllMetricsError_503 = Error;

export type GetAdminFeaturesResponse200 = AdminFeaturesResponse;

export type BeginRuntimeDrainResponse200 = RuntimeDrainResponse;

export type BeginRuntimeDrainError_401 = Error;

export type BeginRuntimeDrainError_403 = Error;

export type BeginRuntimeDrainError_503 = Error;

export type SearchAdminStateQuery = {
  "area"?: string;
  "domain"?: "sessions" | "kv" | "stream" | "queue" | "schedule" | "lease" | "notice" | "rpc";
  "limit"?: number;
  "operation"?: string;
  "q"?: string;
  "realm"?: string;
  "resource"?: string;
  "route_family"?: string;
};

export type SearchAdminStateResponse200 = AdminSearchResponse;

export type SearchAdminStateError_400 = Error;

export type SearchAdminStateError_401 = Error;

export type SearchAdminStateError_403 = Error;

export type SearchAdminStateError_503 = Error;

export type GetAdminSessionResponse200 = SessionResponse;

export type GetAdminSessionError_401 = Error;

export type GetAdminSessionError_503 = Error;

export type CreateAdminSessionBody = LoginRequest;

export type CreateAdminSessionResponse204 = undefined;

export type CreateAdminSessionError_400 = Error;

export type CreateAdminSessionError_401 = Error;

export type CreateAdminSessionError_503 = Error;

export type DeleteAdminSessionResponse204 = undefined;

export type ListActiveSessionsResponse200 = SessionsList;

export type ListActiveSessionsError_401 = Error;

export type ListActiveSessionsError_403 = Error;

export type ListActiveSessionsError_503 = Error;

export type GetGlobalStatsResponse200 = GlobalStats;

export type GetGlobalStatsError_401 = Error;

export type GetGlobalStatsError_403 = Error;

export type GetGlobalStatsError_503 = Error;

export type GetMessagingTopologyResponse200 = MessagingTopology;

export type GetMessagingTopologyError_401 = Error;

export type GetMessagingTopologyError_403 = Error;

export type GetMessagingTopologyError_503 = Error;

export type GetGlobalTroubleshootingGuidanceResponse200 = GlobalTroubleshootingDiagnostics;

export type GetGlobalTroubleshootingGuidanceError_401 = Error;

export type GetGlobalTroubleshootingGuidanceError_403 = Error;

export type GetGlobalTroubleshootingGuidanceError_503 = Error;

export type ListKvRealmsPath = {
  "family": string;
};

export type ListKvRealmsResponse200 = RealmCollection;

export type ListKvRealmsError_404 = Error;

export type GetKvRealmPath = {
  "family": string;
  "realm": string;
};

export type GetKvRealmResponse200 = RealmDetail;

export type GetKvRealmError_404 = Error;

export type ListKvAreasPath = {
  "family": string;
  "realm": string;
};

export type ListKvAreasResponse200 = AreaCollection;

export type ListKvAreasError_404 = Error;

export type GetKvAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetKvAreaResponse200 = AreaDetail;

export type GetKvAreaError_404 = Error;

export type ListKvResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListKvResourcesResponse200 = ResourceCollection;

export type ListKvResourcesError_404 = Error;

export type GetKvResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetKvResourceResponse200 = KvResourceDetail;

export type GetKvResourceError_404 = Error;

export type CompareKvResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareKvResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareKvResourceSnapshotsResponse200 = ResourceComparison;

export type CompareKvResourceSnapshotsError_400 = Error;

export type CompareKvResourceSnapshotsError_401 = Error;

export type CompareKvResourceSnapshotsError_404 = Error;

export type CompareKvResourceSnapshotsError_503 = Error;

export type ListKvResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListKvResourceEventsQuery = {
  "limit"?: number;
};

export type ListKvResourceEventsResponse200 = ResourceTimeline;

export type ListKvResourceEventsError_400 = Error;

export type ListKvResourceEventsError_401 = Error;

export type ListKvResourceEventsError_404 = Error;

export type ListKvResourceEventsError_503 = Error;

export type ScanKvCommittedPrefixPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ScanKvCommittedPrefixQuery = {
  "key_encoding"?: "utf8" | "base64";
  "limit"?: number;
  "prefix": string;
};

export type ScanKvCommittedPrefixResponse200 = KvPrefixScanResponse;

export type ScanKvCommittedPrefixError_400 = Error;

export type ScanKvCommittedPrefixError_401 = Error;

export type ScanKvCommittedPrefixError_403 = Error;

export type ScanKvCommittedPrefixError_404 = Error;

export type ScanKvCommittedPrefixError_503 = Error;

export type BrowseKvCommittedRowsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type BrowseKvCommittedRowsQuery = {
  "cursor"?: string;
  "key_encoding"?: "utf8" | "base64";
  "limit"?: number;
  "starts_with"?: string;
};

export type BrowseKvCommittedRowsResponse200 = KvRowsResponse;

export type BrowseKvCommittedRowsError_400 = Error;

export type BrowseKvCommittedRowsError_401 = Error;

export type BrowseKvCommittedRowsError_403 = Error;

export type BrowseKvCommittedRowsError_404 = Error;

export type BrowseKvCommittedRowsError_503 = Error;

export type ListKvTransactionsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListKvTransactionsResponse200 = KvTransactionsList;

export type ListKvTransactionsError_404 = Error;

export type GetKvCommittedValuePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetKvCommittedValueQuery = {
  "key": string;
  "key_encoding"?: "utf8" | "base64";
};

export type GetKvCommittedValueResponse200 = KvCommittedValueResponse;

export type GetKvCommittedValueError_400 = Error;

export type GetKvCommittedValueError_401 = Error;

export type GetKvCommittedValueError_403 = Error;

export type GetKvCommittedValueError_404 = Error;

export type GetKvCommittedValueError_503 = Error;

export type GetKvStatsPath = {
  "family": string;
};

export type GetKvStatsResponse200 = KvStats;

export type GetKvStatsError_401 = Error;

export type GetKvStatsError_404 = Error;

export type GetKvStatsError_503 = Error;

export type ListLeaseRealmsPath = {
  "family": string;
};

export type ListLeaseRealmsResponse200 = RealmCollection;

export type ListLeaseRealmsError_404 = Error;

export type GetLeaseRealmPath = {
  "family": string;
  "realm": string;
};

export type GetLeaseRealmResponse200 = RealmDetail;

export type GetLeaseRealmError_404 = Error;

export type ListLeaseAreasPath = {
  "family": string;
  "realm": string;
};

export type ListLeaseAreasResponse200 = AreaCollection;

export type ListLeaseAreasError_404 = Error;

export type GetLeaseAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetLeaseAreaResponse200 = AreaDetail;

export type GetLeaseAreaError_404 = Error;

export type ListLeaseResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListLeaseResourcesResponse200 = LeaseResourceCollection;

export type ListLeaseResourcesError_404 = Error;

export type GetLeaseResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetLeaseResourceResponse200 = LeaseResourceDetail;

export type GetLeaseResourceError_404 = Error;

export type CompareLeaseResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareLeaseResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareLeaseResourceSnapshotsResponse200 = ResourceComparison;

export type CompareLeaseResourceSnapshotsError_400 = Error;

export type CompareLeaseResourceSnapshotsError_401 = Error;

export type CompareLeaseResourceSnapshotsError_404 = Error;

export type CompareLeaseResourceSnapshotsError_503 = Error;

export type ListLeaseResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListLeaseResourceEventsQuery = {
  "limit"?: number;
};

export type ListLeaseResourceEventsResponse200 = ResourceTimeline;

export type ListLeaseResourceEventsError_400 = Error;

export type ListLeaseResourceEventsError_401 = Error;

export type ListLeaseResourceEventsError_404 = Error;

export type ListLeaseResourceEventsError_503 = Error;

export type SearchLeaseOwnershipPath = {
  "family": string;
};

export type SearchLeaseOwnershipQuery = {
  "area"?: string;
  "limit"?: number;
  "owner"?: string;
  "realm"?: string;
  "resource"?: string;
  "state"?: "owned" | "waiting" | "contention";
};

export type SearchLeaseOwnershipResponse200 = LeaseSearchResponse;

export type SearchLeaseOwnershipError_400 = Error;

export type SearchLeaseOwnershipError_401 = Error;

export type SearchLeaseOwnershipError_403 = Error;

export type SearchLeaseOwnershipError_404 = Error;

export type SearchLeaseOwnershipError_503 = Error;

export type GetLeaseStatsPath = {
  "family": string;
};

export type GetLeaseStatsResponse200 = LeaseStats;

export type GetLeaseStatsError_401 = Error;

export type GetLeaseStatsError_404 = Error;

export type GetLeaseStatsError_503 = Error;

export type GetFamilyMetricsPath = {
  "family": string;
};

export type GetFamilyMetricsResponse200 = StructuredMetricsResponse;

export type GetFamilyMetricsError_401 = Error;

export type GetFamilyMetricsError_403 = Error;

export type GetFamilyMetricsError_404 = Error;

export type GetFamilyMetricsError_503 = Error;

export type SearchNoticeDeliveriesPath = {
  "family": string;
};

export type SearchNoticeDeliveriesQuery = {
  "area"?: string;
  "limit"?: number;
  "q"?: string;
  "realm"?: string;
  "resource"?: string;
};

export type SearchNoticeDeliveriesResponse200 = NoticeDeliveryObservationList;

export type SearchNoticeDeliveriesError_400 = Error;

export type SearchNoticeDeliveriesError_401 = Error;

export type SearchNoticeDeliveriesError_403 = Error;

export type SearchNoticeDeliveriesError_404 = Error;

export type SearchNoticeDeliveriesError_503 = Error;

export type ListNoticeRealmsPath = {
  "family": string;
};

export type ListNoticeRealmsResponse200 = RealmCollection;

export type ListNoticeRealmsError_404 = Error;

export type GetNoticeRealmPath = {
  "family": string;
  "realm": string;
};

export type GetNoticeRealmResponse200 = RealmDetail;

export type GetNoticeRealmError_404 = Error;

export type ListNoticeAreasPath = {
  "family": string;
  "realm": string;
};

export type ListNoticeAreasResponse200 = AreaCollection;

export type ListNoticeAreasError_404 = Error;

export type GetNoticeAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetNoticeAreaResponse200 = AreaDetail;

export type GetNoticeAreaError_404 = Error;

export type ListNoticeResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListNoticeResourcesResponse200 = NoticeResourceCollection;

export type ListNoticeResourcesError_404 = Error;

export type GetNoticeResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetNoticeResourceResponse200 = NoticeResourceDetail;

export type GetNoticeResourceError_404 = Error;

export type CompareNoticeResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareNoticeResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareNoticeResourceSnapshotsResponse200 = ResourceComparison;

export type CompareNoticeResourceSnapshotsError_400 = Error;

export type CompareNoticeResourceSnapshotsError_401 = Error;

export type CompareNoticeResourceSnapshotsError_404 = Error;

export type CompareNoticeResourceSnapshotsError_503 = Error;

export type ListNoticeResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListNoticeResourceEventsQuery = {
  "limit"?: number;
};

export type ListNoticeResourceEventsResponse200 = ResourceTimeline;

export type ListNoticeResourceEventsError_400 = Error;

export type ListNoticeResourceEventsError_401 = Error;

export type ListNoticeResourceEventsError_404 = Error;

export type ListNoticeResourceEventsError_503 = Error;

export type ListNoticeSubscriptionsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListNoticeSubscriptionsResponse200 = NoticeSubscriptionsList;

export type ListNoticeSubscriptionsError_404 = Error;

export type GetNoticeStatsPath = {
  "family": string;
};

export type GetNoticeStatsResponse200 = NoticeStats;

export type GetNoticeStatsError_401 = Error;

export type GetNoticeStatsError_404 = Error;

export type GetNoticeStatsError_503 = Error;

export type ListQueueRealmsPath = {
  "family": string;
};

export type ListQueueRealmsResponse200 = QueueRealmCollection;

export type ListQueueRealmsError_404 = Error;

export type GetQueueRealmPath = {
  "family": string;
  "realm": string;
};

export type GetQueueRealmResponse200 = QueueRealmDetail;

export type GetQueueRealmError_404 = Error;

export type ListQueueAreasPath = {
  "family": string;
  "realm": string;
};

export type ListQueueAreasResponse200 = QueueAreaCollection;

export type ListQueueAreasError_404 = Error;

export type GetQueueAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetQueueAreaResponse200 = QueueAreaDetail;

export type GetQueueAreaError_404 = Error;

export type ListQueueResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListQueueResourcesResponse200 = QueueResourceCollection;

export type ListQueueResourcesError_404 = Error;

export type GetQueueResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetQueueResourceResponse200 = QueueResourceDetail;

export type GetQueueResourceError_404 = Error;

export type CompareQueueResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareQueueResourceSnapshotsQuery = {
  "against_area": string;
  "against_family"?: number;
  "against_realm": string;
  "against_resource": string;
};

export type CompareQueueResourceSnapshotsResponse200 = ResourceComparison;

export type CompareQueueResourceSnapshotsError_400 = Error;

export type CompareQueueResourceSnapshotsError_401 = Error;

export type CompareQueueResourceSnapshotsError_404 = Error;

export type CompareQueueResourceSnapshotsError_503 = Error;

export type ListQueueDeadLettersPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListQueueDeadLettersResponse200 = QueueDeadLettersList;

export type ListQueueDeadLettersError_401 = Error;

export type ListQueueDeadLettersError_404 = Error;

export type ListQueueDeadLettersError_503 = Error;

export type PurgeQueueDeadLetterPath = {
  "area": string;
  "family": string;
  "message_id": number;
  "realm": string;
  "resource": string;
};

export type PurgeQueueDeadLetterResponse204 = undefined;

export type PurgeQueueDeadLetterError_400 = Error;

export type PurgeQueueDeadLetterError_401 = Error;

export type PurgeQueueDeadLetterError_404 = undefined;

export type PurgeQueueDeadLetterError_503 = Error;

export type ReplayQueueDeadLetterPath = {
  "area": string;
  "family": string;
  "message_id": number;
  "realm": string;
  "resource": string;
};

export type ReplayQueueDeadLetterResponse204 = undefined;

export type ReplayQueueDeadLetterError_400 = Error;

export type ReplayQueueDeadLetterError_401 = Error;

export type ReplayQueueDeadLetterError_404 = undefined;

export type ReplayQueueDeadLetterError_503 = Error;

export type ListQueueResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListQueueResourceEventsQuery = {
  "limit"?: number;
};

export type ListQueueResourceEventsResponse200 = ResourceTimeline;

export type ListQueueResourceEventsError_400 = Error;

export type ListQueueResourceEventsError_401 = Error;

export type ListQueueResourceEventsError_404 = Error;

export type ListQueueResourceEventsError_503 = Error;

export type ListQueueInflightEntriesPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListQueueInflightEntriesResponse200 = QueueInflightList;

export type ListQueueInflightEntriesError_404 = Error;

export type GetQueueStatsPath = {
  "family": string;
};

export type GetQueueStatsResponse200 = QueueStats;

export type GetQueueStatsError_401 = Error;

export type GetQueueStatsError_404 = Error;

export type GetQueueStatsError_503 = Error;

export type SearchRpcCallsPath = {
  "family": string;
};

export type SearchRpcCallsQuery = {
  "area"?: string;
  "correlation_id"?: string;
  "limit"?: number;
  "operation"?: string;
  "q"?: string;
  "realm"?: string;
  "resource"?: string;
};

export type SearchRpcCallsResponse200 = RpcCallObservationList;

export type SearchRpcCallsError_400 = Error;

export type SearchRpcCallsError_401 = Error;

export type SearchRpcCallsError_403 = Error;

export type SearchRpcCallsError_404 = Error;

export type SearchRpcCallsError_503 = Error;

export type ListRpcPendingRequestsPath = {
  "family": string;
};

export type ListRpcPendingRequestsQuery = {
  "realm"?: string;
};

export type ListRpcPendingRequestsResponse200 = RpcPendingList;

export type ListRpcPendingRequestsError_401 = Error;

export type ListRpcPendingRequestsError_404 = Error;

export type ListRpcPendingRequestsError_503 = Error;

export type ListRpcRealmsPath = {
  "family": string;
};

export type ListRpcRealmsResponse200 = RealmCollection;

export type ListRpcRealmsError_404 = Error;

export type GetRpcRealmPath = {
  "family": string;
  "realm": string;
};

export type GetRpcRealmResponse200 = RealmDetail;

export type GetRpcRealmError_404 = Error;

export type ListRpcAreasPath = {
  "family": string;
  "realm": string;
};

export type ListRpcAreasResponse200 = AreaCollection;

export type ListRpcAreasError_404 = Error;

export type GetRpcAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetRpcAreaResponse200 = AreaDetail;

export type GetRpcAreaError_404 = Error;

export type ListRpcResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListRpcResourcesResponse200 = RpcResourceCollection;

export type ListRpcResourcesError_404 = Error;

export type GetRpcResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetRpcResourceResponse200 = OperationCollection;

export type GetRpcResourceError_404 = Error;

export type CompareRpcResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareRpcResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareRpcResourceSnapshotsResponse200 = ResourceComparison;

export type CompareRpcResourceSnapshotsError_400 = Error;

export type CompareRpcResourceSnapshotsError_401 = Error;

export type CompareRpcResourceSnapshotsError_404 = Error;

export type CompareRpcResourceSnapshotsError_503 = Error;

export type ListRpcResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListRpcResourceEventsQuery = {
  "limit"?: number;
};

export type ListRpcResourceEventsResponse200 = ResourceTimeline;

export type ListRpcResourceEventsError_400 = Error;

export type ListRpcResourceEventsError_401 = Error;

export type ListRpcResourceEventsError_404 = Error;

export type ListRpcResourceEventsError_503 = Error;

export type ListRpcOperationsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListRpcOperationsResponse200 = OperationCollection;

export type ListRpcOperationsError_404 = Error;

export type GetRpcOperationPath = {
  "area": string;
  "family": string;
  "operation": string;
  "realm": string;
  "resource": string;
};

export type GetRpcOperationResponse200 = RpcOperationDetail;

export type GetRpcOperationError_404 = Error;

export type ListRpcOperationWorkersPath = {
  "area": string;
  "family": string;
  "operation": string;
  "realm": string;
  "resource": string;
};

export type ListRpcOperationWorkersResponse200 = RpcWorkersList;

export type ListRpcOperationWorkersError_401 = Error;

export type ListRpcOperationWorkersError_404 = Error;

export type ListRpcOperationWorkersError_503 = Error;

export type GetRpcStatsPath = {
  "family": string;
};

export type GetRpcStatsResponse200 = RpcStats;

export type GetRpcStatsError_401 = Error;

export type GetRpcStatsError_404 = Error;

export type GetRpcStatsError_503 = Error;

export type SearchScheduleMissedHandoffsPath = {
  "family": string;
};

export type SearchScheduleMissedHandoffsQuery = {
  "area"?: string;
  "limit"?: number;
  "operation"?: string;
  "realm"?: string;
  "resource"?: string;
};

export type SearchScheduleMissedHandoffsResponse200 = ScheduleMissedObservationList;

export type SearchScheduleMissedHandoffsError_400 = Error;

export type SearchScheduleMissedHandoffsError_401 = Error;

export type SearchScheduleMissedHandoffsError_403 = Error;

export type SearchScheduleMissedHandoffsError_404 = Error;

export type SearchScheduleMissedHandoffsError_503 = Error;

export type ListScheduleRealmsPath = {
  "family": string;
};

export type ListScheduleRealmsResponse200 = RealmCollection;

export type ListScheduleRealmsError_404 = Error;

export type GetScheduleRealmPath = {
  "family": string;
  "realm": string;
};

export type GetScheduleRealmResponse200 = RealmDetail;

export type GetScheduleRealmError_404 = Error;

export type ListScheduleAreasPath = {
  "family": string;
  "realm": string;
};

export type ListScheduleAreasResponse200 = AreaCollection;

export type ListScheduleAreasError_404 = Error;

export type GetScheduleAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetScheduleAreaResponse200 = AreaDetail;

export type GetScheduleAreaError_404 = Error;

export type ListScheduleResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListScheduleResourcesResponse200 = ScheduleResourceCollection;

export type ListScheduleResourcesError_404 = Error;

export type GetScheduleResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetScheduleResourceResponse200 = ScheduleResourceDetail;

export type GetScheduleResourceError_404 = Error;

export type CompareScheduleResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareScheduleResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareScheduleResourceSnapshotsResponse200 = ResourceComparison;

export type CompareScheduleResourceSnapshotsError_400 = Error;

export type CompareScheduleResourceSnapshotsError_401 = Error;

export type CompareScheduleResourceSnapshotsError_404 = Error;

export type CompareScheduleResourceSnapshotsError_503 = Error;

export type ListScheduleResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListScheduleResourceEventsQuery = {
  "limit"?: number;
};

export type ListScheduleResourceEventsResponse200 = ResourceTimeline;

export type ListScheduleResourceEventsError_400 = Error;

export type ListScheduleResourceEventsError_401 = Error;

export type ListScheduleResourceEventsError_404 = Error;

export type ListScheduleResourceEventsError_503 = Error;

export type ListScheduleExecutionObservationsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListScheduleExecutionObservationsQuery = {
  "limit"?: number;
  "operation"?: string;
};

export type ListScheduleExecutionObservationsResponse200 = ScheduleExecutionObservationList;

export type ListScheduleExecutionObservationsError_400 = Error;

export type ListScheduleExecutionObservationsError_401 = Error;

export type ListScheduleExecutionObservationsError_403 = Error;

export type ListScheduleExecutionObservationsError_404 = Error;

export type ListScheduleExecutionObservationsError_503 = Error;

export type GetScheduleStatsPath = {
  "family": string;
};

export type GetScheduleStatsResponse200 = ScheduleStats;

export type GetScheduleStatsError_401 = Error;

export type GetScheduleStatsError_404 = Error;

export type GetScheduleStatsError_503 = Error;

export type ListFamilySessionsPath = {
  "family": string;
};

export type ListFamilySessionsResponse200 = SessionsList;

export type ListFamilySessionsError_401 = Error;

export type ListFamilySessionsError_403 = Error;

export type ListFamilySessionsError_404 = Error;

export type ListFamilySessionsError_503 = Error;

export type GetFamilyStatsPath = {
  "family": string;
};

export type GetFamilyStatsResponse200 = GlobalStats;

export type GetFamilyStatsError_401 = Error;

export type GetFamilyStatsError_403 = Error;

export type GetFamilyStatsError_404 = Error;

export type GetFamilyStatsError_503 = Error;

export type ListStreamRealmsPath = {
  "family": string;
};

export type ListStreamRealmsResponse200 = RealmCollection;

export type ListStreamRealmsError_404 = Error;

export type GetStreamRealmPath = {
  "family": string;
  "realm": string;
};

export type GetStreamRealmResponse200 = RealmDetail;

export type GetStreamRealmError_404 = Error;

export type ListStreamAreasPath = {
  "family": string;
  "realm": string;
};

export type ListStreamAreasResponse200 = AreaCollection;

export type ListStreamAreasError_404 = Error;

export type GetStreamAreaPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetStreamAreaResponse200 = AreaDetail;

export type GetStreamAreaError_404 = Error;

export type ListStreamResourcesPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type ListStreamResourcesResponse200 = StreamResourceCollection;

export type ListStreamResourcesError_404 = Error;

export type GetStreamResourcePath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type GetStreamResourceResponse200 = StreamResourceDetail;

export type GetStreamResourceError_404 = Error;

export type CompareStreamResourceSnapshotsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type CompareStreamResourceSnapshotsQuery = {
  "against_area": string;
  "against_realm": string;
  "against_resource": string;
};

export type CompareStreamResourceSnapshotsResponse200 = ResourceComparison;

export type CompareStreamResourceSnapshotsError_400 = Error;

export type CompareStreamResourceSnapshotsError_401 = Error;

export type CompareStreamResourceSnapshotsError_404 = Error;

export type CompareStreamResourceSnapshotsError_503 = Error;

export type ListStreamResourceEventsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ListStreamResourceEventsQuery = {
  "limit"?: number;
};

export type ListStreamResourceEventsResponse200 = ResourceTimeline;

export type ListStreamResourceEventsError_400 = Error;

export type ListStreamResourceEventsError_401 = Error;

export type ListStreamResourceEventsError_404 = Error;

export type ListStreamResourceEventsError_503 = Error;

export type ReadStreamResourceRecordsPath = {
  "area": string;
  "family": string;
  "realm": string;
  "resource": string;
};

export type ReadStreamResourceRecordsQuery = {
  "discriminator"?: string;
  "from_offset"?: number;
  "limit"?: number;
  "q"?: string;
};

export type ReadStreamResourceRecordsResponse200 = StreamRecordsResponse;

export type ReadStreamResourceRecordsError_400 = Error;

export type ReadStreamResourceRecordsError_401 = Error;

export type ReadStreamResourceRecordsError_403 = Error;

export type ReadStreamResourceRecordsError_404 = Error;

export type ReadStreamResourceRecordsError_503 = Error;

export type GetStreamAreaWatermarksPath = {
  "area": string;
  "family": string;
  "realm": string;
};

export type GetStreamAreaWatermarksResponse200 = StreamAreaWatermarkDetail;

export type GetStreamAreaWatermarksError_401 = Error;

export type GetStreamAreaWatermarksError_404 = Error;

export type GetStreamAreaWatermarksError_503 = Error;

export type GetStreamRealmWatermarksPath = {
  "family": string;
  "realm": string;
};

export type GetStreamRealmWatermarksResponse200 = StreamRealmWatermarkDetail;

export type GetStreamRealmWatermarksError_401 = Error;

export type GetStreamRealmWatermarksError_404 = Error;

export type GetStreamRealmWatermarksError_503 = Error;

export type SearchStreamRecordsPath = {
  "family": string;
};

export type SearchStreamRecordsQuery = {
  "area"?: string;
  "discriminator"?: string;
  "from_offset"?: number;
  "limit"?: number;
  "q"?: string;
  "realm"?: string;
  "resource"?: string;
};

export type SearchStreamRecordsResponse200 = StreamRecordsResponse;

export type SearchStreamRecordsError_400 = Error;

export type SearchStreamRecordsError_401 = Error;

export type SearchStreamRecordsError_403 = Error;

export type SearchStreamRecordsError_404 = Error;

export type SearchStreamRecordsError_503 = Error;

export type GetStreamStatsPath = {
  "family": string;
};

export type GetStreamStatsResponse200 = StreamStats;

export type GetStreamStatsError_401 = Error;

export type GetStreamStatsError_404 = Error;

export type GetStreamStatsError_503 = Error;

export type GetFamilyTopologyPath = {
  "family": string;
};

export type GetFamilyTopologyResponse200 = MessagingTopology;

export type GetFamilyTopologyError_401 = Error;

export type GetFamilyTopologyError_403 = Error;

export type GetFamilyTopologyError_404 = Error;

export type GetFamilyTopologyError_503 = Error;

export type GetFamilyTroubleshootingGuidancePath = {
  "family": string;
};

export type GetFamilyTroubleshootingGuidanceResponse200 = GlobalTroubleshootingDiagnostics;

export type GetFamilyTroubleshootingGuidanceError_401 = Error;

export type GetFamilyTroubleshootingGuidanceError_403 = Error;

export type GetFamilyTroubleshootingGuidanceError_404 = Error;

export type GetFamilyTroubleshootingGuidanceError_503 = Error;
