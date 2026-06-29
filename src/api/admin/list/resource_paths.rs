use super::*;

#[derive(Debug, Clone)]
pub struct ResourcePath<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
}

#[derive(Debug, Clone)]
pub struct RpcOperationPath<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub operation: &'a str,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedRpcOperation {
    pub(crate) realm: String,
    pub(crate) area: String,
    pub(crate) resource: String,
    pub(crate) operation: String,
}

impl ResourcePath<'_> {
    pub(super) fn matches(&self, realm: &str, area: &str, resource: &str) -> bool {
        self.realm == realm && self.area == area && self.resource == resource
    }
}

impl ResourceRef {
    pub(super) fn new(realm: String, area: String, resource: String) -> Self {
        Self {
            realm,
            area,
            resource,
        }
    }

    pub(super) fn matches_path(&self, path: &ResourcePath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource)
    }
}

pub(crate) trait IntoResourceRef {
    fn into_resource_ref(self) -> ResourceRef;
}

impl IntoResourceRef for KvTransaction {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for KvResourceInventoryEntry {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for QueueInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for StreamInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for LeaseInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for ScheduleInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl RpcOperationPath<'_> {
    pub(super) fn matches(&self, realm: &str, area: &str, resource: &str, operation: &str) -> bool {
        self.realm == realm
            && self.area == area
            && self.resource == resource
            && self.operation == operation
    }
}

impl OwnedRpcOperation {
    pub(super) fn matches_resource_path(&self, path: &ResourcePath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource)
    }

    pub(super) fn matches_operation_path(&self, path: &RpcOperationPath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource, &self.operation)
    }
}

pub(crate) fn collect_resource_refs<T: IntoResourceRef>(
    items: impl IntoIterator<Item = T>,
) -> Vec<ResourceRef> {
    items
        .into_iter()
        .map(IntoResourceRef::into_resource_ref)
        .collect()
}

pub(crate) fn collect_distinct_entries<T>(
    values: impl IntoIterator<Item = String>,
    entry: impl Fn(String) -> T,
) -> Vec<T> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(entry)
        .collect()
}

pub(crate) fn matches_family(scope: Option<u64>, route_family: u64) -> bool {
    scope.map(|family| family == route_family).unwrap_or(true)
}

#[derive(Debug, Clone)]
pub(crate) struct QueueRollup {
    subscriptions_active: usize,
    messages_ready: usize,
    messages_delayed: usize,
    messages_inflight: usize,
    messages_dead_lettered: usize,
    messages_total: usize,
    oldest_backlog_age_seconds: u64,
    enqueue_success_total: u64,
    complete_success_total: u64,
    in_rate_per_second: f64,
    out_rate_per_second: f64,
    status: String,
}

impl QueueRollup {
    pub(super) fn new() -> Self {
        Self {
            subscriptions_active: 0,
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 0,
            oldest_backlog_age_seconds: 0,
            enqueue_success_total: 0,
            complete_success_total: 0,
            in_rate_per_second: 0.0,
            out_rate_per_second: 0.0,
            status: "idle".to_string(),
        }
    }

    pub(super) fn record(&mut self, queue: &QueueInfo) {
        self.subscriptions_active += queue.subscriptions_active;
        self.messages_ready += queue.messages_ready;
        self.messages_delayed += queue.messages_delayed;
        self.messages_inflight += queue.messages_inflight;
        self.messages_dead_lettered += queue.messages_dead_lettered;
        self.messages_total += queue.messages_total;
        self.oldest_backlog_age_seconds = self
            .oldest_backlog_age_seconds
            .max(queue.oldest_backlog_age_seconds);
        self.enqueue_success_total += queue.enqueue_success_total;
        self.complete_success_total += queue.complete_success_total;
        self.in_rate_per_second += queue.in_rate_per_second;
        self.out_rate_per_second += queue.out_rate_per_second;
        self.status = worse_queue_status(&self.status, &queue.status);
    }
}

pub(crate) fn queue_rollup<'a>(queues: impl IntoIterator<Item = &'a QueueInfo>) -> QueueRollup {
    let mut rollup = QueueRollup::new();
    for queue in queues {
        rollup.record(queue);
    }
    rollup
}

pub(crate) fn queue_resource_entries(
    queues: &[QueueInfo],
    realm: &str,
    area: &str,
) -> Vec<QueueResourceEntry> {
    let mut grouped: BTreeMap<String, Vec<&QueueInfo>> = BTreeMap::new();
    for queue in queues {
        if queue.realm == realm && queue.area == area {
            grouped
                .entry(queue.resource.clone())
                .or_default()
                .push(queue);
        }
    }

    grouped
        .into_iter()
        .map(|(resource, rows)| {
            let rollup = queue_rollup(rows.iter().copied());
            QueueResourceEntry {
                realm: realm.to_string(),
                area: area.to_string(),
                resource,
                family_count: rows.len(),
                subscriptions_active: rollup.subscriptions_active,
                messages_ready: rollup.messages_ready,
                messages_delayed: rollup.messages_delayed,
                messages_inflight: rollup.messages_inflight,
                messages_dead_lettered: rollup.messages_dead_lettered,
                messages_total: rollup.messages_total,
                oldest_backlog_age_seconds: rollup.oldest_backlog_age_seconds,
                enqueue_success_total: rollup.enqueue_success_total,
                complete_success_total: rollup.complete_success_total,
                in_rate_per_second: rollup.in_rate_per_second,
                out_rate_per_second: rollup.out_rate_per_second,
                status: rollup.status,
            }
        })
        .collect()
}

pub fn collect_queue_realms(queues: &[QueueInfo]) -> QueueRealmCollection {
    let mut grouped: BTreeMap<String, Vec<&QueueInfo>> = BTreeMap::new();
    for queue in queues {
        grouped.entry(queue.realm.clone()).or_default().push(queue);
    }

    QueueRealmCollection {
        realms: grouped
            .into_iter()
            .map(|(realm, rows)| {
                let rollup = queue_rollup(rows.iter().copied());
                let area_count = rows
                    .iter()
                    .map(|queue| queue.area.as_str())
                    .collect::<BTreeSet<_>>()
                    .len();
                let queue_count = rows
                    .iter()
                    .map(|queue| (queue.area.as_str(), queue.resource.as_str()))
                    .collect::<BTreeSet<_>>()
                    .len();
                QueueRealmEntry {
                    realm,
                    area_count,
                    queue_count,
                    subscriptions_active: rollup.subscriptions_active,
                    messages_ready: rollup.messages_ready,
                    messages_delayed: rollup.messages_delayed,
                    messages_inflight: rollup.messages_inflight,
                    messages_dead_lettered: rollup.messages_dead_lettered,
                    messages_total: rollup.messages_total,
                    oldest_backlog_age_seconds: rollup.oldest_backlog_age_seconds,
                    enqueue_success_total: rollup.enqueue_success_total,
                    complete_success_total: rollup.complete_success_total,
                    in_rate_per_second: rollup.in_rate_per_second,
                    out_rate_per_second: rollup.out_rate_per_second,
                    status: rollup.status,
                }
            })
            .collect(),
    }
}

pub fn collect_queue_areas(queues: &[QueueInfo], realm: &str) -> QueueAreaCollection {
    let mut grouped: BTreeMap<String, Vec<&QueueInfo>> = BTreeMap::new();
    for queue in queues {
        if queue.realm == realm {
            grouped.entry(queue.area.clone()).or_default().push(queue);
        }
    }

    QueueAreaCollection {
        realm: realm.to_string(),
        areas: grouped
            .into_iter()
            .map(|(area, rows)| {
                let rollup = queue_rollup(rows.iter().copied());
                let queue_count = rows
                    .iter()
                    .map(|queue| queue.resource.as_str())
                    .collect::<BTreeSet<_>>()
                    .len();
                QueueAreaEntry {
                    realm: realm.to_string(),
                    area,
                    queue_count,
                    subscriptions_active: rollup.subscriptions_active,
                    messages_ready: rollup.messages_ready,
                    messages_delayed: rollup.messages_delayed,
                    messages_inflight: rollup.messages_inflight,
                    messages_dead_lettered: rollup.messages_dead_lettered,
                    messages_total: rollup.messages_total,
                    oldest_backlog_age_seconds: rollup.oldest_backlog_age_seconds,
                    enqueue_success_total: rollup.enqueue_success_total,
                    complete_success_total: rollup.complete_success_total,
                    in_rate_per_second: rollup.in_rate_per_second,
                    out_rate_per_second: rollup.out_rate_per_second,
                    status: rollup.status,
                }
            })
            .collect(),
    }
}

pub fn collect_queue_resources(
    queues: &[QueueInfo],
    realm: &str,
    area: &str,
) -> QueueResourceCollection {
    QueueResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: queue_resource_entries(queues, realm, area),
    }
}

pub fn queue_realm_detail(runtime: &Runtime, realm: &str, family: Option<u64>) -> QueueRealmDetail {
    let queues = runtime
        .queue_list_queues(Some(realm))
        .into_iter()
        .filter(|queue| matches_family(family, queue.family))
        .collect::<Vec<_>>();
    let areas = collect_queue_areas(&queues, realm).areas;
    let queue_entries: Vec<_> = areas
        .iter()
        .flat_map(|area| queue_resource_entries(&queues, realm, &area.area))
        .collect();
    let rollup = queue_rollup(queues.iter());
    QueueRealmDetail {
        realm: realm.to_string(),
        area_count: areas.len(),
        queue_count: queue_entries.len(),
        subscriptions_active: rollup.subscriptions_active,
        messages_ready: rollup.messages_ready,
        messages_delayed: rollup.messages_delayed,
        messages_inflight: rollup.messages_inflight,
        messages_dead_lettered: rollup.messages_dead_lettered,
        messages_total: rollup.messages_total,
        oldest_backlog_age_seconds: rollup.oldest_backlog_age_seconds,
        enqueue_success_total: rollup.enqueue_success_total,
        complete_success_total: rollup.complete_success_total,
        in_rate_per_second: rollup.in_rate_per_second,
        out_rate_per_second: rollup.out_rate_per_second,
        status: rollup.status,
        areas,
        queues: queue_entries,
    }
}

pub fn queue_area_detail(
    runtime: &Runtime,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> QueueAreaDetail {
    let queues = runtime
        .queue_list_queues(Some(realm))
        .into_iter()
        .filter(|queue| matches_family(family, queue.family))
        .collect::<Vec<_>>();
    let queue_entries = queue_resource_entries(&queues, realm, area);
    let rollup = queue_rollup(
        queues
            .iter()
            .filter(|queue| queue.realm == realm && queue.area == area),
    );
    QueueAreaDetail {
        realm: realm.to_string(),
        area: area.to_string(),
        queue_count: queue_entries.len(),
        subscriptions_active: rollup.subscriptions_active,
        messages_ready: rollup.messages_ready,
        messages_delayed: rollup.messages_delayed,
        messages_inflight: rollup.messages_inflight,
        messages_dead_lettered: rollup.messages_dead_lettered,
        messages_total: rollup.messages_total,
        oldest_backlog_age_seconds: rollup.oldest_backlog_age_seconds,
        enqueue_success_total: rollup.enqueue_success_total,
        complete_success_total: rollup.complete_success_total,
        in_rate_per_second: rollup.in_rate_per_second,
        out_rate_per_second: rollup.out_rate_per_second,
        status: rollup.status,
        queues: queue_entries,
    }
}
