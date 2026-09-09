//! Opaque full-domain composition for integration tests.

use crate::boot::domains::BrokerDomains;
use crate::boot::Runtime;
use crate::domains::kv::sink::KvDomain;
use crate::domains::lease::sink::LeaseDomain;
use crate::domains::notice::sink::NoticeDomain;
use crate::domains::queue::sink::QueueDomain;
use crate::domains::rpc::sink::RpcDomain;
use crate::domains::schedule::sink::ScheduleDomain;
use crate::domains::stream::sink::{StreamDomain, StreamStorageWriteOptions};
use crate::runtime::Router;
use std::sync::Arc;

/// Full broker fixture without exposing production composition or endpoints.
pub struct DomainRuntimeFixture {
    runtime: Arc<Runtime>,
    store: Arc<cntryl_midge::Engine>,
    schedule: Arc<ScheduleDomain>,
}

impl DomainRuntimeFixture {
    #[must_use]
    pub fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    #[must_use]
    pub fn store(&self) -> Arc<cntryl_midge::Engine> {
        self.store.clone()
    }

    /// Load durable schedule state through the family-owned endpoint.
    ///
    /// # Errors
    /// Returns the schedule preload failure.
    pub fn preload_schedules(&self) -> Result<(), String> {
        self.schedule.preload_persisted_families()
    }
}

/// Compose all seven domains for black-box admin integration tests.
///
/// # Panics
/// Panics if the isolated Stream endpoint cannot be created.
#[must_use]
pub fn create_domain_runtime_fixture() -> DomainRuntimeFixture {
    let router = Arc::new(Router::new());
    let runtime = Arc::new(Runtime::new(router.clone()));
    let admin_read_model = runtime.admin_read_model();
    let store = super::create_test_engine_with_cfs(vec![1]);
    let schedule = Arc::new(ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store.clone()),
        router.clone(),
        admin_read_model.clone(),
    ));
    let domains = Arc::new(BrokerDomains::new(
        Arc::new(KvDomain::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(QueueDomain::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            crate::domains::WritePolicy::Buffered,
            crate::utils::idempotency::default_dedup_store(),
        )),
        Arc::new(NoticeDomain::new(router.clone(), admin_read_model.clone())),
        Arc::new(
            StreamDomain::try_new(
                store.clone(),
                router.clone(),
                admin_read_model.clone(),
                StreamStorageWriteOptions::local(),
            )
            .expect("create Stream integration-test endpoint"),
        ),
        Arc::new(RpcDomain::new(router.clone(), admin_read_model.clone())),
        Arc::new(LeaseDomain::new(router.clone(), admin_read_model.clone())),
        schedule.clone(),
    ));
    runtime.attach_domains(domains);

    DomainRuntimeFixture {
        runtime,
        store,
        schedule,
    }
}
