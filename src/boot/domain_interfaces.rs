//! Narrow operational views over boot-owned domain handles.

#[derive(Clone)]
pub(crate) struct MaintenanceJob {
    name: &'static str,
    start_immediately: bool,
    interval: std::sync::Arc<dyn Fn() -> std::time::Duration + Send + Sync>,
    active: std::sync::Arc<dyn Fn() -> bool + Send + Sync>,
    run: std::sync::Arc<dyn Fn() + Send + Sync>,
}

impl MaintenanceJob {
    pub(crate) fn new(
        name: &'static str,
        start_immediately: bool,
        interval: impl Fn() -> std::time::Duration + Send + Sync + 'static,
        active: impl Fn() -> bool + Send + Sync + 'static,
        run: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            name,
            start_immediately,
            interval: std::sync::Arc::new(interval),
            active: std::sync::Arc::new(active),
            run: std::sync::Arc::new(run),
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn interval(&self) -> std::time::Duration {
        (self.interval)()
    }

    pub(crate) fn starts_immediately(&self) -> bool {
        self.start_immediately
    }

    pub(crate) fn is_active(&self) -> bool {
        (self.active)()
    }

    pub(crate) fn run(&self) {
        (self.run)();
    }
}
