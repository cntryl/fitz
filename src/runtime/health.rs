/// Runtime-neutral health summary used by broker domain composition.
#[derive(Debug, Clone, Copy)]
pub struct ActorHealthSnapshot {
    pub running: bool,
    pub restart_count: u64,
    pub panic_count: u64,
    pub restart_exhausted: bool,
}
