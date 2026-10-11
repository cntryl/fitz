//! Admission preserves the original transaction and its frozen snapshot.

use super::{KvError, KvStore, KvTransaction};
use std::time::{Duration, Instant};

#[cfg(test)]
type AdmissionHook = Box<dyn FnOnce(u32, Duration) -> cntryl_midge::MidgeResult<bool>>;

#[cfg(test)]
std::thread_local! {
    static ADMISSION_HOOK: std::cell::RefCell<Option<AdmissionHook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) struct AdmissionHookGuard;

#[cfg(test)]
impl Drop for AdmissionHookGuard {
    fn drop(&mut self) {
        ADMISSION_HOOK.with(|hook| hook.borrow_mut().take());
    }
}

#[cfg(test)]
pub(super) fn inject_admission(hook: AdmissionHook) -> AdmissionHookGuard {
    ADMISSION_HOOK.with(|pending| {
        assert!(pending.borrow().is_none());
        *pending.borrow_mut() = Some(hook);
    });
    AdmissionHookGuard
}

impl KvTransaction {
    pub(super) fn wait_for_write_admission(&self) -> Result<(), KvError> {
        let budget = Duration::from_secs(30);
        let deadline = Instant::now() + budget;
        #[cfg(test)]
        let injected = ADMISSION_HOOK.with(|hook| hook.borrow_mut().take());
        #[cfg(test)]
        if let Some(hook) = injected {
            return Self::admission_result(hook(self.family, budget), deadline);
        }
        Self::admission_result(
            self.engine.wait_for_write_stall_clear(self.family, budget),
            deadline,
        )
    }

    fn admission_result(
        result: cntryl_midge::MidgeResult<bool>,
        deadline: Instant,
    ) -> Result<(), KvError> {
        match result {
            Ok(true) if Instant::now() < deadline => Ok(()),
            Ok(_) => Err(KvError::BackendUnavailable(
                "KV storage admission remained stalled for its 30-second budget".to_string(),
            )),
            Err(error) => Err(KvStore::map_error(&error)),
        }
    }
}
