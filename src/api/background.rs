//! Tokio-owned background loops for synchronous domain maintenance.

use crate::boot::domain_interfaces::MaintenanceJob;
use crate::boot::domains::BrokerDomains;
use std::sync::Arc;

pub(crate) fn start_domain_background_tasks(domains: &Arc<BrokerDomains>) {
    for job in domains.maintenance_jobs() {
        start_maintenance_job(job);
    }
}

fn start_maintenance_job(mut job: MaintenanceJob) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::debug!(
            job = job.name(),
            "Domain maintenance not started: no Tokio runtime available"
        );
        return;
    };
    handle.spawn(async move {
        let mut delay = !job.starts_immediately();
        loop {
            if !job.is_active() {
                break;
            }
            if delay {
                tokio::time::sleep(job.interval()).await;
            }
            delay = true;
            if !job.is_active() {
                break;
            }
            let name = job.name();
            // A flush or actor request may block. Await each run to preserve
            // serialization while keeping the transport executor responsive.
            match tokio::task::spawn_blocking(move || {
                job.run();
                job
            })
            .await
            {
                Ok(completed) => job = completed,
                Err(error) => {
                    tracing::error!(job = name, %error, "Domain maintenance task failed");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    };
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread")]
    async fn should_keep_transport_executor_available_during_synchronous_maintenance() {
        // Arrange
        let active = Arc::new(AtomicBool::new(true));
        let (started, observed_start) = tokio::sync::oneshot::channel();
        let started = Mutex::new(Some(started));
        let (release, wait_for_release) = std::sync::mpsc::channel();
        let wait_for_release = Mutex::new(wait_for_release);
        let (finished, observed_finish) = tokio::sync::oneshot::channel();
        let finished = Mutex::new(Some(finished));
        let job = MaintenanceJob::new(
            "blocking-test",
            true,
            || Duration::from_millis(1),
            {
                let active = active.clone();
                move || active.load(Ordering::Acquire)
            },
            move || {
                started.lock().unwrap().take().unwrap().send(()).unwrap();
                let released = wait_for_release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .is_ok();
                active.store(false, Ordering::Release);
                let _ = finished.lock().unwrap().take().unwrap().send(released);
            },
        );

        // Act
        start_maintenance_job(job);
        observed_start.await.unwrap();
        let executor_ran_during_maintenance = release.send(()).is_ok();
        let maintenance_was_released = observed_finish.await.unwrap();

        // Assert
        assert!(executor_ran_during_maintenance);
        assert!(maintenance_was_released);
    }
}
