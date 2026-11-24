//! Frame-loop busy accounting for one transport session.
//!
//! A session reads and processes inbound frames one at a time, so while a frame
//! is in progress (for example a Stream commit waiting on storage) the session
//! cannot read anything else. Domains that wait on a session's next frame use
//! this busy time to avoid charging the session for time it could not read.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

#[derive(Default)]
pub(crate) struct FrameLoopClock {
    state: Mutex<FrameLoopState>,
}

#[derive(Default)]
struct FrameLoopState {
    completed_busy: Duration,
    busy_since: Option<Instant>,
}

/// Marks the frame loop busy until dropped, including when the frame future is cancelled.
pub(crate) struct FrameLoopBusy<'a> {
    clock: &'a FrameLoopClock,
}

impl FrameLoopClock {
    pub(crate) fn begin_frame(&self, now: Instant) -> FrameLoopBusy<'_> {
        self.state.lock().busy_since = Some(now);
        FrameLoopBusy { clock: self }
    }

    fn end_frame(&self, now: Instant) {
        let mut state = self.state.lock();
        if let Some(started) = state.busy_since.take() {
            state.completed_busy += now.saturating_duration_since(started);
        }
    }

    /// Total busy time up to `now`, including a frame still in progress.
    pub(crate) fn busy_time(&self, now: Instant) -> Duration {
        let state = self.state.lock();
        let in_progress = state.busy_since.map_or(Duration::ZERO, |started| {
            now.saturating_duration_since(started)
        });
        state.completed_busy + in_progress
    }
}

impl Drop for FrameLoopBusy<'_> {
    fn drop(&mut self) {
        self.clock.end_frame(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_count_frame_in_progress_as_busy() {
        // Arrange
        let clock = FrameLoopClock::default();
        let started = Instant::now();

        // Act
        let _busy = clock.begin_frame(started);
        let busy = clock.busy_time(started + Duration::from_secs(7));

        // Assert
        assert_eq!(busy, Duration::from_secs(7));
    }

    #[test]
    fn should_stop_counting_once_frame_finishes() {
        // Arrange
        let clock = FrameLoopClock::default();
        let started = Instant::now();
        // Forget the guard so the test, not the wall clock, decides when the frame ends.
        std::mem::forget(clock.begin_frame(started));
        clock.end_frame(started + Duration::from_secs(2));

        // Act
        let later = clock.busy_time(started + Duration::from_secs(60));

        // Assert
        assert_eq!(later, Duration::from_secs(2));
    }

    #[test]
    fn should_end_frame_when_busy_guard_is_dropped() {
        // Arrange
        let clock = FrameLoopClock::default();
        let busy = clock.begin_frame(Instant::now());

        // Act
        drop(busy);
        let after_drop = clock.busy_time(Instant::now());
        let much_later = clock.busy_time(Instant::now() + Duration::from_secs(60));

        // Assert
        assert_eq!(after_drop, much_later);
    }

    #[test]
    fn should_report_idle_loop_as_not_busy() {
        let clock = FrameLoopClock::default();

        assert_eq!(clock.busy_time(Instant::now()), Duration::ZERO);
    }
}
