//! Actor supervision trees and fault tolerance

use super::actor::ActorError;
use std::time::Duration;

/// Strategy for handling actor failures
#[derive(Debug, Clone)]
pub enum SupervisorStrategy {
    /// Restart the actor on failure
    Restart { max_restarts: u32, within: Duration },
    /// Stop the actor on failure
    Stop,
    /// Escalate the failure to the parent supervisor
    Escalate,
    /// Resume processing (ignore the error)
    Resume,
}

impl SupervisorStrategy {
    /// Create a restart strategy with max restarts and time window
    #[must_use]
    pub fn restart(max_restarts: u32, within: Duration) -> Self {
        Self::Restart {
            max_restarts,
            within,
        }
    }

    /// Create a stop strategy
    #[must_use]
    pub fn stop() -> Self {
        Self::Stop
    }

    /// Create an escalate strategy
    #[must_use]
    pub fn escalate() -> Self {
        Self::Escalate
    }

    /// Create a resume strategy
    #[must_use]
    pub fn resume() -> Self {
        Self::Resume
    }

    /// Determine what action to take for a given error
    #[must_use]
    pub fn decide(&self, _error: &ActorError) -> SupervisionAction {
        match self {
            Self::Restart { .. } => SupervisionAction::Restart,
            Self::Stop => SupervisionAction::Stop,
            Self::Escalate => SupervisionAction::Escalate,
            Self::Resume => SupervisionAction::Resume,
        }
    }
}

impl Default for SupervisorStrategy {
    fn default() -> Self {
        Self::restart(3, Duration::from_mins(1))
    }
}

/// Action to take when supervising a failed actor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisionAction {
    /// Restart the actor
    Restart,
    /// Stop the actor
    Stop,
    /// Escalate to parent supervisor
    Escalate,
    /// Resume processing
    Resume,
}

/// Tracks restart attempts for supervision
pub struct RestartTracker {
    restarts: Vec<std::time::Instant>,
    max_restarts: u32,
    within: Duration,
}

impl RestartTracker {
    #[must_use]
    pub fn new(max_restarts: u32, within: Duration) -> Self {
        Self {
            restarts: Vec::new(),
            max_restarts,
            within,
        }
    }

    /// Record a restart attempt and return true if within limits
    pub fn record_restart(&mut self) -> bool {
        let now = std::time::Instant::now();

        // Remove old restart records outside the time window
        self.restarts
            .retain(|&t| now.duration_since(t) < self.within);

        // Check if we're within the restart limit
        if self.restarts.len() >= self.max_restarts as usize {
            return false;
        }

        self.restarts.push(now);
        true
    }

    /// Reset the restart tracker
    pub fn reset(&mut self) {
        self.restarts.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_restart_strategy() {
        // Arrange
        let max_restarts = 3;
        let within = Duration::from_mins(1);

        // Act
        let strategy = SupervisorStrategy::restart(max_restarts, within);

        // Assert
        match strategy {
            SupervisorStrategy::Restart {
                max_restarts: m,
                within: w,
            } => {
                assert_eq!(m, max_restarts);
                assert_eq!(w, within);
            }
            _ => panic!("Expected Restart strategy"),
        }
    }

    #[test]
    fn should_decide_restart_action() {
        // Arrange
        let strategy = SupervisorStrategy::restart(3, Duration::from_mins(1));
        let error = ActorError::Panic("test".to_string());

        // Act
        let action = strategy.decide(&error);

        // Assert
        assert_eq!(action, SupervisionAction::Restart);
    }

    #[test]
    fn should_decide_stop_action() {
        // Arrange
        let strategy = SupervisorStrategy::stop();
        let error = ActorError::Panic("test".to_string());

        // Act
        let action = strategy.decide(&error);

        // Assert
        assert_eq!(action, SupervisionAction::Stop);
    }

    #[test]
    fn should_track_restart_within_limit() {
        // Arrange
        let mut tracker = RestartTracker::new(3, Duration::from_mins(1));

        // Act
        let result1 = tracker.record_restart();
        let result2 = tracker.record_restart();
        let result3 = tracker.record_restart();

        // Assert
        assert!(result1);
        assert!(result2);
        assert!(result3);
    }

    #[test]
    fn should_fail_restart_when_exceeding_limit() {
        // Arrange
        let mut tracker = RestartTracker::new(2, Duration::from_mins(1));
        tracker.record_restart();
        tracker.record_restart();

        // Act
        let result = tracker.record_restart();

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_reset_restart_tracker() {
        // Arrange
        let mut tracker = RestartTracker::new(3, Duration::from_mins(1));
        tracker.record_restart();
        tracker.record_restart();

        // Act
        tracker.reset();
        let result = tracker.record_restart();

        // Assert
        assert!(result);
    }

    #[test]
    fn should_expire_old_restart_records() {
        // Arrange
        let mut tracker = RestartTracker::new(2, Duration::from_millis(50));
        tracker.record_restart();
        std::thread::sleep(Duration::from_millis(100));

        // Act
        let result = tracker.record_restart();

        // Assert
        assert!(result);
    }
}
