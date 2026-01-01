//! Actor execution context and environment

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Timer identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

impl TimerId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Timer registration for scheduled messages
pub struct Timer {
    id: TimerId,
    deadline: Instant,
    interval: Option<Duration>,
}

impl Timer {
    pub fn new(id: TimerId, deadline: Instant, interval: Option<Duration>) -> Self {
        Self {
            id,
            deadline,
            interval,
        }
    }

    pub fn id(&self) -> TimerId {
        self.id
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn interval(&self) -> Option<Duration> {
        self.interval
    }

    /// Check if this timer has fired
    pub fn is_fired(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    /// Reschedule a repeating timer
    pub fn reschedule(&mut self, now: Instant) {
        if let Some(interval) = self.interval {
            self.deadline = now + interval;
        }
    }
}

/// Timer manager for scheduling delayed and recurring messages
pub struct TimerManager {
    next_timer_id: u64,
    timers: HashMap<TimerId, Timer>,
}

impl TimerManager {
    pub fn new() -> Self {
        Self {
            next_timer_id: 1,
            timers: HashMap::new(),
        }
    }

    /// Schedule a one-time timer
    pub fn schedule_once(&mut self, delay: Duration) -> TimerId {
        let timer_id = TimerId::new(self.next_timer_id);
        self.next_timer_id += 1;

        let deadline = Instant::now() + delay;
        let timer = Timer::new(timer_id, deadline, None);
        self.timers.insert(timer_id, timer);

        timer_id
    }

    /// Schedule a repeating timer
    pub fn schedule_repeat(&mut self, delay: Duration, interval: Duration) -> TimerId {
        let timer_id = TimerId::new(self.next_timer_id);
        self.next_timer_id += 1;

        let deadline = Instant::now() + delay;
        let timer = Timer::new(timer_id, deadline, Some(interval));
        self.timers.insert(timer_id, timer);

        timer_id
    }

    /// Cancel a timer
    pub fn cancel(&mut self, timer_id: TimerId) -> bool {
        self.timers.remove(&timer_id).is_some()
    }

    /// Get all fired timers and reschedule repeating ones
    pub fn fired_timers(&mut self) -> Vec<TimerId> {
        let now = Instant::now();
        let mut fired = Vec::new();

        for (id, timer) in self.timers.iter_mut() {
            if timer.is_fired(now) {
                fired.push(*id);

                if timer.interval().is_some() {
                    timer.reschedule(now);
                }
            }
        }

        // Remove one-time timers that have fired
        fired.retain(|id| {
            if let Some(timer) = self.timers.get(id) {
                if timer.interval().is_none() {
                    self.timers.remove(id);
                }
            }
            true
        });

        fired
    }

    /// Get the next timer deadline
    pub fn next_deadline(&self) -> Option<Instant> {
        self.timers.values().map(|t| t.deadline()).min()
    }

    /// Clear all timers
    pub fn clear(&mut self) {
        self.timers.clear();
    }
}

impl Default for TimerManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn should_fire_timer_after_delay() {
        // Arrange
        let mut tm = TimerManager::new();
        let timer_id = tm.schedule_once(Duration::from_millis(50));
        let fired = tm.fired_timers();
        assert!(fired.is_empty());

        // Act
        thread::sleep(Duration::from_millis(100));
        let fired = tm.fired_timers();

        // Assert
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0], timer_id);
        let fired_again = tm.fired_timers();
        assert!(fired_again.is_empty());
    }

    #[test]
    fn should_schedule_repeating_timer() {
        // Arrange
        let mut tm = TimerManager::new();
        let timer_id = tm.schedule_repeat(Duration::from_millis(50), Duration::from_millis(50));

        // Act
        thread::sleep(Duration::from_millis(120));
        let fired = tm.fired_timers();

        // Assert
        assert!(!fired.is_empty());
        assert_eq!(fired[0], timer_id);
        thread::sleep(Duration::from_millis(60));
        let fired_again = tm.fired_timers();
        assert_eq!(fired_again.len(), 1);
        assert_eq!(fired_again[0], timer_id);
    }

    #[test]
    fn should_cancel_timer() {
        // Arrange
        let mut tm = TimerManager::new();
        let timer_id = tm.schedule_once(Duration::from_millis(50));

        // Act
        let cancelled = tm.cancel(timer_id);
        thread::sleep(Duration::from_millis(100));
        let fired = tm.fired_timers();

        // Assert
        assert!(cancelled);
        assert!(fired.is_empty());
    }
}
