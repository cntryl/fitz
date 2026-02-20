//! Actor execution context and environment

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Timer identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

impl TimerId {
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    #[inline]
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

    #[inline]
    pub fn id(&self) -> TimerId {
        self.id
    }

    #[inline]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    #[inline]
    pub fn interval(&self) -> Option<Duration> {
        self.interval
    }

    /// Check if this timer has fired
    #[inline]
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
const WHEEL_LEVELS: usize = 4;
const WHEEL_SIZE: usize = 256;
const LEVEL_SHIFT: u32 = 8;
const TICK_MS: u64 = 1;

struct TimingWheel {
    slots: Vec<Vec<Vec<TimerId>>>,
}

impl TimingWheel {
    fn new() -> Self {
        let mut slots = Vec::with_capacity(WHEEL_LEVELS);
        for _ in 0..WHEEL_LEVELS {
            let mut level = Vec::with_capacity(WHEEL_SIZE);
            for _ in 0..WHEEL_SIZE {
                level.push(Vec::new());
            }
            slots.push(level);
        }

        Self { slots }
    }

    fn insert(&mut self, level: usize, slot: usize, id: TimerId) {
        self.slots[level][slot].push(id);
    }

    fn take_slot(&mut self, level: usize, slot: usize) -> Vec<TimerId> {
        std::mem::take(&mut self.slots[level][slot])
    }

    fn remove(&mut self, level: usize, slot: usize, id: TimerId) -> bool {
        let slot_vec = &mut self.slots[level][slot];
        if let Some(pos) = slot_vec.iter().position(|existing| *existing == id) {
            slot_vec.swap_remove(pos);
            return true;
        }
        false
    }

    fn slot_is_empty(&self, level: usize, slot: usize) -> bool {
        self.slots[level][slot].is_empty()
    }
}

struct TimerEntry {
    timer: Timer,
    deadline_tick: u64,
    level: usize,
    slot: usize,
}

pub struct TimerManager {
    next_timer_id: u64,
    timers: HashMap<TimerId, TimerEntry>,
    wheel: TimingWheel,
    start_instant: Instant,
    current_tick: u64,
}

impl TimerManager {
    pub fn new() -> Self {
        let start_instant = Instant::now();
        Self {
            next_timer_id: 1,
            timers: HashMap::new(),
            wheel: TimingWheel::new(),
            start_instant,
            current_tick: 0,
        }
    }

    fn ticks_from_instant(&self, instant: Instant) -> u64 {
        Self::ticks_from_instant_with_start(self.start_instant, instant)
    }

    fn ticks_from_instant_with_start(start_instant: Instant, instant: Instant) -> u64 {
        if instant <= start_instant {
            return 0;
        }
        let delta_ms = instant.duration_since(start_instant).as_millis() as u64;
        delta_ms / TICK_MS
    }

    fn level_for_delta(delta: u64) -> usize {
        if delta < (1_u64 << LEVEL_SHIFT) {
            0
        } else if delta < (1_u64 << (LEVEL_SHIFT * 2)) {
            1
        } else if delta < (1_u64 << (LEVEL_SHIFT * 3)) {
            2
        } else {
            3
        }
    }

    fn slot_for_tick(deadline_tick: u64, level: usize) -> usize {
        ((deadline_tick >> (LEVEL_SHIFT * level as u32)) & (WHEEL_SIZE as u64 - 1)) as usize
    }

    fn insert_entry(&mut self, id: TimerId, timer: Timer) {
        let mut deadline_tick = self.ticks_from_instant(timer.deadline());
        if deadline_tick < self.current_tick {
            deadline_tick = self.current_tick;
        }

        let delta = deadline_tick.saturating_sub(self.current_tick);
        let level = Self::level_for_delta(delta);
        let slot = Self::slot_for_tick(deadline_tick, level);

        self.wheel.insert(level, slot, id);
        self.timers.insert(
            id,
            TimerEntry {
                timer,
                deadline_tick,
                level,
                slot,
            },
        );
    }

    fn move_entry(&mut self, id: TimerId, deadline_tick: u64) {
        if let Some(entry) = self.timers.get_mut(&id) {
            entry.deadline_tick = deadline_tick;
            let delta = deadline_tick.saturating_sub(self.current_tick);
            let level = Self::level_for_delta(delta);
            let slot = Self::slot_for_tick(deadline_tick, level);
            entry.level = level;
            entry.slot = slot;
            self.wheel.insert(level, slot, id);
        }
    }

    fn cascade_level(&mut self, level: usize) {
        let slot = ((self.current_tick >> (LEVEL_SHIFT * level as u32)) & (WHEEL_SIZE as u64 - 1))
            as usize;
        let ids = self.wheel.take_slot(level, slot);
        for id in ids {
            if let Some(entry) = self.timers.get(&id) {
                let deadline_tick = entry.deadline_tick;
                self.move_entry(id, deadline_tick);
            }
        }
    }

    fn recompute_next_deadline_tick(&self) -> Option<u64> {
        let mut best: Option<u64> = None;

        for level in 0..WHEEL_LEVELS {
            let shift = LEVEL_SHIFT * level as u32;
            let current_slot = (self.current_tick >> shift) & (WHEEL_SIZE as u64 - 1);

            for offset in 0..WHEEL_SIZE {
                let slot = ((current_slot + offset as u64) & (WHEEL_SIZE as u64 - 1)) as usize;
                if self.wheel.slot_is_empty(level, slot) {
                    continue;
                }

                let base = (self.current_tick >> shift).saturating_add(offset as u64);
                let mut candidate = base << shift;
                if candidate < self.current_tick {
                    candidate = self.current_tick;
                }

                best = match best {
                    Some(existing) => Some(existing.min(candidate)),
                    None => Some(candidate),
                };
                break;
            }
        }

        best
    }

    /// Schedule a one-time timer
    #[inline]
    pub fn schedule_once(&mut self, delay: Duration) -> TimerId {
        let timer_id = TimerId::new(self.next_timer_id);
        self.next_timer_id += 1;

        let deadline = Instant::now() + delay;
        let timer = Timer::new(timer_id, deadline, None);
        self.insert_entry(timer_id, timer);

        timer_id
    }

    /// Schedule a repeating timer
    #[inline]
    pub fn schedule_repeat(&mut self, delay: Duration, interval: Duration) -> TimerId {
        let timer_id = TimerId::new(self.next_timer_id);
        self.next_timer_id += 1;

        let deadline = Instant::now() + delay;
        let timer = Timer::new(timer_id, deadline, Some(interval));
        self.insert_entry(timer_id, timer);

        timer_id
    }

    /// Cancel a timer
    #[inline]
    pub fn cancel(&mut self, timer_id: TimerId) -> bool {
        if let Some(entry) = self.timers.remove(&timer_id) {
            self.wheel.remove(entry.level, entry.slot, timer_id);
            return true;
        }
        false
    }

    /// Get all fired timers and reschedule repeating ones
    #[inline]
    pub fn fired_timers(&mut self) -> Vec<TimerId> {
        let now = Instant::now();
        let start_instant = self.start_instant;
        let now_tick = self.ticks_from_instant(now);
        let mut fired = Vec::new();

        while self.current_tick <= now_tick {
            let slot = (self.current_tick & (WHEEL_SIZE as u64 - 1)) as usize;
            let ids = self.wheel.take_slot(0, slot);

            for id in ids {
                if let Some(mut entry) = self.timers.remove(&id) {
                    if entry.deadline_tick <= now_tick {
                        fired.push(id);
                        if entry.timer.interval().is_some() {
                            entry.timer.reschedule(now);
                            let mut deadline_tick = Self::ticks_from_instant_with_start(
                                start_instant,
                                entry.timer.deadline(),
                            );
                            if deadline_tick < self.current_tick {
                                deadline_tick = self.current_tick;
                            }
                            entry.deadline_tick = deadline_tick;
                            self.insert_entry(id, entry.timer);
                        }
                    } else {
                        self.insert_entry(id, entry.timer);
                    }
                }
            }

            if slot == 0 {
                self.cascade_level(1);
                let level1_slot =
                    ((self.current_tick >> LEVEL_SHIFT) & (WHEEL_SIZE as u64 - 1)) as usize;
                if level1_slot == 0 {
                    self.cascade_level(2);
                    let level2_slot = ((self.current_tick >> (LEVEL_SHIFT * 2))
                        & (WHEEL_SIZE as u64 - 1)) as usize;
                    if level2_slot == 0 {
                        self.cascade_level(3);
                    }
                }
            }

            self.current_tick = self.current_tick.saturating_add(1);
        }

        fired
    }

    /// Get the next timer deadline
    #[inline]
    pub fn next_deadline(&self) -> Option<Instant> {
        let next_tick = self.recompute_next_deadline_tick()?;
        let deadline = self
            .start_instant
            .checked_add(Duration::from_millis(next_tick.saturating_mul(TICK_MS)))?;
        Some(deadline)
    }

    /// Clear all timers
    pub fn clear(&mut self) {
        self.timers.clear();
        self.wheel = TimingWheel::new();
        self.current_tick = 0;
        self.start_instant = Instant::now();
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
    use std::time::Instant;

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

    #[test]
    fn should_fire_next_timer_sub_millisecond_given_10k_staggered() {
        // Arrange
        let mut tm = TimerManager::new();
        for index in 1..=10_000_u64 {
            tm.schedule_once(Duration::from_millis(index));
        }

        thread::sleep(Duration::from_millis(2));

        // Act
        let start = Instant::now();
        let fired = tm.fired_timers();
        let elapsed = start.elapsed();

        // Assert
        assert!(!fired.is_empty());
        assert!(elapsed < Duration::from_millis(1));
    }
}
