//! Token accounting independent of HTTP, tested with a deterministic clock.
//! Each account/exchange must share one read and one write bucket.
use std::time::{Duration, Instant};

pub const TARGET_UTILIZATION: f64 = 0.97;

pub struct Bucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    updated: Instant,
    blocked_until: Instant,
    last_throttle: Option<Instant>,
    utilization: f64,
    strikes: u32,
    reserve: f64,
}

impl Bucket {
    pub fn new(rate: f64, capacity: f64, reserve: f64, now: Instant) -> Self {
        assert!(rate.is_finite() && rate > 0.0);
        assert!(capacity.is_finite() && capacity > 0.0);
        Self { rate, capacity, tokens: 0.0, updated: now, blocked_until: now,
            last_throttle: None, utilization: TARGET_UTILIZATION, strikes: 0,
            reserve: reserve.max(0.0).min(capacity / 2.0) }
    }

    pub fn configure(&mut self, rate: f64, capacity: f64) -> Result<(), &'static str> {
        if !rate.is_finite() || rate <= 0.0 || !capacity.is_finite() || capacity <= 0.0 {
            return Err("invalid server token budget");
        }
        self.rate = rate;
        self.capacity = capacity;
        self.tokens = self.tokens.min(capacity);
        self.reserve = self.reserve.min(capacity / 2.0);
        Ok(())
    }

    /// None means the tokens were consumed; Some is the earliest retry delay.
    /// An oversized request fails instead of waiting forever.
    pub fn acquire(&mut self, cost: f64, urgent: bool, now: Instant) -> Result<Option<Duration>, &'static str> {
        if !cost.is_finite() || cost <= 0.0 || cost > self.capacity {
            return Err("request cost exceeds token bucket capacity or is invalid");
        }
        if now < self.blocked_until { return Ok(Some(self.blocked_until - now)); }
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate * self.utilization).min(self.capacity);
        self.updated = now;
        // Recover slowly after 30 quiet seconds, never exceed the target.
        if self.last_throttle.map_or(false, |t| now.saturating_duration_since(t).as_secs() >= 30) {
            self.utilization = (self.utilization + elapsed * 0.005).min(TARGET_UTILIZATION);
            if self.utilization >= TARGET_UTILIZATION { self.strikes = 0; }
        }
        let reserve = if urgent { 0.0 } else { self.reserve.min(self.capacity - cost) };
        let needed = cost + reserve;
        if self.tokens + 1e-9 >= needed {
            self.tokens = (self.tokens - cost).max(0.0);
            Ok(None)
        } else {
            Ok(Some(Duration::from_secs_f64((needed - self.tokens) / (self.rate * self.utilization))))
        }
    }

    pub fn throttled(&mut self, now: Instant) {
        self.strikes = (self.strikes + 1).min(6);
        self.utilization = (self.utilization * 0.8).max(0.1);
        self.tokens = 0.0;
        let delay = Duration::from_millis(250 * (1u64 << (self.strikes - 1)));
        self.blocked_until = now + delay;
        // Don't accumulate a burst during the backoff.
        self.updated = self.blocked_until;
        self.last_throttle = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sustained_capacity_stays_near_target() {
        let start = Instant::now();
        let mut bucket = Bucket::new(200.0, 400.0, 0.0, start);
        let mut admitted = 0;
        for millis in 0..10_000 {
            let now = start + Duration::from_millis(millis);
            while bucket.acquire(10.0, false, now).unwrap().is_none() { admitted += 1; }
        }
        assert!((193..=194).contains(&admitted), "{admitted}");
    }
    #[test]
    fn idle_burst_is_capped() {
        let now = Instant::now();
        let mut bucket = Bucket::new(100.0, 100.0, 0.0, now);
        let later = now + Duration::from_secs(100);
        for _ in 0..10 { assert_eq!(bucket.acquire(10.0, true, later).unwrap(), None); }
        assert!(bucket.acquire(10.0, true, later).unwrap().is_some());
    }
    #[test]
    fn cancellations_can_use_entry_reserve() {
        let now = Instant::now();
        let mut bucket = Bucket::new(100.0, 100.0, 10.0, now);
        let later = now + Duration::from_secs(2);
        for _ in 0..9 { assert_eq!(bucket.acquire(10.0, false, later).unwrap(), None); }
        assert!(bucket.acquire(10.0, false, later).unwrap().is_some());
        assert_eq!(bucket.acquire(10.0, true, later).unwrap(), None);
    }
    #[test]
    fn throttle_waits_and_reduces_rate() {
        let now = Instant::now();
        let mut bucket = Bucket::new(100.0, 100.0, 0.0, now);
        bucket.throttled(now);
        assert_eq!(bucket.acquire(10.0, true, now).unwrap(), Some(Duration::from_millis(250)));
        assert!(bucket.acquire(10.0, true, now + Duration::from_millis(250)).unwrap().is_some());
        assert!(bucket.utilization < TARGET_UTILIZATION);
    }
    #[test]
    fn invalid_cost_and_server_limits_fail() {
        let now = Instant::now();
        let mut bucket = Bucket::new(100.0, 100.0, 0.0, now);
        assert!(bucket.acquire(101.0, true, now).is_err());
        assert!(bucket.acquire(f64::NAN, true, now).is_err());
        assert!(bucket.configure(0.0, 100.0).is_err());
    }
    #[test]
    fn exchanges_and_operations_have_independent_budgets() {
        let now = Instant::now();
        let mut predictions = Bucket::new(100.0, 100.0, 0.0, now);
        let mut perps = Bucket::new(100.0, 100.0, 0.0, now);
        predictions.throttled(now);
        assert_eq!(perps.acquire(10.0, true, now + Duration::from_secs(1)).unwrap(), None);
    }
}
