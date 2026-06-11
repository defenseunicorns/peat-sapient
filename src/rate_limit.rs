//! Per-node token-bucket rate limiter for `DetectionReport` emissions.
//!
//! Tokens refill at `max_per_second`; burst up to `burst_size`. Detections that
//! exceed the limit are dropped (stale tracks are worse than dropped ones).
//!
//! Set `max_per_second = 0.0` or `burst_size = 0` to disable limiting.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::time::Instant;

/// Rate-limit parameters stored in `BridgeConfig`.
#[derive(Debug, Clone, Copy)]
pub struct RateLimitConfig {
    /// Maximum sustained detection emissions per second. `0.0` disables limiting.
    pub max_per_second: f64,
    /// Maximum burst depth (tokens). `0` disables limiting.
    pub burst_size: u32,
}

struct TokenBucket {
    tokens: f64,
    capacity: f64,
    refill_rate: f64,
    last_check: Instant,
}

impl TokenBucket {
    fn new(config: &RateLimitConfig) -> Self {
        Self {
            tokens: config.burst_size as f64,
            capacity: config.burst_size as f64,
            refill_rate: config.max_per_second,
            last_check: Instant::now(),
        }
    }

    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_check).as_secs_f64();
        self.last_check = now;
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-node token-bucket limiter. Shared across the bridge routing loop.
///
/// All methods take `&self` — the internal mutex is held only for arithmetic.
pub struct DetectionLimiter {
    buckets: Mutex<HashMap<String, TokenBucket>>,
    config: RateLimitConfig,
    enabled: bool,
}

impl DetectionLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        let enabled = config.max_per_second > 0.0 && config.burst_size > 0;
        Self {
            buckets: Mutex::new(HashMap::new()),
            config,
            enabled,
        }
    }

    /// Returns `true` if the detection should be forwarded, `false` if it should be dropped.
    pub fn check(&self, node_id: &str) -> bool {
        if !self.enabled {
            return true;
        }
        let mut map = self.buckets.lock().unwrap();
        let bucket = map
            .entry(node_id.to_string())
            .or_insert_with(|| TokenBucket::new(&self.config));
        bucket.try_consume()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limiter(per_sec: f64, burst: u32) -> DetectionLimiter {
        DetectionLimiter::new(RateLimitConfig {
            max_per_second: per_sec,
            burst_size: burst,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn burst_allows_exactly_burst_size() {
        let l = limiter(10.0, 3);
        assert!(l.check("node-1"), "1st");
        assert!(l.check("node-1"), "2nd");
        assert!(l.check("node-1"), "3rd");
        assert!(!l.check("node-1"), "4th exceeds burst");
    }

    #[tokio::test(start_paused = true)]
    async fn tokens_refill_after_interval() {
        let l = limiter(10.0, 1); // 1 token per 100ms
        assert!(l.check("node-1"));
        assert!(!l.check("node-1")); // empty

        tokio::time::advance(Duration::from_millis(100)).await;

        assert!(l.check("node-1"), "token should have refilled");
    }

    #[tokio::test(start_paused = true)]
    async fn partial_refill_not_enough_for_token() {
        let l = limiter(10.0, 1); // 1 token per 100ms
        assert!(l.check("node-1"));
        assert!(!l.check("node-1")); // empty

        // Only 50ms → 0.5 tokens accumulated, not enough
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(
            !l.check("node-1"),
            "0.5 tokens is not enough for one detection"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn different_nodes_have_independent_buckets() {
        let l = limiter(10.0, 1);
        assert!(l.check("node-1"));
        assert!(!l.check("node-1")); // node-1 empty
        assert!(l.check("node-2")); // node-2 is independent
    }

    #[test]
    fn zero_rate_disables_limiting() {
        let l = limiter(0.0, 3);
        for _ in 0..20 {
            assert!(l.check("node-1"), "zero rate should disable limiting");
        }
    }

    #[test]
    fn zero_burst_disables_limiting() {
        let l = limiter(10.0, 0);
        for _ in 0..20 {
            assert!(l.check("node-1"), "zero burst should disable limiting");
        }
    }
}
