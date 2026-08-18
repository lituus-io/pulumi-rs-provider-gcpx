// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Per-service circuit breakers.
//!
//! A breaker stops a caller from hammering an API that is already failing.
//! Two properties matter and both were wrong in the predecessor:
//!
//! 1. **Breakers are per service.** One shared breaker means a BigQuery outage
//!    fast-rejects unrelated Cloud Scheduler and agent calls, turning a partial
//!    outage into a total one. Each service gets its own.
//! 2. **Elapsed time is monotonic.** The recovery timeout was measured with
//!    `SystemTime`, so an NTP step could hold a breaker open far longer than
//!    intended, or trip it back to half-open immediately. `Instant` cannot move
//!    backwards.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;

/// The services that get an independent breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    BigQuery,
    Workflows,
    Scheduler,
    Dataproc,
    DataAgents,
    Vertex,
}

impl Service {
    pub const COUNT: usize = 6;

    pub const fn index(self) -> usize {
        match self {
            Self::BigQuery => 0,
            Self::Workflows => 1,
            Self::Scheduler => 2,
            Self::Dataproc => 3,
            Self::DataAgents => 4,
            Self::Vertex => 5,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::BigQuery => "BigQuery",
            Self::Workflows => "Workflows",
            Self::Scheduler => "Cloud Scheduler",
            Self::Dataproc => "Dataproc",
            Self::DataAgents => "Conversational Analytics",
            Self::Vertex => "Vertex AI",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{service} circuit breaker is open after repeated failures; retry in {retry_in_secs}s")]
pub struct CircuitOpen {
    pub service: &'static str,
    pub retry_in_secs: u64,
}

pub struct CircuitBreaker {
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
    state: AtomicU8,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    /// Milliseconds since `base` — monotonic, so clock adjustments cannot
    /// shorten or extend the recovery window.
    last_failure_ms: AtomicU64,
    base: Instant,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, success_threshold: u32, timeout: Duration) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            timeout,
            state: AtomicU8::new(CLOSED),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure_ms: AtomicU64::new(0),
            base: Instant::now(),
        }
    }

    /// Open after 5 consecutive failures, probe after 30s, close after 2 successes.
    pub fn default_config() -> Self {
        Self::new(5, 2, Duration::from_secs(30))
    }

    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }

    /// Whether a request may proceed, and how long to wait if not.
    pub fn allow_request(&self, service: Service) -> Result<(), CircuitOpen> {
        match self.state.load(Ordering::Acquire) {
            CLOSED | HALF_OPEN => Ok(()),
            OPEN => {
                let elapsed_ms = self
                    .now_ms()
                    .saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                let timeout_ms = self.timeout.as_millis() as u64;
                if elapsed_ms >= timeout_ms {
                    // Let a single probe through.
                    self.state.store(HALF_OPEN, Ordering::Release);
                    self.success_count.store(0, Ordering::Relaxed);
                    Ok(())
                } else {
                    Err(CircuitOpen {
                        service: service.name(),
                        retry_in_secs: (timeout_ms - elapsed_ms).div_ceil(1000),
                    })
                }
            }
            _ => Ok(()),
        }
    }

    pub fn record_success(&self) {
        match self.state.load(Ordering::Acquire) {
            HALF_OPEN => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.success_threshold {
                    self.state.store(CLOSED, Ordering::Release);
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                }
            }
            CLOSED => self.failure_count.store(0, Ordering::Relaxed),
            _ => {}
        }
    }

    pub fn record_failure(&self) {
        self.last_failure_ms.store(self.now_ms(), Ordering::Relaxed);
        match self.state.load(Ordering::Acquire) {
            CLOSED => {
                let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.failure_threshold {
                    self.state.store(OPEN, Ordering::Release);
                }
            }
            HALF_OPEN => {
                self.state.store(OPEN, Ordering::Release);
                self.success_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) == OPEN
    }

    pub fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLOSED
    }
}

/// One breaker per service, indexed by discriminant — no map, no hashing, no
/// allocation on the request path.
pub struct BreakerSet([CircuitBreaker; Service::COUNT]);

impl Default for BreakerSet {
    fn default() -> Self {
        Self::new()
    }
}

impl BreakerSet {
    pub fn new() -> Self {
        Self(std::array::from_fn(|_| CircuitBreaker::default_config()))
    }

    pub fn get(&self, service: Service) -> &CircuitBreaker {
        &self.0[service.index()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_closed() {
        let cb = CircuitBreaker::default_config();
        assert!(cb.is_closed());
        assert!(cb.allow_request(Service::BigQuery).is_ok());
    }

    #[test]
    fn opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(3, 1, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_closed());
        cb.record_failure();
        assert!(cb.is_open());
        assert!(cb.allow_request(Service::BigQuery).is_err());
    }

    #[test]
    fn success_resets_consecutive_failures() {
        let cb = CircuitBreaker::new(3, 1, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_closed());
    }

    #[test]
    fn half_open_probe_closes_on_success() {
        let cb = CircuitBreaker::new(2, 1, Duration::from_secs(0));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.is_open());
        assert!(cb.allow_request(Service::BigQuery).is_ok());
        cb.record_success();
        assert!(cb.is_closed());
    }

    #[test]
    fn half_open_probe_failure_reopens() {
        let cb = CircuitBreaker::new(2, 2, Duration::from_secs(0));
        cb.record_failure();
        cb.record_failure();
        assert!(cb.allow_request(Service::BigQuery).is_ok());
        cb.record_failure();
        assert!(cb.is_open());
    }

    #[test]
    fn open_error_names_the_service_and_a_retry_delay() {
        let cb = CircuitBreaker::new(1, 1, Duration::from_secs(30));
        cb.record_failure();
        let err = cb.allow_request(Service::DataAgents).unwrap_err();
        assert_eq!(err.service, "Conversational Analytics");
        assert!(err.retry_in_secs > 0 && err.retry_in_secs <= 30);
        assert!(err.to_string().contains("Conversational Analytics"));
    }

    #[test]
    fn breakers_are_independent_per_service() {
        // The reason this type exists: one failing API must not reject calls to
        // every other API.
        let set = BreakerSet::new();
        for _ in 0..10 {
            set.get(Service::BigQuery).record_failure();
        }
        assert!(set.get(Service::BigQuery).is_open());
        assert!(set.get(Service::Vertex).is_closed());
        assert!(set
            .get(Service::Vertex)
            .allow_request(Service::Vertex)
            .is_ok());
    }

    #[test]
    fn every_service_maps_to_a_distinct_slot() {
        let all = [
            Service::BigQuery,
            Service::Workflows,
            Service::Scheduler,
            Service::Dataproc,
            Service::DataAgents,
            Service::Vertex,
        ];
        let mut seen = [false; Service::COUNT];
        for s in all {
            assert!(!seen[s.index()], "duplicate slot for {s:?}");
            seen[s.index()] = true;
        }
        assert!(seen.iter().all(|s| *s), "a service has no breaker slot");
    }
}
