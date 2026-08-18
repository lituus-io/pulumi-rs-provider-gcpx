// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The shared HTTP client every GCP API is reached through.
//!
//! Domain crates do not build requests themselves; they call the verb helpers
//! here with a [`Service`], and get retry, circuit breaking, scope selection,
//! and error classification for free. That keeps those policies in one place
//! instead of once per API surface.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::auth::{CredentialSource, ScopeSet};
use crate::breaker::{BreakerSet, Service};
use crate::error::GcpError;

/// Backoff ladder for retryable failures. Five attempts, ~30s of sleeping in
/// the worst case, which sits inside the per-request timeout below.
const RETRY_DELAYS: [Duration; 4] = [
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];

/// Per-attempt timeout. The retry ladder can sleep ~30s on top of this, so the
/// client is built without a global timeout and each attempt is bounded here
/// instead — a single deadline covering both would expire mid-ladder and turn a
/// recoverable rate limit into a hard failure.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);

pub struct HttpGcpClient<C: CredentialSource> {
    http: reqwest::Client,
    credentials: C,
    breakers: BreakerSet,
    /// Jitter state. Seeded per process and advanced per use, so concurrent
    /// plugin instances retrying the same failed API do not re-collide on the
    /// same schedule — which is what a clock-derived jitter would do.
    jitter: AtomicU64,
}

impl<C: CredentialSource> HttpGcpClient<C> {
    pub fn new(http: reqwest::Client, credentials: C) -> Self {
        Self {
            http,
            credentials,
            breakers: BreakerSet::new(),
            jitter: AtomicU64::new(seed()),
        }
    }

    /// A client tuned for many small requests to a handful of Google hosts.
    pub fn default_http_client() -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder()
            // Redirects are never legitimate for these APIs, and following one
            // would forward the Authorization header to the redirect target.
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(16)
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .build()
    }

    pub fn credentials(&self) -> &C {
        &self.credentials
    }

    async fn token(&self, service: Service) -> Result<String, GcpError> {
        Ok(self.credentials.access_token(scope_for(service)).await?)
    }

    fn next_jitter(&self) -> Duration {
        // xorshift64*, inlined: no dependency, no lock, no syscall.
        let mut x = self.jitter.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.jitter.store(x, Ordering::Relaxed);
        Duration::from_millis(x % 1000)
    }

    /// Whether a failure is worth retrying.
    ///
    /// The 400/403 cases are not a mistake: BigQuery reports job rate limits
    /// with a client-error status and only names the real cause in the body.
    fn is_retryable(status: u16, message: &str) -> bool {
        match status {
            408 | 429 | 500 | 502 | 503 | 504 => true,
            400 | 403 => {
                message.contains("rateLimitExceeded")
                    || message.contains("jobRateLimitExceeded")
                    || message.contains("Job exceeded rate limits")
                    || message.contains("backendError")
            }
            _ => false,
        }
    }

    async fn with_retry<F, Fut, R>(&self, service: Service, op: F) -> Result<R, GcpError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<R, GcpError>>,
    {
        let breaker = self.breakers.get(service);
        breaker.allow_request(service).map_err(|e| GcpError::Api {
            status: 503,
            message: e.to_string(),
        })?;

        let mut last_err = None;
        for (attempt, delay) in std::iter::once(Duration::ZERO)
            .chain(RETRY_DELAYS.iter().copied())
            .enumerate()
        {
            if attempt > 0 {
                // Re-check between attempts: a concurrent caller may have
                // tripped the breaker while this one was sleeping.
                breaker.allow_request(service).map_err(|e| GcpError::Api {
                    status: 503,
                    message: e.to_string(),
                })?;
                tokio::time::sleep(delay + self.next_jitter()).await;
            }
            match op().await {
                Ok(val) => {
                    breaker.record_success();
                    return Ok(val);
                }
                Err(GcpError::Api {
                    status,
                    ref message,
                }) if Self::is_retryable(status, message) => {
                    breaker.record_failure();
                    last_err = Some(GcpError::Api {
                        status,
                        message: crate::error::redact(message),
                    });
                }
                // Client errors are the caller's problem, not the service's:
                // retrying cannot help and tripping the breaker on them would
                // let one malformed request block every other resource.
                Err(e) => return Err(e),
            }
        }
        Err(last_err.expect("loop runs at least once and only exits early on Ok"))
    }

    async fn send_json<R: DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<R, GcpError> {
        let resp = req.timeout(ATTEMPT_TIMEOUT).send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(GcpError::Api {
                status: status.as_u16(),
                message: resp.text().await.unwrap_or_default(),
            });
        }
        Ok(resp.json().await?)
    }

    pub async fn get_json<R: DeserializeOwned>(
        &self,
        service: Service,
        url: &str,
    ) -> Result<R, GcpError> {
        let token = self.token(service).await?;
        self.with_retry(service, || {
            self.send_json(self.http.get(url).bearer_auth(&token))
        })
        .await
    }

    pub async fn post_json<R: DeserializeOwned>(
        &self,
        service: Service,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        self.body_request(service, reqwest::Method::POST, url, body)
            .await
    }

    pub async fn patch_json<R: DeserializeOwned>(
        &self,
        service: Service,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        self.body_request(service, reqwest::Method::PATCH, url, body)
            .await
    }

    pub async fn put_json<R: DeserializeOwned>(
        &self,
        service: Service,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        self.body_request(service, reqwest::Method::PUT, url, body)
            .await
    }

    async fn body_request<R: DeserializeOwned>(
        &self,
        service: Service,
        method: reqwest::Method,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<R, GcpError> {
        // Serialised once and shared across attempts: `Bytes` clones are a
        // refcount bump, so a retry never re-serialises the payload.
        let bytes = bytes::Bytes::from(serde_json::to_vec(body).map_err(|e| GcpError::Api {
            status: 400,
            message: format!("request serialization failed: {e}"),
        })?);
        let token = self.token(service).await?;
        self.with_retry(service, || {
            self.send_json(
                self.http
                    .request(method.clone(), url)
                    .bearer_auth(&token)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(bytes.clone()),
            )
        })
        .await
    }

    /// Delete, treating "already gone" as success.
    ///
    /// Delete is the one verb where a 404 means the caller's goal is already
    /// met, so surfacing it as an error would make teardown non-idempotent.
    pub async fn delete_ok(&self, service: Service, url: &str) -> Result<(), GcpError> {
        let token = self.token(service).await?;
        self.with_retry(service, || async {
            let resp = self
                .http
                .delete(url)
                .bearer_auth(&token)
                .timeout(ATTEMPT_TIMEOUT)
                .send()
                .await?;
            let status = resp.status();
            if !status.is_success() && status.as_u16() != 404 {
                return Err(GcpError::Api {
                    status: status.as_u16(),
                    message: resp.text().await.unwrap_or_default(),
                });
            }
            Ok(())
        })
        .await
    }

    /// POST a request whose response is a stream, returning the byte stream
    /// without buffering it.
    ///
    /// Used for the chat surface, whose responses are unbounded in principle;
    /// collecting them into a `String` first would put the peer in control of
    /// this process's memory. Not retried: a partially-consumed stream cannot
    /// be replayed safely.
    pub async fn post_stream(
        &self,
        service: Service,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<reqwest::Response, GcpError> {
        let breaker = self.breakers.get(service);
        breaker.allow_request(service).map_err(|e| GcpError::Api {
            status: 503,
            message: e.to_string(),
        })?;

        let token = self.token(service).await?;
        let resp = self
            .http
            .post(url)
            .bearer_auth(&token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            breaker.record_failure();
            return Err(GcpError::Api {
                status: status.as_u16(),
                message: resp.text().await.unwrap_or_default(),
            });
        }
        breaker.record_success();
        Ok(resp)
    }
}

/// The narrowest scope each service accepts.
pub const fn scope_for(service: Service) -> ScopeSet {
    match service {
        Service::BigQuery => ScopeSet::BigQuery,
        Service::Workflows
        | Service::Scheduler
        | Service::Dataproc
        | Service::DataAgents
        | Service::Vertex => ScopeSet::CloudPlatform,
    }
}

/// Seed the jitter PRNG from process-unique state.
fn seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_usize(std::process::id() as usize);
    h.finish() | 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::StaticCredentials;

    fn client() -> HttpGcpClient<StaticCredentials> {
        HttpGcpClient::new(
            HttpGcpClient::<StaticCredentials>::default_http_client().unwrap(),
            StaticCredentials("test-token".into()),
        )
    }

    #[test]
    fn server_errors_are_retryable() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(
                HttpGcpClient::<StaticCredentials>::is_retryable(status, ""),
                "{status} should retry"
            );
        }
    }

    #[test]
    fn ordinary_client_errors_are_not_retryable() {
        for status in [400, 401, 403, 404, 409, 412] {
            assert!(
                !HttpGcpClient::<StaticCredentials>::is_retryable(status, "bad request"),
                "{status} should not retry"
            );
        }
    }

    #[test]
    fn bigquery_rate_limits_hidden_in_client_errors_are_retryable() {
        // BigQuery reports job rate limits as 400/403 and only says so in the
        // body; treating those as terminal is the single most common cause of
        // spurious deploy failures.
        for msg in [
            "rateLimitExceeded",
            "jobRateLimitExceeded",
            "Job exceeded rate limits: too many jobs",
        ] {
            assert!(HttpGcpClient::<StaticCredentials>::is_retryable(403, msg));
            assert!(HttpGcpClient::<StaticCredentials>::is_retryable(400, msg));
        }
    }

    #[test]
    fn scope_selection_is_least_privilege() {
        assert_eq!(scope_for(Service::BigQuery), ScopeSet::BigQuery);
        assert_eq!(scope_for(Service::Vertex), ScopeSet::CloudPlatform);
        assert_eq!(scope_for(Service::DataAgents), ScopeSet::CloudPlatform);
    }

    #[test]
    fn jitter_varies_and_stays_under_a_second() {
        let c = client();
        let samples: Vec<_> = (0..16).map(|_| c.next_jitter()).collect();
        assert!(samples.iter().all(|d| *d < Duration::from_secs(1)));
        // A constant jitter is the failure mode worth catching: it would make
        // every retrying client wake at the same instant.
        assert!(
            samples
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1,
            "jitter is not varying"
        );
    }

    #[test]
    fn retry_ladder_fits_inside_the_attempt_timeout() {
        let worst: Duration = RETRY_DELAYS.iter().sum::<Duration>()
            + Duration::from_millis(1000 * RETRY_DELAYS.len() as u64);
        assert!(
            worst < ATTEMPT_TIMEOUT,
            "backoff {worst:?} would outlast a single attempt timeout"
        );
    }

    #[tokio::test]
    async fn breaker_opens_after_repeated_server_errors() {
        let c = client();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let res: Result<(), _> = c
            .with_retry(Service::BigQuery, || {
                calls.fetch_add(1, Ordering::Relaxed);
                async {
                    Err(GcpError::Api {
                        status: 503,
                        message: "unavailable".into(),
                    })
                }
            })
            .await;
        assert!(res.is_err());
        // Five attempts: the initial one plus the four-step ladder.
        assert_eq!(calls.load(Ordering::Relaxed), 5);
        assert!(c.breakers.get(Service::BigQuery).is_open());
        // ...and only for BigQuery.
        assert!(c.breakers.get(Service::Vertex).is_closed());
    }

    #[tokio::test]
    async fn client_errors_do_not_trip_the_breaker() {
        let c = client();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let res: Result<(), _> = c
            .with_retry(Service::BigQuery, || {
                calls.fetch_add(1, Ordering::Relaxed);
                async {
                    Err(GcpError::Api {
                        status: 404,
                        message: "missing".into(),
                    })
                }
            })
            .await;
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1, "404 must not be retried");
        assert!(
            c.breakers.get(Service::BigQuery).is_closed(),
            "a caller's bad request must not block every other resource"
        );
    }

    #[tokio::test]
    async fn retry_error_message_is_redacted() {
        let c = client();
        let res: Result<(), _> = c
            .with_retry(Service::BigQuery, || async {
                Err(GcpError::Api {
                    status: 500,
                    message: "failed for ya29.aaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                })
            })
            .await;
        let msg = res.unwrap_err().to_string();
        assert!(!msg.contains("ya29."), "token leaked through retry: {msg}");
    }
}
