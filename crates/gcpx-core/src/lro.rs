// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Long-running operation polling.
//!
//! Several GCP APIs answer a mutation with an `Operation` that settles later.
//! Pulumi's CRUD calls are synchronous, so the provider must wait — but waiting
//! badly is its own failure mode: a fixed-interval poll either wastes hundreds
//! of requests on a slow operation or reports a fast one late.
//!
//! This module polls on a capped exponential schedule and gives up at a
//! deadline rather than never, so a stuck operation surfaces as a timeout the
//! user can act on instead of a hung deploy.

use std::future::Future;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::error::GcpError;

/// The shape every `google.longrunning.Operation` shares.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Operation {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub done: bool,
    #[serde(default)]
    pub error: Option<OperationError>,
    #[serde(default)]
    pub response: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OperationError {
    #[serde(default)]
    pub code: i32,
    #[serde(default)]
    pub message: String,
}

/// How long to wait, and how often to ask.
#[derive(Debug, Clone, Copy)]
pub struct PollConfig {
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub deadline: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            initial_interval: Duration::from_millis(500),
            max_interval: Duration::from_secs(10),
            deadline: Duration::from_secs(600),
        }
    }
}

impl PollConfig {
    /// For operations that usually settle in seconds.
    pub fn quick() -> Self {
        Self {
            initial_interval: Duration::from_millis(250),
            max_interval: Duration::from_secs(2),
            deadline: Duration::from_secs(120),
        }
    }

    /// For operations that provision infrastructure and can take many minutes.
    pub fn slow() -> Self {
        Self {
            initial_interval: Duration::from_secs(2),
            max_interval: Duration::from_secs(30),
            deadline: Duration::from_secs(1800),
        }
    }
}

/// Poll `fetch` until the operation completes, fails, or the deadline passes.
///
/// `fetch` is called with no arguments so the caller keeps ownership of the URL
/// and client; it is re-invoked per poll rather than holding a future open.
pub async fn poll_operation<F, Fut>(
    initial: Operation,
    fetch: F,
    config: PollConfig,
) -> Result<Operation, GcpError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Operation, GcpError>>,
{
    if initial.done {
        return finish(initial);
    }

    let started = Instant::now();
    let mut interval = config.initial_interval;

    loop {
        if started.elapsed() >= config.deadline {
            return Err(GcpError::OperationTimeout {
                state: if initial.name.is_empty() {
                    "pending".to_owned()
                } else {
                    initial.name.clone()
                },
                waited_secs: started.elapsed().as_secs(),
            });
        }

        tokio::time::sleep(interval).await;
        // Back off up to the cap: a slow operation should not be asked about
        // 1,200 times, and a quick one should not wait 30s to be noticed.
        interval = (interval * 2).min(config.max_interval);

        let op = fetch().await?;
        if op.done {
            return finish(op);
        }
    }
}

fn finish(op: Operation) -> Result<Operation, GcpError> {
    match op.error {
        Some(err) => Err(GcpError::OperationFailed {
            message: crate::error::redact(&format!("[{}] {}", err.code, err.message)),
        }),
        None => Ok(op),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn done(name: &str) -> Operation {
        Operation {
            name: name.into(),
            done: true,
            ..Default::default()
        }
    }

    fn pending(name: &str) -> Operation {
        Operation {
            name: name.into(),
            done: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn already_complete_operation_never_polls() {
        let calls = AtomicU32::new(0);
        let op = poll_operation(
            done("op/1"),
            || {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Ok(done("op/1")) }
            },
            PollConfig::quick(),
        )
        .await
        .unwrap();
        assert_eq!(op.name, "op/1");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "an operation returned done must not cost a round-trip"
        );
    }

    #[tokio::test]
    async fn polls_until_done() {
        let calls = AtomicU32::new(0);
        let op = poll_operation(
            pending("op/2"),
            || {
                let n = calls.fetch_add(1, Ordering::Relaxed) + 1;
                async move {
                    Ok(if n >= 3 {
                        done("op/2")
                    } else {
                        pending("op/2")
                    })
                }
            },
            PollConfig {
                initial_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(2),
                deadline: Duration::from_secs(5),
            },
        )
        .await
        .unwrap();
        assert!(op.done);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn operation_error_becomes_a_failure() {
        let op = Operation {
            name: "op/3".into(),
            done: true,
            error: Some(OperationError {
                code: 7,
                message: "permission denied".into(),
            }),
            ..Default::default()
        };
        let err = poll_operation(op, || async { unreachable!() }, PollConfig::quick())
            .await
            .unwrap_err();
        assert!(matches!(err, GcpError::OperationFailed { .. }));
        assert!(err.to_string().contains("permission denied"));
    }

    #[tokio::test]
    async fn operation_error_is_redacted() {
        let op = Operation {
            done: true,
            error: Some(OperationError {
                code: 3,
                message: "bad token ya29.aaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            }),
            ..Default::default()
        };
        let err = poll_operation(op, || async { unreachable!() }, PollConfig::quick())
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("ya29."));
    }

    #[tokio::test]
    async fn gives_up_at_the_deadline() {
        let err = poll_operation(
            pending("op/4"),
            || async { Ok(pending("op/4")) },
            PollConfig {
                initial_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(1),
                deadline: Duration::from_millis(20),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, GcpError::OperationTimeout { .. }));
        // The message has to say the work may still be running, or a user will
        // assume the deploy failed and retry into a duplicate.
        assert!(err.to_string().contains("did not complete"));
    }

    #[tokio::test]
    async fn fetch_failure_propagates_immediately() {
        let err = poll_operation(
            pending("op/5"),
            || async {
                Err(GcpError::Api {
                    status: 500,
                    message: "boom".into(),
                })
            },
            PollConfig {
                initial_interval: Duration::from_millis(1),
                max_interval: Duration::from_millis(1),
                deadline: Duration::from_secs(5),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, GcpError::Api { status: 500, .. }));
    }

    #[test]
    fn backoff_caps_rather_than_growing_without_bound() {
        let cfg = PollConfig::slow();
        let mut interval = cfg.initial_interval;
        for _ in 0..20 {
            interval = (interval * 2).min(cfg.max_interval);
        }
        assert_eq!(interval, cfg.max_interval);
    }
}
