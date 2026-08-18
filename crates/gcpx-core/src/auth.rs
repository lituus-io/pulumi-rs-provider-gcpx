// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Credential acquisition and OAuth scope selection.
//!
//! # Why there is no token cache here
//!
//! Application Default Credentials already cache tokens **per scope set**, keyed
//! on the scopes themselves, and refresh them from the token's real expiry. A
//! second cache layered on top of that cannot make anything faster — the inner
//! cache is a lock and a comparison — but it can very easily make things wrong,
//! and previously did: a single-slot cache that ignored its `scopes` argument
//! handed a BigQuery-scoped token to callers asking for `cloud-platform`, and
//! assumed a fixed one-hour lifetime instead of reading the token's expiry.
//!
//! So this module deliberately owns *no* cache. It selects the narrowest scope
//! each API accepts and gets out of the way.
//!
//! # Why credentials are lazy
//!
//! The plugin is launched for every validate and preview, not only for deploys,
//! and those paths may never call a Google API at all — a schema fetch needs no
//! token. Authenticating during startup makes offline validation impossible and
//! puts a network round-trip in front of every invocation. Credentials are
//! therefore resolved on first use and never before.

use std::future::Future;

/// The set of OAuth scopes a request is made under.
///
/// One variant per genuinely distinct scope set, so callers pick a capability
/// rather than pasting URLs, and least privilege is the default rather than a
/// discipline. Adding an API that needs a narrower scope means adding a variant,
/// which makes the widening visible in review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScopeSet {
    /// BigQuery only — narrower than `cloud-platform`, and all the BigQuery
    /// REST surface requires.
    BigQuery,
    /// The APIs that publish no narrower scope: Workflows, Cloud Scheduler,
    /// Dataproc, Vertex AI, and Conversational Analytics.
    CloudPlatform,
}

impl ScopeSet {
    pub const fn as_scopes(self) -> &'static [&'static str] {
        match self {
            Self::BigQuery => &["https://www.googleapis.com/auth/bigquery"],
            Self::CloudPlatform => &["https://www.googleapis.com/auth/cloud-platform"],
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("could not resolve GCP credentials: {0}")]
    Discovery(String),
    #[error("could not obtain an access token: {0}")]
    Token(String),
}

/// A source of OAuth access tokens.
///
/// Generic rather than `dyn` so every call site monomorphises: the production
/// client and the test doubles share no vtable.
pub trait CredentialSource: Send + Sync + 'static {
    fn access_token(
        &self,
        scopes: ScopeSet,
    ) -> impl Future<Output = Result<String, AuthError>> + Send;
}

/// Application Default Credentials, resolved on first use.
///
/// Discovery order is ADC's own: `GOOGLE_APPLICATION_CREDENTIALS`, then gcloud
/// user credentials, then the metadata server.
pub struct AdcCredentials {
    inner: tokio::sync::OnceCell<std::sync::Arc<dyn gcp_auth::TokenProvider>>,
}

impl Default for AdcCredentials {
    fn default() -> Self {
        Self::new()
    }
}

impl AdcCredentials {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::OnceCell::new(),
        }
    }

    /// Resolve credentials, at most once, on the first request that needs them.
    ///
    /// `OnceCell::get_or_try_init` means a failed discovery is not memoised, so
    /// a plugin that outlives a transient credential outage can recover instead
    /// of failing for its whole lifetime.
    async fn provider(&self) -> Result<&std::sync::Arc<dyn gcp_auth::TokenProvider>, AuthError> {
        self.inner
            .get_or_try_init(|| async {
                gcp_auth::provider()
                    .await
                    .map_err(|e| AuthError::Discovery(e.to_string()))
            })
            .await
    }
}

impl CredentialSource for AdcCredentials {
    async fn access_token(&self, scopes: ScopeSet) -> Result<String, AuthError> {
        let provider = self.provider().await?;
        let token = provider
            .token(scopes.as_scopes())
            .await
            .map_err(|e| AuthError::Token(e.to_string()))?;
        Ok(token.as_str().to_owned())
    }
}

/// A fixed token, for tests and for the offline paths in conformance runs.
pub struct StaticCredentials(pub String);

impl CredentialSource for StaticCredentials {
    async fn access_token(&self, _scopes: ScopeSet) -> Result<String, AuthError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigquery_scope_is_narrower_than_cloud_platform() {
        // Least privilege is the point of the enum; if BigQuery ever silently
        // widens to cloud-platform, this catches it.
        assert_eq!(
            ScopeSet::BigQuery.as_scopes(),
            &["https://www.googleapis.com/auth/bigquery"]
        );
        assert_ne!(
            ScopeSet::BigQuery.as_scopes(),
            ScopeSet::CloudPlatform.as_scopes()
        );
    }

    #[test]
    fn scope_sets_are_non_empty() {
        for set in [ScopeSet::BigQuery, ScopeSet::CloudPlatform] {
            assert!(!set.as_scopes().is_empty());
            for scope in set.as_scopes() {
                assert!(scope.starts_with("https://www.googleapis.com/auth/"));
            }
        }
    }

    #[tokio::test]
    async fn static_credentials_ignore_scope() {
        let creds = StaticCredentials("t".into());
        assert_eq!(
            creds.access_token(ScopeSet::BigQuery).await.unwrap(),
            creds.access_token(ScopeSet::CloudPlatform).await.unwrap()
        );
    }

    #[tokio::test]
    async fn adc_does_not_resolve_credentials_until_asked() {
        // The whole point of laziness: constructing the provider must not touch
        // the network, the filesystem, or the metadata server.
        let creds = AdcCredentials::new();
        assert!(creds.inner.get().is_none());
    }
}
