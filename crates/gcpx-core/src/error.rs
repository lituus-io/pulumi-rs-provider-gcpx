// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Error types and their translation into gRPC status codes.
//!
//! Three layers, deliberately distinct:
//!
//! - [`GcpError`] — what the transport saw (HTTP status, auth failure, IO).
//! - [`GcpxError`] — what it *means* for the user, with a suggested fix.
//! - [`tonic::Status`] — what the Pulumi engine receives.
//!
//! The middle layer matters: without it every failure reaches the engine as
//! `internal`, which tells the engine nothing about whether a retry, a config
//! change, or re-authentication is the answer. [`GcpxError::classify`] performs
//! that translation at the client boundary so handlers do not each reinvent it.

use std::fmt;

use tonic::Status;

/// Inspect a transport error's status without matching on a concrete type.
///
/// Implemented for the production client's error and for test doubles, so
/// lifecycle helpers work against both.
pub trait GcpApiError: std::error::Error + Send + Sync + 'static {
    fn is_conflict(&self) -> bool;
    fn is_not_found(&self) -> bool;
    fn is_rate_limited(&self) -> bool;
    fn is_unauthenticated(&self) -> bool;
    /// The HTTP status, when the error came from an API response.
    fn http_status(&self) -> Option<u16>;
    /// The response body, when there was one. Used for classification only —
    /// never surfaced verbatim, since bodies can echo request content.
    fn api_message(&self) -> &str;
}

#[derive(Debug, thiserror::Error)]
pub enum GcpError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("auth error: {0}")]
    Auth(#[from] crate::auth::AuthError),
    #[error("GCP API error ({status}): {message}")]
    Api { status: u16, message: String },
    #[error("operation did not complete within {waited_secs}s (last state: {state})")]
    OperationTimeout { state: String, waited_secs: u64 },
    #[error("operation failed: {message}")]
    OperationFailed { message: String },
}

impl GcpApiError for GcpError {
    fn is_conflict(&self) -> bool {
        matches!(self, GcpError::Api { status: 409, .. })
    }
    fn is_not_found(&self) -> bool {
        matches!(self, GcpError::Api { status: 404, .. })
    }
    fn is_rate_limited(&self) -> bool {
        matches!(self, GcpError::Api { status: 429, .. })
    }
    fn is_unauthenticated(&self) -> bool {
        matches!(self, GcpError::Api { status: 401, .. }) || matches!(self, GcpError::Auth(_))
    }
    fn http_status(&self) -> Option<u16> {
        match self {
            GcpError::Api { status, .. } => Some(*status),
            _ => None,
        }
    }
    fn api_message(&self) -> &str {
        match self {
            GcpError::Api { message, .. } => message,
            _ => "",
        }
    }
}

/// A failure expressed in the user's terms, with an actionable suggestion.
///
/// Every variant maps to a gRPC code that tells the engine what kind of problem
/// it is, so `pulumi up` can distinguish "retry later" from "fix your config".
#[derive(Debug)]
pub enum GcpxError {
    /// Quota or rate limit exceeded on a specific API.
    RateLimited { project: String, api: String },
    /// `ALTER TABLE` (or equivalent) was rejected.
    SchemaEvolutionFailed {
        table: String,
        reason: String,
        suggestion: String,
    },
    /// A dbt model referenced something that was never declared.
    DbtResolutionFailed { model: String, missing_ref: String },
    /// Credentials are absent, expired, or lack the required scope.
    AuthExpired,
    /// The caller's identity lacks an IAM permission.
    PermissionDenied { resource: String, api: String },
    /// The resource does not exist.
    NotFound { resource: String },
    /// A long-running operation did not settle in time.
    OperationTimeout { resource: String, waited_secs: u64 },
    /// Anything the layers above could not classify.
    Upstream { api: String, detail: String },
}

impl fmt::Display for GcpxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RateLimited { project, api } => write!(
                f,
                "Rate limited on {api} for project '{project}'. \
                 Suggestion: reduce concurrent operations, or raise the quota for this API."
            ),
            Self::SchemaEvolutionFailed {
                table,
                reason,
                suggestion,
            } => write!(
                f,
                "Schema evolution failed for table '{table}': {reason}. Suggestion: {suggestion}"
            ),
            Self::DbtResolutionFailed { model, missing_ref } => write!(
                f,
                "dbt model '{model}' references '{missing_ref}' which is not declared. \
                 Suggestion: add '{missing_ref}' to declaredModels on the dbt Project, \
                 and wire it into this model's modelRefs."
            ),
            Self::AuthExpired => write!(
                f,
                "GCP credentials are missing, expired, or lack the required scope. \
                 Suggestion: run `gcloud auth application-default login`, or check that the \
                 service account key is valid and has the needed API scopes."
            ),
            Self::PermissionDenied { resource, api } => write!(
                f,
                "Permission denied on '{resource}' ({api}). \
                 Suggestion: grant the deploying identity the role that includes this \
                 permission, then retry."
            ),
            Self::NotFound { resource } => write!(
                f,
                "'{resource}' was not found. \
                 Suggestion: confirm the project, location, and resource id, and that the \
                 resource was not deleted outside of this stack."
            ),
            Self::OperationTimeout {
                resource,
                waited_secs,
            } => write!(
                f,
                "Operation on '{resource}' did not complete within {waited_secs}s. \
                 Suggestion: the operation may still be running — re-run to pick up its \
                 result, or check the operation in the Cloud Console."
            ),
            Self::Upstream { api, detail } => write!(f, "{api} request failed: {detail}"),
        }
    }
}

impl std::error::Error for GcpxError {}

impl GcpxError {
    /// Translate a transport error into a user-facing one.
    ///
    /// `api` names the service ("BigQuery", "Conversational Analytics") and
    /// `resource` the thing being acted on, so the message is specific without
    /// the caller building strings by hand.
    pub fn classify<E: GcpApiError>(err: &E, api: &str, project: &str, resource: &str) -> Self {
        if err.is_unauthenticated() {
            return Self::AuthExpired;
        }
        if err.is_rate_limited() {
            return Self::RateLimited {
                project: project.to_owned(),
                api: api.to_owned(),
            };
        }
        match err.http_status() {
            Some(403) => Self::PermissionDenied {
                resource: resource.to_owned(),
                api: api.to_owned(),
            },
            Some(404) => Self::NotFound {
                resource: resource.to_owned(),
            },
            _ => Self::Upstream {
                api: api.to_owned(),
                detail: redact(err.api_message()),
            },
        }
    }

    /// The gRPC code that matches this failure's nature.
    pub fn code(&self) -> tonic::Code {
        match self {
            Self::RateLimited { .. } => tonic::Code::ResourceExhausted,
            Self::SchemaEvolutionFailed { .. } => tonic::Code::FailedPrecondition,
            Self::DbtResolutionFailed { .. } => tonic::Code::InvalidArgument,
            Self::AuthExpired => tonic::Code::Unauthenticated,
            Self::PermissionDenied { .. } => tonic::Code::PermissionDenied,
            Self::NotFound { .. } => tonic::Code::NotFound,
            Self::OperationTimeout { .. } => tonic::Code::DeadlineExceeded,
            Self::Upstream { .. } => tonic::Code::Internal,
        }
    }

    #[allow(clippy::result_large_err)]
    pub fn into_status(self) -> Status {
        Status::new(self.code(), self.to_string())
    }
}

impl From<GcpxError> for Status {
    fn from(err: GcpxError) -> Self {
        err.into_status()
    }
}

/// Strip anything that looks like a credential out of an upstream message
/// before it can reach a log, a diagnostic, or resource state.
///
/// API error bodies can echo request headers and payloads; this is the single
/// choke point where that becomes visible to a user.
pub fn redact(message: &str) -> String {
    const MAX: usize = 512;
    const PLACEHOLDER: &str = "[redacted]";

    // PEM material spans many whitespace-separated tokens, so scrubbing token
    // by token would leave most of the key intact. A message carrying key
    // material has nothing worth forwarding, so none of it is.
    if message.contains("-----BEGIN") {
        return "[redacted: message contained private key material]".to_owned();
    }

    let mut out = String::with_capacity(message.len().min(MAX));
    let mut after_bearer = false;

    for token in message.split_whitespace() {
        // `Bearer` is short, but the token *after* it is the credential.
        let secret = after_bearer
            || token.starts_with("ya29.")   // Google OAuth access token
            || token.starts_with("AIza")    // Google API key
            || token.starts_with("gho_")    // OAuth app token
            || token.starts_with("ghp_")    // personal access token
            || (token.starts_with("eyJ") && token.len() > 20); // JWT
        after_bearer = token.eq_ignore_ascii_case("bearer");

        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(if secret { PLACEHOLDER } else { token });

        if out.len() >= MAX {
            out.truncate(MAX);
            out.push('…');
            break;
        }
    }
    out
}

/// Convert a `Result` into a `Status`-carrying one.
#[allow(clippy::result_large_err)]
pub trait IntoStatus<T> {
    /// For failures the user cannot act on.
    fn status_internal(self) -> Result<T, Status>;
    /// For malformed or contradictory inputs.
    fn status_invalid(self) -> Result<T, Status>;
}

/// Like [`IntoStatus`], with context prefixed to the message.
#[allow(clippy::result_large_err)]
pub trait IntoStatusWith<T> {
    fn status_internal_with(self, prefix: &str) -> Result<T, Status>;
}

impl<T, E: fmt::Display> IntoStatus<T> for Result<T, E> {
    fn status_internal(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(redact(&e.to_string())))
    }

    fn status_invalid(self) -> Result<T, Status> {
        self.map_err(|e| Status::invalid_argument(redact(&e.to_string())))
    }
}

impl<T, E: fmt::Display> IntoStatusWith<T> for Result<T, E> {
    fn status_internal_with(self, prefix: &str) -> Result<T, Status> {
        self.map_err(|e| Status::internal(format!("{prefix}: {}", redact(&e.to_string()))))
    }
}

/// Convert a transport failure straight into a classified `Status`.
#[allow(clippy::result_large_err)]
pub trait ClassifyStatus<T> {
    fn classify(self, api: &str, project: &str, resource: &str) -> Result<T, Status>;
}

impl<T, E: GcpApiError> ClassifyStatus<T> for Result<T, E> {
    fn classify(self, api: &str, project: &str, resource: &str) -> Result<T, Status> {
        self.map_err(|e| GcpxError::classify(&e, api, project, resource).into_status())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn api(status: u16, message: &str) -> GcpError {
        GcpError::Api {
            status,
            message: message.to_owned(),
        }
    }

    #[test]
    fn classifies_rate_limit_as_resource_exhausted() {
        let e = GcpxError::classify(&api(429, "quota"), "BigQuery", "proj", "tbl");
        assert_eq!(e.code(), tonic::Code::ResourceExhausted);
        assert!(e.to_string().contains("proj"));
    }

    #[test]
    fn classifies_403_as_permission_denied() {
        let e = GcpxError::classify(&api(403, "forbidden"), "BigQuery", "proj", "ds.tbl");
        assert_eq!(e.code(), tonic::Code::PermissionDenied);
        assert!(e.to_string().contains("ds.tbl"));
    }

    #[test]
    fn classifies_404_as_not_found() {
        let e = GcpxError::classify(&api(404, "nope"), "BigQuery", "proj", "ds.tbl");
        assert_eq!(e.code(), tonic::Code::NotFound);
    }

    #[test]
    fn classifies_401_as_unauthenticated() {
        let e = GcpxError::classify(&api(401, "bad token"), "BigQuery", "proj", "tbl");
        assert_eq!(e.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn unclassified_falls_through_to_internal() {
        let e = GcpxError::classify(&api(500, "boom"), "BigQuery", "proj", "tbl");
        assert_eq!(e.code(), tonic::Code::Internal);
    }

    #[test]
    fn every_variant_carries_a_suggestion() {
        // A classified error without a next step is just a stack trace with
        // better grammar; assert the guidance is actually present.
        let cases = [
            GcpxError::RateLimited {
                project: "p".into(),
                api: "BigQuery".into(),
            },
            GcpxError::SchemaEvolutionFailed {
                table: "t".into(),
                reason: "r".into(),
                suggestion: "do the thing".into(),
            },
            GcpxError::DbtResolutionFailed {
                model: "m".into(),
                missing_ref: "x".into(),
            },
            GcpxError::AuthExpired,
            GcpxError::PermissionDenied {
                resource: "r".into(),
                api: "a".into(),
            },
            GcpxError::NotFound {
                resource: "r".into(),
            },
            GcpxError::OperationTimeout {
                resource: "r".into(),
                waited_secs: 10,
            },
        ];
        for case in cases {
            assert!(
                case.to_string().contains("Suggestion:"),
                "missing suggestion: {case:?}"
            );
        }
    }

    #[test]
    fn redacts_oauth_access_tokens() {
        let msg = "request failed with ya29.a0AfH6SMBxxxxxxxxxxxxxxxxxxxxxxxx and retry";
        let out = redact(msg);
        assert!(!out.contains("ya29."), "token leaked: {out}");
        assert!(out.contains("[redacted]"));
    }

    #[test]
    fn redacts_jwts() {
        assert!(redact("token eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9aaaaaaa").contains("[redacted]"));
    }

    #[test]
    fn drops_the_whole_message_when_it_carries_key_material() {
        // A PEM block spans many whitespace-separated tokens, so scrubbing them
        // one at a time would forward most of the key.
        let pem = "config error: -----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0\nBAQEFAASC\n-----END PRIVATE KEY-----";
        let out = redact(pem);
        assert!(
            !out.contains("MIIEvQIBADANBgkqhkiG9w0"),
            "key body leaked: {out}"
        );
        assert!(!out.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn redacts_the_value_following_bearer() {
        // The credential is the *next* token, and it need not match any prefix.
        let out = redact("Authorization: Bearer 1//0eXaMpLeReFrEsHtOkEn");
        assert!(!out.contains("1//0eXaMpLeReFrEsHtOkEn"), "leaked: {out}");
        assert!(out.contains("[redacted]"));
    }

    #[test]
    fn redacts_api_keys_and_app_tokens() {
        for secret in [
            "AIzaSyA-ExampleKeyValue123456789",
            "gho_ExampleTokenValue1234567890",
            "ghp_ExampleTokenValue1234567890",
        ] {
            let out = redact(&format!("failed with {secret}"));
            assert!(!out.contains(secret), "leaked {secret}: {out}");
        }
    }

    #[test]
    fn keeps_ordinary_diagnostics_readable() {
        // Over-redaction is its own failure: an unreadable error is as useless
        // as a leaked one.
        let msg = "Table proj.ds.tbl not found in location US";
        assert_eq!(redact(msg), msg);
    }

    #[test]
    fn redact_bounds_message_length() {
        let long = "word ".repeat(1000);
        assert!(redact(&long).len() <= 520);
    }

    #[test]
    fn status_helpers_redact_too() {
        let r: Result<(), _> = Err(GcpError::Api {
            status: 500,
            message: "token ya29.aaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        });
        let status = r.status_internal().unwrap_err();
        assert!(!status.message().contains("ya29."));
    }
}
