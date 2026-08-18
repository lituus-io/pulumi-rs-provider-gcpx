// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Shared foundation for the gcpx Pulumi resource provider.
//!
//! This crate holds everything that is not specific to a single GCP API:
//! credential acquisition, the HTTP client with its retry and circuit-breaking
//! policy, long-running-operation polling, identifier escaping, the Pulumi
//! property builders, and the create/delete lifecycle helpers.
//!
//! Domain crates depend on this one and define their own operation traits,
//! implementing them for [`http::HttpGcpClient`]. Rust's orphan rule allows
//! that because the trait is local to the implementing crate — which keeps this
//! crate free of BigQuery, dbt, or agent knowledge while still leaving exactly
//! one concrete client type and no dynamic dispatch anywhere on the path.

pub mod auth;
pub mod breaker;
pub mod diff_macro;
pub mod error;
pub mod handler_util;
pub mod http;
pub mod json_body;
pub mod lifecycle;
pub mod lro;
pub mod output;
pub mod prost_util;
pub mod resource;
pub mod sanitize;

/// Maximum gRPC message size, matching the Pulumi engine and the language
/// runtime this provider is deployed alongside.
///
/// tonic defaults to a 4 MiB receive cap. Provider schemas and large resource
/// registrations exceed it, and the failure is opaque — `OutOfRange: decoded
/// message length too large` — and lands during *validation*, not just deploy.
/// The client side of that conversation already raises this limit; the server
/// side has to agree or the raise accomplishes nothing.
pub const MAX_GRPC_MESSAGE_BYTES: usize = 512 * 1024 * 1024;
