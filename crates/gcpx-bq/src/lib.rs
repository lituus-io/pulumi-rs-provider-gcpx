// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! BigQuery resources: declarative schema evolution, tables, datasets, and
//! routines.
//!
//! Handlers are free functions taking `&C` rather than methods on a provider
//! type. That is partly forced — the provider type lives in another crate and
//! Rust forbids inherent impls across crate boundaries — and partly an
//! improvement: each handler can be tested against a client double without
//! constructing a provider at all.

pub mod dataset;
pub mod ops;
pub mod routine;
pub mod schema;
pub mod table;
pub mod types;

pub use ops::BqOps;
