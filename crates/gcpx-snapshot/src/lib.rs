// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! SCD Type 2 snapshots.
//!
//! A snapshot turns a query that shows *current* state into a table that keeps
//! *every* state, stamped with the window it was true for. The generated SQL
//! runs on a schedule: invalidate rows whose source changed, insert the new
//! versions, and optionally close out rows that vanished from the source.

pub mod ddl;
pub mod handlers;
pub mod parse;
pub mod types;
pub mod workflow;
