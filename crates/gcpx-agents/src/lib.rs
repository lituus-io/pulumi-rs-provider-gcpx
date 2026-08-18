// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! GCP data agents.
//!
//! Two APIs, one purpose: the Conversational Analytics API produces an agent
//! that answers questions about data, and Vertex AI's agent engine hosts custom
//! agents alongside a memory store.
//!
//! What makes this worth having in *this* provider rather than a generic API
//! wrapper is that the objects an agent must be grounded on — the tables, the
//! models, the SQL functions — are already declared in the same stack. An agent
//! can therefore be pointed at a dbt model rather than at a table name typed out
//! by hand, and the dependency edge that creates means the agent is never
//! published before the data it describes exists.

pub mod api_body;
pub mod chat;
pub mod grounding;
pub mod ops;
pub mod parse;
pub mod types;

pub use ops::{DataAgentOps, VertexAgentOps};
