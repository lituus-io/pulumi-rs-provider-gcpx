// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! dbt-style SQL models on BigQuery.
//!
//! The pipeline is: preprocess (Jinja-ish constructs the scanner cannot see) →
//! expand macros → scan and resolve refs and sources → generate DDL.
//!
//! The distinguishing choice is that the model DAG is not discovered by walking
//! a project directory. It is declared in the stack, so the Pulumi dependency
//! graph *is* the dbt graph: a model cannot be built before the models it
//! references exist, because the engine already knows that edge.

pub mod handlers;
pub mod macros;
pub mod options;
pub mod parse;
pub mod preprocess;
pub mod resolver;
pub mod scanner;
pub mod types;
pub mod validate;
