// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Dataproc Serverless ingest and export jobs.
//!
//! Ingest pulls from a JDBC source into GCS or BigQuery; export pushes a
//! BigQuery table back out to a relational database. Both run as scheduled
//! Spark batches, so both are built on the workflow-plus-scheduler pair.

pub mod export;
pub mod ingest;
pub mod parse;
pub mod types;
pub mod workflow_template;
