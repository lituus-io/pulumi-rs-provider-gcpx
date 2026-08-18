// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Cloud Workflows and Cloud Scheduler: the scheduled-execution substrate.
//!
//! Every recurring resource in this provider — scheduled SQL, snapshots,
//! Dataproc ingest and export — is the same shape underneath: a workflow that
//! holds the work, and a scheduler job that decides when it runs. That pair is
//! owned here so the resources built on it do not each reimplement it.

pub mod handlers;
pub mod job_lifecycle;
#[cfg(feature = "mock")]
pub mod mock;
pub mod ops;
pub mod parse;
pub mod scheduler_body;
pub mod types;
pub mod workflow_template;

pub use ops::{SchedulerJobMeta, SchedulerOps, WorkflowMeta};
