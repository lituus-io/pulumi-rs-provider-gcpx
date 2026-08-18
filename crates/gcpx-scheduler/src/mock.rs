// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! A recording double for scheduled resources.
//!
//! Scheduled resources need both BigQuery and the scheduling APIs — a SQL job
//! validates its query against BigQuery before it ever creates a workflow — so
//! the double has to satisfy `BqOps + SchedulerOps` at once. It embeds
//! [`MockBqClient`] and forwards, rather than restating the BigQuery surface, so
//! there is only ever one definition of that behaviour to keep in step.
//!
//! Rust's orphan rule permits implementing `BqOps` here because the *type* is
//! local to this crate, even though the trait is not.

use std::future::Future;
use std::sync::Mutex;

use gcpx_bq::mock::{MockBqClient, MockError};
use gcpx_bq::types::{BqField, BqTableMeta, DatasetMeta, DryRunResult, RoutineMeta};
use gcpx_bq::BqOps;

use crate::ops::{SchedulerJobMeta, SchedulerOps, WorkflowMeta};

#[derive(Default)]
pub struct MockSchedulerClient {
    pub bq: MockBqClient,
    /// (operation, name, definition) for every workflow call.
    pub workflow_log: Mutex<Vec<(String, String, String)>>,
    /// (operation, name) for every scheduler-job call.
    pub scheduler_log: Mutex<Vec<(String, String)>>,
    pub fail_on: Mutex<Option<String>>,
}

impl MockSchedulerClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_bq(bq: MockBqClient) -> Self {
        Self {
            bq,
            ..Default::default()
        }
    }

    pub fn workflow_log(&self) -> Vec<(String, String, String)> {
        self.workflow_log.lock().unwrap().clone()
    }

    pub fn scheduler_log(&self) -> Vec<(String, String)> {
        self.scheduler_log.lock().unwrap().clone()
    }

    fn guard(&self, method: &'static str) -> Result<(), MockError> {
        let failing = self
            .fail_on
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|m| m == method);
        if failing {
            Err(MockError(format!("{method} failed")))
        } else {
            Ok(())
        }
    }

    fn workflow(name: &str) -> WorkflowMeta {
        WorkflowMeta {
            name: name.to_owned(),
            state: "ACTIVE".to_owned(),
            revision_id: "1".to_owned(),
            create_time: String::new(),
            update_time: String::new(),
            service_account: String::new(),
        }
    }

    fn job(name: &str) -> SchedulerJobMeta {
        SchedulerJobMeta {
            name: name.to_owned(),
            state: "ENABLED".to_owned(),
            schedule: "0 * * * *".to_owned(),
            time_zone: "UTC".to_owned(),
            schedule_time: String::new(),
            last_attempt_time: String::new(),
            user_update_time: String::new(),
        }
    }
}

#[allow(
    clippy::manual_async_fn,
    reason = "calls are recorded eagerly, before the future is returned"
)]
impl SchedulerOps for MockSchedulerClient {
    type Error = MockError;

    fn create_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        definition: &'a str,
        _sa: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "create".to_owned(),
            name.to_owned(),
            definition.to_owned(),
        ));
        async move {
            self.guard("create_workflow")?;
            Ok(Self::workflow(name))
        }
    }

    fn get_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_workflow")?;
            Ok(Self::workflow(name))
        }
    }

    fn update_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "update".to_owned(),
            name.to_owned(),
            definition.to_owned(),
        ));
        async move {
            self.guard("update_workflow")?;
            Ok(Self::workflow(name))
        }
    }

    fn delete_workflow<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.workflow_log.lock().unwrap().push((
            "delete".to_owned(),
            name.to_owned(),
            String::new(),
        ));
        async move { self.guard("delete_workflow") }
    }

    fn create_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        let name = body
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned();
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), name.clone()));
        async move {
            self.guard("create_scheduler_job")?;
            Ok(Self::job(&name))
        }
    }

    fn get_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_scheduler_job")?;
            Ok(Self::job(name))
        }
    }

    fn patch_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), name.to_owned()));
        async move {
            self.guard("patch_scheduler_job")?;
            Ok(Self::job(name))
        }
    }

    fn pause_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("pause".to_owned(), name.to_owned()));
        async move {
            self.guard("pause_scheduler_job")?;
            let mut job = Self::job(name);
            job.state = "PAUSED".to_owned();
            Ok(job)
        }
    }

    fn resume_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("resume".to_owned(), name.to_owned()));
        async move {
            self.guard("resume_scheduler_job")?;
            Ok(Self::job(name))
        }
    }

    fn delete_scheduler_job<'a>(
        &'a self,
        _project: &'a str,
        _region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.scheduler_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), name.to_owned()));
        async move { self.guard("delete_scheduler_job") }
    }
}

/// Forwarded to the embedded BigQuery double so there is one definition of that
/// behaviour, not two that can drift apart.
#[allow(
    clippy::manual_async_fn,
    reason = "plain delegation to the embedded double; an async fn here would \
              add a wrapper future for no benefit"
)]
impl BqOps for MockSchedulerClient {
    type Error = MockError;

    fn execute_ddl<'a>(
        &'a self,
        project: &'a str,
        ddl: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.bq.execute_ddl(project, ddl, max_bytes_billed)
    }

    fn get_table_schema<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<Vec<BqField>, Self::Error>> + Send + 'a {
        self.bq.get_table_schema(project, dataset, table_id)
    }

    fn create_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.bq.create_table(project, dataset, body)
    }

    fn get_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.bq.get_table(project, dataset, table_id)
    }

    fn patch_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.bq.patch_table(project, dataset, table_id, body)
    }

    fn delete_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.bq.delete_table(project, dataset, table_id)
    }

    fn dry_run_query<'a>(
        &'a self,
        project: &'a str,
        sql: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<DryRunResult, Self::Error>> + Send + 'a {
        self.bq.dry_run_query(project, sql, max_bytes_billed)
    }

    fn create_dataset<'a>(
        &'a self,
        project: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.bq.create_dataset(project, body)
    }

    fn get_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.bq.get_dataset(project, dataset_id)
    }

    fn patch_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.bq.patch_dataset(project, dataset_id, body)
    }

    fn delete_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        delete_contents: bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.bq.delete_dataset(project, dataset_id, delete_contents)
    }

    fn create_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.bq.create_routine(project, dataset, body)
    }

    fn get_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.bq.get_routine(project, dataset, routine_id)
    }

    fn update_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.bq.update_routine(project, dataset, routine_id, body)
    }

    fn delete_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.bq.delete_routine(project, dataset, routine_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_workflow_and_scheduler_calls_separately() {
        let c = MockSchedulerClient::new();
        c.create_workflow("p", "r", "w", "- step", "sa")
            .await
            .unwrap();
        c.pause_scheduler_job("p", "r", "j").await.unwrap();
        assert_eq!(c.workflow_log()[0].0, "create");
        assert_eq!(c.workflow_log()[0].2, "- step");
        assert_eq!(c.scheduler_log()[0], ("pause".into(), "j".into()));
    }

    #[tokio::test]
    async fn bigquery_calls_forward_to_the_embedded_double() {
        let c = MockSchedulerClient::new();
        c.execute_ddl("p", "SELECT 1", None).await.unwrap();
        assert_eq!(c.bq.ddl_log(), vec!["SELECT 1"]);
    }

    #[tokio::test]
    async fn pause_reports_the_paused_state() {
        let c = MockSchedulerClient::new();
        assert_eq!(
            c.pause_scheduler_job("p", "r", "j").await.unwrap().state,
            "PAUSED"
        );
    }
}
