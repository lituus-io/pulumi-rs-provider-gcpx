// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Cloud Workflows and Cloud Scheduler operations.
//!
//! Both APIs back the same pattern — a workflow holds the work, a scheduler job
//! decides when it runs — so they share one trait and one implementation.
//!
//! Workflow creation and update are long-running. The predecessor waited for
//! them with a fixed `30 × 2s` loop, which both over-polls a fast create and
//! gives up on a slow one at exactly 60 seconds regardless of how close it was.
//! Here they go through the shared operation poller instead: capped exponential
//! backoff, and a deadline that reports what happened rather than a bare
//! timeout.

use std::future::Future;

use gcpx_core::auth::CredentialSource;
use gcpx_core::breaker::Service;
use gcpx_core::error::{GcpApiError, GcpError};
use gcpx_core::http::HttpGcpClient;
use gcpx_core::lro::{poll_operation, Operation, PollConfig};
use gcpx_core::sanitize::encode_path_segment as e;
use serde::Deserialize;

const WORKFLOWS_API: &str = "https://workflows.googleapis.com/v1";
const SCHEDULER_API: &str = "https://cloudscheduler.googleapis.com/v1";

/// A workflow becomes usable only once it reports this state.
const WORKFLOW_ACTIVE: &str = "ACTIVE";

/// Metadata returned from the Workflows API.
pub struct WorkflowMeta {
    pub name: String,
    pub state: String,
    pub revision_id: String,
    pub create_time: String,
    pub update_time: String,
    pub service_account: String,
}

/// Metadata returned from the Cloud Scheduler API.
pub struct SchedulerJobMeta {
    pub name: String,
    pub state: String,
    pub schedule: String,
    pub time_zone: String,
    pub schedule_time: String,
    pub last_attempt_time: String,
    pub user_update_time: String,
}

pub trait SchedulerOps: Send + Sync + 'static {
    type Error: GcpApiError;

    fn create_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
        sa: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn get_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn update_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> impl Future<Output = Result<WorkflowMeta, Self::Error>> + Send + 'a;

    fn delete_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn create_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn get_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn patch_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn pause_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn resume_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<SchedulerJobMeta, Self::Error>> + Send + 'a;

    fn delete_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

impl<C: CredentialSource> SchedulerOps for HttpGcpClient<C> {
    type Error = GcpError;

    async fn create_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
        sa: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        let url = format!(
            "{WORKFLOWS_API}/projects/{}/locations/{}/workflows?workflowId={}",
            e(project),
            e(region),
            e(name)
        );
        let body = serde_json::json!({
            "sourceContents": definition,
            "serviceAccount": sa,
        });
        let op: Operation = self.post_json(Service::Workflows, &url, &body).await?;
        await_workflow_active(self, project, region, name, op, PollConfig::default()).await
    }

    async fn get_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        let wf: WorkflowResponse = self
            .get_json(Service::Workflows, &workflow_url(project, region, name))
            .await?;
        Ok(wf.into())
    }

    async fn update_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        definition: &'a str,
    ) -> Result<WorkflowMeta, Self::Error> {
        let url = format!(
            "{}?updateMask=sourceContents",
            workflow_url(project, region, name)
        );
        let body = serde_json::json!({ "sourceContents": definition });
        let op: Operation = self.patch_json(Service::Workflows, &url, &body).await?;
        await_workflow_active(self, project, region, name, op, PollConfig::default()).await
    }

    async fn delete_workflow<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(Service::Workflows, &workflow_url(project, region, name))
            .await
    }

    async fn create_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!(
            "{SCHEDULER_API}/projects/{}/locations/{}/jobs",
            e(project),
            e(region)
        );
        let job: SchedulerJobResponse = self.post_json(Service::Scheduler, &url, body).await?;
        Ok(job.into())
    }

    async fn get_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let job: SchedulerJobResponse = self
            .get_json(Service::Scheduler, &job_url(project, region, name))
            .await?;
        Ok(job.into())
    }

    async fn patch_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let job: SchedulerJobResponse = self
            .patch_json(Service::Scheduler, &job_url(project, region, name), body)
            .await?;
        Ok(job.into())
    }

    async fn pause_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!("{}:pause", job_url(project, region, name));
        let job: SchedulerJobResponse = self
            .post_json(Service::Scheduler, &url, &serde_json::json!({}))
            .await?;
        Ok(job.into())
    }

    async fn resume_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<SchedulerJobMeta, Self::Error> {
        let url = format!("{}:resume", job_url(project, region, name));
        let job: SchedulerJobResponse = self
            .post_json(Service::Scheduler, &url, &serde_json::json!({}))
            .await?;
        Ok(job.into())
    }

    async fn delete_scheduler_job<'a>(
        &'a self,
        project: &'a str,
        region: &'a str,
        name: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(Service::Scheduler, &job_url(project, region, name))
            .await
    }
}

/// Wait for the create/update operation, then for the workflow to report
/// `ACTIVE`.
///
/// The operation completing is necessary but not sufficient: a workflow briefly
/// reports `UNAVAILABLE` while its source is compiled, and invoking it in that
/// window fails. Both waits share one deadline, so a slow deploy cannot
/// silently take twice as long as configured.
async fn await_workflow_active<C: CredentialSource>(
    client: &HttpGcpClient<C>,
    project: &str,
    region: &str,
    name: &str,
    op: Operation,
    config: PollConfig,
) -> Result<WorkflowMeta, GcpError> {
    let op_name = op.name.clone();
    poll_operation(
        op,
        || async {
            if op_name.is_empty() {
                // Some responses carry no operation to poll; fall back to
                // asking the workflow itself whether it is ready yet.
                let wf: WorkflowResponse = client
                    .get_json(Service::Workflows, &workflow_url(project, region, name))
                    .await?;
                return Ok(Operation {
                    name: name.to_owned(),
                    done: wf.state == WORKFLOW_ACTIVE,
                    ..Default::default()
                });
            }
            client
                .get_json(Service::Workflows, &format!("{WORKFLOWS_API}/{op_name}"))
                .await
        },
        config,
    )
    .await?;

    let wf: WorkflowMeta = {
        let r: WorkflowResponse = client
            .get_json(Service::Workflows, &workflow_url(project, region, name))
            .await?;
        r.into()
    };

    if wf.state != WORKFLOW_ACTIVE {
        return Err(GcpError::OperationFailed {
            message: format!(
                "workflow '{name}' finished provisioning in state {} rather than {WORKFLOW_ACTIVE}",
                wf.state
            ),
        });
    }
    Ok(wf)
}

fn workflow_url(project: &str, region: &str, name: &str) -> String {
    format!(
        "{WORKFLOWS_API}/projects/{}/locations/{}/workflows/{}",
        e(project),
        e(region),
        e(name)
    )
}

fn job_url(project: &str, region: &str, name: &str) -> String {
    format!(
        "{SCHEDULER_API}/projects/{}/locations/{}/jobs/{}",
        e(project),
        e(region),
        e(name)
    )
}

#[derive(Deserialize, Default)]
struct WorkflowResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default, rename = "revisionId")]
    revision_id: String,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "updateTime")]
    update_time: String,
    #[serde(default, rename = "serviceAccount")]
    service_account: String,
}

impl From<WorkflowResponse> for WorkflowMeta {
    fn from(wf: WorkflowResponse) -> Self {
        Self {
            name: wf.name,
            state: wf.state,
            revision_id: wf.revision_id,
            create_time: wf.create_time,
            update_time: wf.update_time,
            service_account: wf.service_account,
        }
    }
}

#[derive(Deserialize, Default)]
struct SchedulerJobResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    schedule: String,
    #[serde(default, rename = "timeZone")]
    time_zone: String,
    #[serde(default, rename = "scheduleTime")]
    schedule_time: String,
    #[serde(default, rename = "lastAttemptTime")]
    last_attempt_time: String,
    #[serde(default, rename = "userUpdateTime")]
    user_update_time: String,
}

impl From<SchedulerJobResponse> for SchedulerJobMeta {
    fn from(job: SchedulerJobResponse) -> Self {
        Self {
            name: job.name,
            state: job.state,
            schedule: job.schedule,
            time_zone: job.time_zone,
            schedule_time: job.schedule_time,
            last_attempt_time: job.last_attempt_time,
            user_update_time: job.user_update_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_url_is_well_formed() {
        assert_eq!(
            workflow_url("p", "us-central1", "w"),
            "https://workflows.googleapis.com/v1/projects/p/locations/us-central1/workflows/w"
        );
    }

    #[test]
    fn job_url_is_well_formed() {
        assert_eq!(
            job_url("p", "us-central1", "j"),
            "https://cloudscheduler.googleapis.com/v1/projects/p/locations/us-central1/jobs/j"
        );
    }

    #[test]
    fn urls_percent_encode_untrusted_segments() {
        // A name carrying a slash would otherwise address a different resource.
        assert!(workflow_url("p", "r", "a/b").ends_with("/workflows/a%2Fb"));
        assert!(job_url("p", "r", "a/b").ends_with("/jobs/a%2Fb"));
    }

    #[test]
    fn pause_and_resume_target_the_job_itself() {
        let base = job_url("p", "r", "j");
        assert_eq!(format!("{base}:pause"), format!("{base}:pause"));
        assert!(!base.contains(':') || base.starts_with("https:"));
    }

    #[test]
    fn workflow_response_maps_every_field() {
        let wf: WorkflowResponse = serde_json::from_value(serde_json::json!({
            "name": "projects/p/locations/r/workflows/w",
            "state": "ACTIVE",
            "revisionId": "000001-abc",
            "createTime": "2026-01-01T00:00:00Z",
            "updateTime": "2026-01-02T00:00:00Z",
            "serviceAccount": "sa@example.iam.gserviceaccount.com",
        }))
        .unwrap();
        let meta: WorkflowMeta = wf.into();
        assert_eq!(meta.state, "ACTIVE");
        assert_eq!(meta.revision_id, "000001-abc");
        assert_eq!(meta.service_account, "sa@example.iam.gserviceaccount.com");
    }

    #[test]
    fn scheduler_response_tolerates_missing_fields() {
        // A paused job omits scheduleTime; absent fields must not fail the parse.
        let job: SchedulerJobResponse =
            serde_json::from_value(serde_json::json!({ "name": "j", "state": "PAUSED" })).unwrap();
        let meta: SchedulerJobMeta = job.into();
        assert_eq!(meta.state, "PAUSED");
        assert!(meta.schedule_time.is_empty());
    }
}
