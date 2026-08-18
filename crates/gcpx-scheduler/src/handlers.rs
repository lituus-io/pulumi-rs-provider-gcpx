// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::job_lifecycle::delete_scheduler_and_workflow;
use crate::ops::SchedulerOps;
use crate::parse::{build_sqljob_output, parse_sqljob_inputs};
use crate::scheduler_body::{
    build_scheduler_create_body, build_scheduler_patch_body, SchedulerBodyConfig,
};
use crate::types::SqlJobState;
use crate::workflow_template::generate_workflow_yaml;
use gcpx_bq::BqOps;
use gcpx_core::diff_fields;
use gcpx_core::error::IntoStatusWith;
use gcpx_core::handler_util::{build_check_response, build_diff_response};
use gcpx_core::lifecycle::create_or_adopt;
use gcpx_core::prost_util::get_str;
use gcpx_core::resource::require_non_empty;
use gcpx_core::resource::CheckFailure;

fn validate_sqljob(inputs: &crate::types::SqlJobInputs<'_>) -> Vec<CheckFailure> {
    let mut failures = Vec::new();

    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "region", inputs.region);
    require_non_empty(&mut failures, "name", inputs.name);
    require_non_empty(&mut failures, "sql", inputs.sql);
    require_non_empty(&mut failures, "schedule", inputs.schedule);
    require_non_empty(&mut failures, "serviceAccount", inputs.service_account);

    if let Some(rc) = inputs.retry_count {
        if !(0..=5).contains(&rc) {
            failures.push(CheckFailure {
                property: "retryCount".into(),
                reason: "retryCount must be 0-5 (inclusive): use 0 for no retries, or 1-5 for automatic retry".into(),
            });
        }
    }

    failures
}

pub async fn check_sql_job<C: BqOps + SchedulerOps>(
    // Pure: no client needed, but the signature stays uniform so dispatch
    // can treat every handler alike.
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;
    let failures = validate_sqljob(&inputs);

    build_check_response(req.news, failures)
}

pub async fn diff_sql_job<C: BqOps + SchedulerOps>(
    // Pure: no client needed, but the signature stays uniform so dispatch
    // can treat every handler alike.
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    let olds = req
        .olds
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing olds"))?;
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let old_inputs = parse_sqljob_inputs(olds).map_err(Status::internal)?;
    let new_inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;

    let mut replace_keys = Vec::new();
    let mut update_keys = Vec::new();

    diff_fields!(old_inputs, new_inputs, replace_keys, update_keys;
        project => replace,
        region => replace,
        name => replace,
        service_account => replace "serviceAccount",
        sql => update,
        schedule => update,
        time_zone => update "timeZone",
        paused => update,
        description => update,
        retry_count => update "retryCount",
        attempt_deadline => update "attemptDeadline",
    );

    Ok(build_diff_response(&gcpx_core::resource::DiffResult {
        replace_keys,
        update_keys,
    }))
}

pub async fn create_sql_job<C: BqOps + SchedulerOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;

    let inputs = parse_sqljob_inputs(props).map_err(Status::invalid_argument)?;
    let id = format!(
        "sql-job/{}/{}/{}",
        inputs.project, inputs.region, inputs.name
    );

    let wf_name = format!("gcpx-wf-{}", inputs.name);
    let sched_name = format!("gcpx-sched-{}", inputs.name);

    if req.preview {
        let state = SqlJobState {
            workflow_name: format!(
                "projects/{}/locations/{}/workflows/{}",
                inputs.project, inputs.region, wf_name
            ),
            scheduler_job_name: format!(
                "projects/{}/locations/{}/jobs/{}",
                inputs.project, inputs.region, sched_name
            ),
            state: "PREVIEW".to_owned(),
            next_run_time: String::new(),
        };
        let outputs = build_sqljob_output(&inputs, &state);
        return Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(outputs),
            ..Default::default()
        }));
    }

    // Validate SQL via dry-run before creating infrastructure.
    let dry_run = client
        .dry_run_query(inputs.project, inputs.sql, None)
        .await
        .map_err(|e| Status::invalid_argument(format!("SQL validation failed: {e}")))?;
    if !dry_run.valid {
        return Err(Status::invalid_argument(format!(
            "SQL is invalid — scheduler job would fail at runtime: {}",
            dry_run.error_message.unwrap_or_default()
        )));
    }

    // 1. Generate workflow YAML.
    let workflow_yaml = generate_workflow_yaml(inputs.project, inputs.sql);

    // 2. Create GCP Workflow (with 409 auto-adopt).
    let wf_meta = create_or_adopt(
        client.create_workflow(
            inputs.project,
            inputs.region,
            &wf_name,
            &workflow_yaml,
            inputs.service_account,
        ),
        || client.update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml),
        "workflow",
    )
    .await?;

    // 3. Build scheduler job body.
    let sched_cfg = SchedulerBodyConfig {
        project: inputs.project,
        region: inputs.region,
        sched_name: &sched_name,
        wf_name: &wf_name,
        schedule: inputs.schedule,
        time_zone: inputs.time_zone,
        service_account: inputs.service_account,
        description: inputs.description,
        paused: inputs.paused,
        retry_count: inputs.retry_count,
        attempt_deadline: inputs.attempt_deadline,
    };
    let sched_body = build_scheduler_create_body(&sched_cfg);

    // 4. Create Cloud Scheduler job (with 409 auto-adopt).
    let sched_meta = match create_or_adopt(
        client.create_scheduler_job(inputs.project, inputs.region, &sched_body),
        || client.patch_scheduler_job(inputs.project, inputs.region, &sched_name, &sched_body),
        "scheduler job",
    )
    .await
    {
        Ok(m) => m,
        Err(status) => {
            // Rollback: delete the workflow.
            let _ = client
                .delete_workflow(inputs.project, inputs.region, &wf_name)
                .await;
            return Err(status);
        }
    };

    // Cloud Scheduler ignores state in create body — use dedicated pause API.
    let sched_meta = if inputs.paused == Some(true) {
        client
            .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
            .await
            .status_internal_with("failed to pause scheduler job")?
    } else {
        sched_meta
    };

    let state = SqlJobState {
        workflow_name: wf_meta.name,
        scheduler_job_name: sched_meta.name,
        state: sched_meta.state,
        next_run_time: sched_meta.schedule_time,
    };
    let outputs = build_sqljob_output(&inputs, &state);
    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn read_sql_job<C: BqOps + SchedulerOps>(
    // Pure: no client needed, but the signature stays uniform so dispatch
    // can treat every handler alike.
    _client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    // Return stored state.
    Ok(Response::new(pulumirpc::ReadResponse {
        id: req.id,
        properties: req.properties,
        inputs: req.inputs,
        ..Default::default()
    }))
}

pub async fn update_sql_job<C: BqOps + SchedulerOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let inputs = parse_sqljob_inputs(news).map_err(Status::invalid_argument)?;
    let wf_name = format!("gcpx-wf-{}", inputs.name);
    let sched_name = format!("gcpx-sched-{}", inputs.name);

    if !req.preview {
        // Check if SQL changed -> update workflow.
        let old_sql = req
            .olds
            .as_ref()
            .and_then(|o| get_str(&o.fields, "sql"))
            .unwrap_or("");

        if old_sql != inputs.sql {
            // Validate new SQL via dry-run before updating infrastructure.
            let dry_run = client
                .dry_run_query(inputs.project, inputs.sql, None)
                .await
                .map_err(|e| Status::invalid_argument(format!("SQL validation failed: {e}")))?;
            if !dry_run.valid {
                return Err(Status::invalid_argument(format!(
                    "SQL is invalid — scheduler job would fail at runtime: {}",
                    dry_run.error_message.unwrap_or_default()
                )));
            }

            let workflow_yaml = generate_workflow_yaml(inputs.project, inputs.sql);
            client
                .update_workflow(inputs.project, inputs.region, &wf_name, &workflow_yaml)
                .await
                .status_internal_with("failed to update workflow")?;
        }

        // Patch scheduler job (state handled via dedicated pause API below).
        let sched_cfg = SchedulerBodyConfig {
            project: inputs.project,
            region: inputs.region,
            sched_name: &sched_name,
            wf_name: &wf_name,
            schedule: inputs.schedule,
            time_zone: inputs.time_zone,
            service_account: inputs.service_account,
            description: inputs.description,
            paused: inputs.paused,
            retry_count: inputs.retry_count,
            attempt_deadline: inputs.attempt_deadline,
        };
        let patch = build_scheduler_patch_body(&sched_cfg, true);

        client
            .patch_scheduler_job(inputs.project, inputs.region, &sched_name, &patch)
            .await
            .status_internal_with("failed to update scheduler job")?;

        // Cloud Scheduler ignores state in PATCH — use dedicated pause/resume API.
        match inputs.paused {
            Some(true) => {
                client
                    .pause_scheduler_job(inputs.project, inputs.region, &sched_name)
                    .await
                    .status_internal_with("failed to pause scheduler job")?;
            }
            Some(false) => {
                client
                    .resume_scheduler_job(inputs.project, inputs.region, &sched_name)
                    .await
                    .status_internal_with("failed to resume scheduler job")?;
            }
            None => {}
        }
    }

    let state = SqlJobState {
        workflow_name: format!(
            "projects/{}/locations/{}/workflows/{}",
            inputs.project, inputs.region, wf_name
        ),
        scheduler_job_name: format!(
            "projects/{}/locations/{}/jobs/{}",
            inputs.project, inputs.region, sched_name
        ),
        state: if inputs.paused == Some(true) {
            "PAUSED"
        } else {
            "ENABLED"
        }
        .to_owned(),
        next_run_time: String::new(),
    };
    let outputs = build_sqljob_output(&inputs, &state);

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn delete_sql_job<C: BqOps + SchedulerOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    if let Some(ref props) = req.properties {
        let project = get_str(&props.fields, "project").unwrap_or("");
        let region = get_str(&props.fields, "region").unwrap_or("");
        let name = get_str(&props.fields, "name").unwrap_or("");

        if !project.is_empty() && !region.is_empty() && !name.is_empty() {
            let sched_name = format!("gcpx-sched-{}", name);
            let wf_name = format!("gcpx-wf-{}", name);
            return delete_scheduler_and_workflow(client, project, region, &sched_name, &wf_name)
                .await;
        }
    }

    Ok(Response::new(()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockSchedulerClient;
    use gcpx_bq::types::DryRunResult;
    use gcpx_core::prost_util::prost_string;

    fn make_sqljob_struct(
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
        schedule: &str,
        sa: &str,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string(project)),
                ("region".to_owned(), prost_string(region)),
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                ("schedule".to_owned(), prost_string(schedule)),
                ("timeZone".to_owned(), prost_string("UTC")),
                ("serviceAccount".to_owned(), prost_string(sa)),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[tokio::test]
    async fn create_sql_job_dry_run_invalid_sql_rejects() {
        let client = MockSchedulerClient::with_bq(gcpx_bq::mock::MockBqClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("Syntax error at position 7".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..Default::default()
        });
        let props =
            make_sqljob_struct("proj", "us-central1", "j", "SELEC 1", "0 * * * *", "sa@iam");
        let result = create_sql_job(
            &client,
            pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL is invalid"));
        assert!(err.message().contains("Syntax error"));

        // No workflow or scheduler should have been created.
        assert!(client.workflow_log.lock().unwrap().is_empty());
        assert!(client.scheduler_log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_sql_job_dry_run_error_rejects() {
        let client = MockSchedulerClient::with_bq(gcpx_bq::mock::MockBqClient {
            fail_on: std::sync::Mutex::new(Some("dry_run_query".to_owned())),
            ..Default::default()
        });
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = create_sql_job(
            &client,
            pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL validation failed"));
    }

    #[tokio::test]
    async fn create_sql_job_preview_skips_dry_run() {
        // Even with invalid dry-run, preview should succeed.
        let client = MockSchedulerClient::with_bq(gcpx_bq::mock::MockBqClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("invalid".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..Default::default()
        });
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = create_sql_job(
            &client,
            pulumirpc::CreateRequest {
                properties: Some(props),
                preview: true,
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn create_sql_job_valid_sql_succeeds() {
        let client = MockSchedulerClient::new();
        let props = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = create_sql_job(
            &client,
            pulumirpc::CreateRequest {
                properties: Some(props),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_ok());
        let inner = result.unwrap().into_inner();
        assert_eq!(inner.id, "sql-job/proj/us-central1/j");
    }

    #[tokio::test]
    async fn update_sql_job_dry_run_invalid_rejects() {
        let client = MockSchedulerClient::with_bq(gcpx_bq::mock::MockBqClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("bad SQL".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..Default::default()
        });
        let olds = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let news = make_sqljob_struct("proj", "us-central1", "j", "SELEC 2", "0 * * * *", "sa@iam");
        let result = update_sql_job(
            &client,
            pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("SQL is invalid"));
    }

    #[tokio::test]
    async fn update_sql_job_same_sql_skips_dry_run() {
        // Same SQL means no dry-run or workflow update.
        let client = MockSchedulerClient::with_bq(gcpx_bq::mock::MockBqClient {
            dry_run_result: Some(DryRunResult {
                valid: false,
                error_message: Some("would fail if called".to_owned()),
                total_bytes_processed: 0,
                schema: Vec::new(),
            }),
            ..Default::default()
        });
        let inputs = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = update_sql_job(
            &client,
            pulumirpc::UpdateRequest {
                olds: Some(inputs.clone()),
                news: Some(inputs),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_ok());
    }

    fn make_sqljob_struct_with_paused(
        project: &str,
        region: &str,
        name: &str,
        sql: &str,
        schedule: &str,
        sa: &str,
        paused: bool,
    ) -> prost_types::Struct {
        prost_types::Struct {
            fields: vec![
                ("project".to_owned(), prost_string(project)),
                ("region".to_owned(), prost_string(region)),
                ("name".to_owned(), prost_string(name)),
                ("sql".to_owned(), prost_string(sql)),
                ("schedule".to_owned(), prost_string(schedule)),
                ("timeZone".to_owned(), prost_string("UTC")),
                ("serviceAccount".to_owned(), prost_string(sa)),
                (
                    "paused".to_owned(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::BoolValue(paused)),
                    },
                ),
            ]
            .into_iter()
            .collect(),
        }
    }

    #[tokio::test]
    async fn update_sql_job_paused_false_calls_resume() {
        let client = MockSchedulerClient::new();
        let olds = make_sqljob_struct_with_paused(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
            true,
        );
        let news = make_sqljob_struct_with_paused(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
            false,
        );
        let result = update_sql_job(
            &client,
            pulumirpc::UpdateRequest {
                olds: Some(olds),
                news: Some(news),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_ok());
        let sched_log = client.scheduler_log.lock().unwrap().clone();
        assert!(
            sched_log.iter().any(|(op, _)| op == "resume"),
            "expected resume call in scheduler log: {:?}",
            sched_log,
        );
    }

    #[tokio::test]
    async fn update_sql_job_paused_none_skips_pause_resume() {
        let client = MockSchedulerClient::new();
        let inputs = make_sqljob_struct(
            "proj",
            "us-central1",
            "j",
            "SELECT 1",
            "0 * * * *",
            "sa@iam",
        );
        let result = update_sql_job(
            &client,
            pulumirpc::UpdateRequest {
                olds: Some(inputs.clone()),
                news: Some(inputs),
                preview: false,
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_ok());
        let sched_log = client.scheduler_log.lock().unwrap().clone();
        assert!(
            !sched_log
                .iter()
                .any(|(op, _)| op == "pause" || op == "resume"),
            "expected no pause/resume calls: {:?}",
            sched_log,
        );
    }
}
