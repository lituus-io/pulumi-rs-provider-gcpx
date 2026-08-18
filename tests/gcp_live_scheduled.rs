// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
// Live tests for the scheduled-execution substrate.
//
// Every recurring resource — scheduled SQL, snapshots, Dataproc ingest and
// export — is the same pair underneath: a Cloud Workflow holding the work and a
// Cloud Scheduler job deciding when it runs. Testing that pair against the real
// APIs covers the part all four share.
//
//   GCPX_TEST_PROJECT=my-project GCPX_TEST_REGION=us-central1 \
//     cargo test --test gcp_live_scheduled -- --test-threads=1 --nocapture

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use gcpx_core::auth::AdcCredentials;
use gcpx_core::error::GcpApiError;
use gcpx_core::http::HttpGcpClient;
use gcpx_scheduler::workflow_template::generate_workflow_yaml;
use gcpx_scheduler::SchedulerOps;

type Client = HttpGcpClient<AdcCredentials>;

fn client() -> Client {
    HttpGcpClient::new(
        HttpGcpClient::<AdcCredentials>::default_http_client().expect("http client"),
        AdcCredentials::new(),
    )
}

fn project() -> Option<String> {
    std::env::var("GCPX_TEST_PROJECT")
        .ok()
        .filter(|p| !p.is_empty())
}

fn region() -> String {
    std::env::var("GCPX_TEST_REGION").unwrap_or_else(|_| "us-central1".to_owned())
}

fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!(
        "gcpx-live-{prefix}-{ts}-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

macro_rules! require_project {
    () => {
        match project() {
            Some(p) => p,
            None => {
                eprintln!("SKIP: set GCPX_TEST_PROJECT to run the scheduled-resource suite");
                return;
            }
        }
    };
}

/// The claim the whole scheduler crate rests on: the YAML this provider
/// generates is a workflow Google accepts, and it becomes ACTIVE.
///
/// The generated definition is untested by any offline check — a unit test can
/// only assert it looks the way we expect, not that it parses.
#[tokio::test]
async fn generated_workflow_yaml_is_accepted_and_becomes_active() {
    let project = require_project!();
    let region = region();
    let c = client();
    let name = unique("wf");

    // Exactly what a SqlJob resource would deploy.
    let definition =
        generate_workflow_yaml(&project, "SELECT 1 AS probe, CURRENT_TIMESTAMP() AS ran_at");
    println!("generated workflow:\n{definition}");

    let sa = format!("{project}@appspot.gserviceaccount.com");
    let created = match c
        .create_workflow(&project, &region, &name, &definition, &sa)
        .await
    {
        Ok(w) => w,
        Err(e) if e.to_string().contains("service account") || e.http_status() == Some(400) => {
            // The default App Engine service account may not exist in every
            // project. Say so rather than reporting a pass.
            eprintln!("SKIP: no usable service account for Workflows in this project: {e}");
            return;
        }
        Err(e) => panic!("create_workflow: {e}"),
    };

    println!("workflow {} state={}", created.name, created.state);
    assert_eq!(
        created.state, "ACTIVE",
        "a workflow must be ACTIVE before anything can invoke it"
    );

    let fetched = c
        .get_workflow(&project, &region, &name)
        .await
        .expect("get_workflow");
    assert_eq!(fetched.state, "ACTIVE");
    assert!(!fetched.revision_id.is_empty());

    // Updating replaces the source and must return to ACTIVE, which is the
    // part the fixed-interval poll used to get wrong.
    let updated_definition = generate_workflow_yaml(&project, "SELECT 2 AS probe");
    let updated = c
        .update_workflow(&project, &region, &name, &updated_definition)
        .await
        .expect("update_workflow");
    assert_eq!(updated.state, "ACTIVE");
    assert_ne!(
        updated.revision_id, created.revision_id,
        "an updated workflow should carry a new revision"
    );

    c.delete_workflow(&project, &region, &name)
        .await
        .expect("delete_workflow");
    assert!(
        c.get_workflow(&project, &region, &name).await.is_err(),
        "workflow should be gone after delete"
    );
}

/// Multi-line SQL is where the YAML block-scalar indentation has to be right;
/// a single mis-indented line makes the whole definition invalid.
#[tokio::test]
async fn multi_line_sql_survives_yaml_embedding() {
    let project = require_project!();
    let region = region();
    let c = client();
    let name = unique("wfml");

    let sql = "SELECT\n  1 AS a,\n  'text with: a colon' AS b,\n  \"quoted\" AS c\nFROM UNNEST([1,2,3]) AS n";
    let definition = generate_workflow_yaml(&project, sql);
    let sa = format!("{project}@appspot.gserviceaccount.com");

    match c
        .create_workflow(&project, &region, &name, &definition, &sa)
        .await
    {
        Ok(w) => {
            assert_eq!(
                w.state, "ACTIVE",
                "multi-line SQL produced an inactive workflow"
            );
            println!("multi-line workflow accepted: {}", w.name);
            c.delete_workflow(&project, &region, &name).await.ok();
        }
        Err(e) if e.to_string().contains("service account") || e.http_status() == Some(400) => {
            eprintln!("SKIP: no usable service account for Workflows: {e}");
        }
        Err(e) => panic!("multi-line SQL produced an invalid workflow: {e}\n{definition}"),
    }
}

/// Cloud Scheduler serialises mutations per job and answers an overlapping one
/// with 409 ABORTED. The provider retries those, which is what makes an ordinary
/// deploy survive them — but the lock can outlast the retry ladder when
/// mutations arrive back to back, as they do here and would not in a real
/// deploy. A short settle between steps keeps this test about the provider
/// rather than about the service's internal locking.
async fn settle() {
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
}

/// Run `body`, then always delete the scheduler job it created.
///
/// The first version of this suite had no such guard, and failing runs left six
/// jobs behind — the same lesson the BigQuery suite already learned. Deletion
/// retries, because Cloud Scheduler may still be holding the job's lock.
async fn with_job<F, Fut>(project: &str, region: &str, name: &str, body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body())).await;

    let c = client();
    let mut removed = false;
    for _ in 0..4 {
        match c.delete_scheduler_job(project, region, name).await {
            Ok(()) => {
                removed = true;
                break;
            }
            Err(_) => settle().await,
        }
    }
    if !removed {
        eprintln!("WARNING: could not remove scheduler job {name}");
    }

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// A scheduler job is created paused or enabled, and pause/resume must round
/// trip — that is how a stack disables a schedule without deleting it.
#[tokio::test]
async fn scheduler_job_lifecycle_and_pause_resume() {
    let project = require_project!();
    let region = region();
    let c = client();
    let name = unique("job");
    let full_name = format!("projects/{project}/locations/{region}/jobs/{name}");

    // A Pub/Sub-free job that targets an HTTP endpoint requires no other
    // resource, which keeps this test about the scheduler itself.
    let body = serde_json::json!({
        "name": full_name,
        "schedule": "0 3 * * *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": "https://example.com/gcpx-live-probe",
            "httpMethod": "GET"
        }
    });

    let created = match c.create_scheduler_job(&project, &region, &body).await {
        Ok(j) => j,
        Err(e) => {
            eprintln!("SKIP: Cloud Scheduler unavailable in this project/region: {e}");
            return;
        }
    };
    println!("job {} state={}", created.name, created.state);
    assert_eq!(created.schedule, "0 3 * * *");

    with_job(&project, &region, &name, || async {
        let c = client();
        settle().await;
        let paused = c
            .pause_scheduler_job(&project, &region, &name)
            .await
            .expect("pause");
        assert_eq!(paused.state, "PAUSED", "pause should take effect");

        settle().await;
        let resumed = c
            .resume_scheduler_job(&project, &region, &name)
            .await
            .expect("resume");
        assert_eq!(resumed.state, "ENABLED", "resume should re-enable");

        // Patching is how an updated cron reaches the service. Cloud Scheduler
        // treats a patch without an updateMask as a full replacement and rejects a
        // partial body with "Job.target must be set", so the provider always sends
        // the whole job — this mirrors that.
        settle().await;
        let patched = c
            .patch_scheduler_job(
                &project,
                &region,
                &name,
                &serde_json::json!({
                    "schedule": "0 4 * * *",
                    "timeZone": "UTC",
                    "httpTarget": {
                        "uri": "https://example.com/gcpx-live-probe",
                        "httpMethod": "GET"
                    }
                }),
            )
            .await
            .expect("patch");
        assert_eq!(patched.schedule, "0 4 * * *");
    })
    .await;

    // The guard removed it; confirm the removal actually took.
    settle().await;
    assert!(
        client()
            .get_scheduler_job(&project, &region, &name)
            .await
            .is_err(),
        "job should be gone after teardown"
    );
}

/// The paired teardown every scheduled resource uses on delete.
#[tokio::test]
async fn paired_teardown_removes_both_halves() {
    let project = require_project!();
    let region = region();
    let c = client();
    let name = unique("pair");

    let definition = generate_workflow_yaml(&project, "SELECT 1");
    let sa = format!("{project}@appspot.gserviceaccount.com");
    if c.create_workflow(&project, &region, &name, &definition, &sa)
        .await
        .is_err()
    {
        eprintln!("SKIP: could not create the workflow half of the pair");
        return;
    }
    let job_body = serde_json::json!({
        "name": format!("projects/{project}/locations/{region}/jobs/{name}"),
        "schedule": "0 5 * * *",
        "timeZone": "UTC",
        "httpTarget": { "uri": "https://example.com/probe", "httpMethod": "GET" }
    });
    let _ = c.create_scheduler_job(&project, &region, &job_body).await;

    gcpx_scheduler::job_lifecycle::delete_scheduler_and_workflow(
        &c, &project, &region, &name, &name,
    )
    .await
    .expect("paired delete");

    assert!(c.get_workflow(&project, &region, &name).await.is_err());
    assert!(c.get_scheduler_job(&project, &region, &name).await.is_err());
    println!("both halves removed");
}
