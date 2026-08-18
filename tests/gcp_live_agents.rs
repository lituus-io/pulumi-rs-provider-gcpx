// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Live Conversational Analytics tests.
//!
//! Skipped unless a project is configured:
//!
//! ```text
//! GCPX_TEST_PROJECT=my-project GCPX_TEST_DATASET=my_dataset \
//!   cargo test --test gcp_live_agents -- --test-threads=1 --nocapture
//! ```
//!
//! Requires `geminidataanalytics.googleapis.com` to be enabled, and the caller
//! to have read access to the tables the agents are grounded on — the API
//! rejects a grounding it cannot read, which is a good failure but an opaque
//! one if you are not expecting it.
//!
//! These exist because the double cannot tell us Google accepts what we send.
//! Everything here is about the wire: does `createSync` exist, does the body
//! shape match, does the response deserialize, does publishing behave the way
//! the resource claims.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use gcpx_agents::api_body::{agent_update_mask, build_agent_body};
use gcpx_agents::types::*;
use gcpx_agents::DataAgentOps;
use gcpx_core::auth::AdcCredentials;
use gcpx_core::http::HttpGcpClient;

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

/// A dataset holding a table the caller can read; the API refuses to ground an
/// agent on anything else.
fn dataset() -> String {
    std::env::var("GCPX_TEST_DATASET").unwrap_or_else(|_| "wb_mock_lab_dataset".to_owned())
}

fn table() -> String {
    std::env::var("GCPX_TEST_TABLE").unwrap_or_else(|_| "gcpx_probe".to_owned())
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
                eprintln!("SKIP: set GCPX_TEST_PROJECT to run the live agent suite");
                return;
            }
        }
    };
}

fn inputs<'a>(
    project: &'a str,
    agent_id: &'a str,
    dataset: &'a str,
    table: &'a str,
    publish: bool,
) -> DataAgentInputs<'a> {
    DataAgentInputs {
        project,
        location: "global",
        agent_id,
        display_name: Some("gcpx live suite"),
        description: Some("created by the gcpx live test suite"),
        labels: Default::default(),
        kms_key: None,
        context: AgentContext {
            system_instruction: "You are a test analyst. Answer questions about the sample table.",
            datasources: Datasources::BigQuery(vec![TableRef {
                project,
                dataset,
                table,
            }]),
            options: AgentOptions::default(),
            example_queries: vec![],
            glossary_terms: vec![],
        },
        publish,
    }
}

/// Ensure the grounding table exists; the API rejects an agent whose tables it
/// cannot read, and the message does not say which table.
async fn ensure_grounding_table(project: &str) {
    use gcpx_bq::BqOps;
    let c = client();
    let ds = dataset();
    let tbl = table();
    if c.get_table(project, &ds, &tbl).await.is_ok() {
        return;
    }
    c.create_table(
        project,
        &ds,
        &serde_json::json!({
            "tableReference": { "projectId": project, "datasetId": ds, "tableId": tbl },
            "schema": { "fields": [
                { "name": "id", "type": "INT64" },
                { "name": "amount", "type": "FLOAT64" },
            ]},
        }),
    )
    .await
    .expect("create grounding table");
}

/// Create an agent, run `body`, then always delete it.
async fn with_agent<F, Fut>(project: &str, agent_id: &str, publish: bool, body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let c = client();
    let ds = dataset();
    let tbl = table();
    let request = build_agent_body(&inputs(project, agent_id, &ds, &tbl, publish));

    c.create_data_agent(project, "global", agent_id, &request)
        .await
        .unwrap_or_else(|e| panic!("create_data_agent({agent_id}): {e}"));

    let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body())).await;

    match c.delete_data_agent(project, "global", agent_id).await {
        Ok(()) => println!("cleaned up agent {agent_id}"),
        Err(e) if outcome.is_ok() => panic!("teardown failed for agent {agent_id}: {e}"),
        Err(e) => eprintln!("teardown failed for agent {agent_id}: {e}"),
    }

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// The assumption the whole design rests on: the synchronous method exists and
/// returns the resource, so a create is one round-trip rather than a create
/// plus indeterminate polling.
#[tokio::test]
async fn create_sync_returns_the_agent_directly() {
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("sync");
    let c = client();

    let request = build_agent_body(&inputs(&project, &agent_id, &dataset(), &table(), false));
    let meta = c
        .create_data_agent(&project, "global", &agent_id, &request)
        .await
        .expect("createSync should exist and return the agent");

    println!("created {}", meta.name);
    assert!(meta.name.ends_with(&agent_id), "name was {}", meta.name);
    assert!(
        !meta.create_time.is_empty(),
        "createTime should be populated"
    );
    assert!(
        !meta.published,
        "an unpublished agent must not report a published context"
    );

    c.delete_data_agent(&project, "global", &agent_id)
        .await
        .expect("delete");
}

#[tokio::test]
async fn publishing_is_visible_in_the_read_back_state() {
    // `published` is derived from what the service reports, not from what we
    // asked for, so this is the test that the derivation is right.
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("publish");

    with_agent(&project, &agent_id, true, || async {
        let meta = client()
            .get_data_agent(&project, "global", &agent_id)
            .await
            .expect("get_data_agent");
        println!("published={} name={}", meta.published, meta.name);
        assert!(
            meta.published,
            "an agent created with publish should report a published context"
        );
    })
    .await;
}

#[tokio::test]
async fn an_unpublished_agent_reports_staging_only() {
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("staging");

    with_agent(&project, &agent_id, false, || async {
        let meta = client()
            .get_data_agent(&project, "global", &agent_id)
            .await
            .expect("get_data_agent");
        assert!(
            !meta.published,
            "a staged context must not be reported as live"
        );
    })
    .await;
}

/// Update is the other half of sync-first, and the update mask is what decides
/// whether the patch changes anything.
#[tokio::test]
async fn update_applies_and_can_promote_to_published() {
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("update");

    with_agent(&project, &agent_id, false, || async {
        let c = client();
        let ds = dataset();
        let tbl = table();

        let promoted = inputs(&project, &agent_id, &ds, &tbl, true);
        let body = build_agent_body(&promoted);
        let mask = agent_update_mask(&promoted);
        println!("updateMask: {mask}");

        c.update_data_agent(&project, "global", &agent_id, &body, &mask)
            .await
            .expect("updateSync");

        let after = c
            .get_data_agent(&project, "global", &agent_id)
            .await
            .expect("get after update");
        assert!(
            after.published,
            "promoting a staged context should publish it"
        );
    })
    .await;
}

/// A deleted agent must read back as gone, or the provider would report a
/// resource that no longer exists as still present.
#[tokio::test]
async fn a_deleted_agent_reads_back_as_missing() {
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("gone");
    let c = client();

    let request = build_agent_body(&inputs(&project, &agent_id, &dataset(), &table(), false));
    c.create_data_agent(&project, "global", &agent_id, &request)
        .await
        .expect("create");
    c.delete_data_agent(&project, "global", &agent_id)
        .await
        .expect("delete");

    let after = c.get_data_agent(&project, "global", &agent_id).await;
    match after {
        Err(e) => {
            use gcpx_core::error::GcpApiError;
            println!("after delete: {e}");
            assert!(
                e.is_not_found(),
                "a deleted agent should read as not-found, got: {e}"
            );
        }
        // The API keeps a soft-deleted agent addressable for a window; that is
        // acceptable as long as it is marked, which is what deleteTime is for.
        Ok(meta) => assert!(
            !meta.delete_time.is_empty(),
            "a deleted agent should either be gone or carry a deleteTime"
        ),
    }
}

/// Grounding an agent on a table the caller cannot read is rejected. Worth
/// pinning because the message names neither the table nor the permission, and
/// it is the first thing to go wrong in a new project.
#[tokio::test]
async fn grounding_on_an_unreadable_table_is_rejected() {
    let project = require_project!();
    let agent_id = unique("badtable");
    let c = client();

    let request = build_agent_body(&inputs(
        &project,
        &agent_id,
        &dataset(),
        "table_that_does_not_exist",
        false,
    ));
    let err = c
        .create_data_agent(&project, "global", &agent_id, &request)
        .await
        .expect_err("an unreadable grounding table should be rejected");
    println!("rejection: {err}");
    assert!(
        err.to_string().contains("BigQuery") || err.to_string().contains("access"),
        "unexpected rejection: {err}"
    );
}

#[tokio::test]
async fn iam_policy_round_trips() {
    let project = require_project!();
    ensure_grounding_table(&project).await;
    let agent_id = unique("iam");

    with_agent(&project, &agent_id, false, || async {
        let c = client();
        let policy = c
            .get_agent_iam_policy(&project, "global", &agent_id)
            .await
            .expect("getIamPolicy");
        println!(
            "policy etag={:?} bindings={}",
            policy.etag,
            policy.bindings.len()
        );

        // Writing the policy straight back must be accepted: that round-trip is
        // what every additive merge is built on.
        let body = gcpx_agents::api_body::build_iam_policy_body(&[], &policy.etag);
        c.set_agent_iam_policy(&project, "global", &agent_id, &body)
            .await
            .expect("setIamPolicy should accept the policy it just returned");
    })
    .await;
}
