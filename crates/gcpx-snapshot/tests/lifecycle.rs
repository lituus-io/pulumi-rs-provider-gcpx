// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The SCD Type 2 snapshot resource, end to end against the client double.
//!
//! A snapshot is a workflow-and-scheduler pair like the other recurring
//! resources, but with a MERGE that closes the previous version of a row and
//! opens a new one. Getting that wrong loses history silently — the table still
//! has rows, just not the ones it should — so the lifecycle is worth driving
//! rather than leaving to the live suite, which does not run in CI.

use gcpx_scheduler::mock::MockSchedulerClient;
use gcpx_snapshot::handlers::*;
use prost_types::{value::Kind, Struct, Value};
use pulumi_rs_yaml_proto::pulumirpc;

fn v(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_owned())),
    }
}
fn st(pairs: Vec<(&str, Value)>) -> Struct {
    Struct {
        fields: pairs.into_iter().map(|(k, x)| (k.to_owned(), x)).collect(),
    }
}

fn inputs() -> Struct {
    st(vec![
        ("project", v("p")),
        ("region", v("us-central1")),
        ("dataset", v("d")),
        ("name", v("dim_customers")),
        ("sourceSql", v("SELECT id, name, updated_at FROM `p.d.stg`")),
        ("uniqueKey", v("id")),
        ("strategy", v("timestamp")),
        ("updatedAt", v("updated_at")),
        ("schedule", v("0 2 * * *")),
        ("serviceAccount", v("sa@p.iam.gserviceaccount.com")),
    ])
}

#[tokio::test]
async fn check_accepts_a_valid_snapshot() {
    let c = MockSchedulerClient::new();
    let out = check_snapshot(
        &c,
        pulumirpc::CheckRequest {
            news: Some(inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("check")
    .into_inner();
    assert!(out.failures.is_empty(), "{:?}", out.failures);
}

/// `timestamp` is the only strategy implemented. Accepting another name would
/// produce a workflow that runs and quietly does the wrong thing.
#[tokio::test]
async fn check_rejects_an_unsupported_strategy() {
    let c = MockSchedulerClient::new();
    let mut bad = inputs();
    bad.fields.insert("strategy".to_owned(), v("check"));
    let out = check_snapshot(
        &c,
        pulumirpc::CheckRequest {
            news: Some(bad),
            ..Default::default()
        },
    )
    .await
    .expect("check")
    .into_inner();
    assert!(
        !out.failures.is_empty(),
        "an unsupported strategy was accepted"
    );
}

#[tokio::test]
async fn check_rejects_a_snapshot_with_no_unique_key() {
    let c = MockSchedulerClient::new();
    let mut bad = inputs();
    bad.fields.insert("uniqueKey".to_owned(), v(""));
    let out = check_snapshot(
        &c,
        pulumirpc::CheckRequest {
            news: Some(bad),
            ..Default::default()
        },
    )
    .await
    .expect("check")
    .into_inner();
    assert!(
        !out.failures.is_empty(),
        "a snapshot with nothing to match rows on was accepted"
    );
}

#[tokio::test]
async fn create_read_update_delete() {
    let c = MockSchedulerClient::new();

    let created = create_snapshot(
        &c,
        pulumirpc::CreateRequest {
            properties: Some(inputs()),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create")
    .into_inner();
    assert!(!created.id.is_empty());

    // Both halves of the pair must have been created, not just one: a workflow
    // with no schedule never runs, and a schedule with no workflow fails when
    // it fires.
    let wf = c.workflow_log.lock().unwrap().clone();
    let jobs = c.scheduler_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(op, _, _)| op == "create"),
        "no workflow was created: {wf:?}"
    );
    assert!(
        jobs.iter().any(|(op, _)| op == "create"),
        "no scheduler job was created: {jobs:?}"
    );

    read_snapshot(
        &c,
        pulumirpc::ReadRequest {
            id: created.id.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("read");

    let mut changed = inputs();
    changed.fields.insert("schedule".to_owned(), v("0 3 * * *"));
    update_snapshot(
        &c,
        pulumirpc::UpdateRequest {
            id: created.id.clone(),
            olds: Some(inputs()),
            news: Some(changed),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    delete_snapshot(
        &c,
        pulumirpc::DeleteRequest {
            id: created.id,
            properties: Some(inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("delete");

    // Teardown must remove both halves; leaving the scheduler job behind means
    // a job that fires against a workflow that no longer exists.
    let wf = c.workflow_log.lock().unwrap().clone();
    let jobs = c.scheduler_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(op, _, _)| op == "delete"),
        "the workflow was not deleted: {wf:?}"
    );
    assert!(
        jobs.iter().any(|(op, _)| op == "delete"),
        "the scheduler job was not deleted: {jobs:?}"
    );
}

#[tokio::test]
async fn preview_creates_nothing() {
    let c = MockSchedulerClient::new();
    create_snapshot(
        &c,
        pulumirpc::CreateRequest {
            properties: Some(inputs()),
            preview: true,
            ..Default::default()
        },
    )
    .await
    .expect("preview");
    assert!(
        c.workflow_log.lock().unwrap().is_empty(),
        "preview created a workflow"
    );
}

#[tokio::test]
async fn diff_settles_and_notices_a_schedule_change() {
    let c = MockSchedulerClient::new();
    let same = diff_snapshot(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(inputs()),
            old_inputs: Some(inputs()),
            news: Some(inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();
    assert_eq!(
        same.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32,
        "an unchanged snapshot reported a diff"
    );

    let mut changed = inputs();
    changed.fields.insert("schedule".to_owned(), v("0 5 * * *"));
    let diff = diff_snapshot(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(inputs()),
            old_inputs: Some(inputs()),
            news: Some(changed),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();
    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

/// Changing the unique key changes what a row *is*, so the existing history
/// cannot be reconciled with the new one — it has to be a replace, not an
/// update that silently starts matching rows differently.
#[tokio::test]
async fn changing_the_unique_key_forces_a_replace() {
    let c = MockSchedulerClient::new();
    let mut changed = inputs();
    changed
        .fields
        .insert("uniqueKey".to_owned(), v("customer_id"));
    let diff = diff_snapshot(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(inputs()),
            old_inputs: Some(inputs()),
            news: Some(changed),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();
    assert!(
        !diff.replaces.is_empty(),
        "changing the unique key was treated as an in-place update: {diff:?}"
    );
}
