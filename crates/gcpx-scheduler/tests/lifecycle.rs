// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Scheduled SQL, end to end against the client double.
//!
//! `SqlJob` is the pair every recurring resource is built on: a Cloud Workflow
//! holding the query and a Cloud Scheduler job deciding when it runs. Both
//! halves have to be created together and torn down together — a workflow with
//! no schedule never runs, and a schedule pointing at a workflow that no longer
//! exists fails when it fires, at 2am, silently.

use gcpx_scheduler::handlers::*;
use gcpx_scheduler::mock::MockSchedulerClient;
use prost_types::{value::Kind, Struct, Value};
use pulumi_rs_yaml_proto::pulumirpc;

fn v(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_owned())),
    }
}
fn b(x: bool) -> Value {
    Value {
        kind: Some(Kind::BoolValue(x)),
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
        ("name", v("nightly-rollup")),
        ("sql", v("SELECT 1")),
        ("schedule", v("0 2 * * *")),
        ("timeZone", v("UTC")),
        ("serviceAccount", v("sa@p.iam.gserviceaccount.com")),
    ])
}

fn check_req(s: Struct) -> pulumirpc::CheckRequest {
    pulumirpc::CheckRequest {
        news: Some(s),
        ..Default::default()
    }
}

#[tokio::test]
async fn check_accepts_a_valid_job_and_names_what_is_missing() {
    let c = MockSchedulerClient::new();
    let ok = check_sql_job(&c, check_req(inputs()))
        .await
        .expect("check")
        .into_inner();
    assert!(ok.failures.is_empty(), "{:?}", ok.failures);

    for missing in [
        "project",
        "region",
        "name",
        "sql",
        "schedule",
        "serviceAccount",
    ] {
        let mut bad = inputs();
        bad.fields.insert(missing.to_owned(), v(""));
        let out = check_sql_job(&c, check_req(bad))
            .await
            .expect("check")
            .into_inner();
        assert!(
            out.failures.iter().any(|f| f.property == missing),
            "an empty {missing} was accepted, or reported against the wrong property: {:?}",
            out.failures
        );
    }
}

#[tokio::test]
async fn create_makes_both_halves_and_delete_removes_both() {
    let c = MockSchedulerClient::new();

    let created = create_sql_job(
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

    let wf = c.workflow_log.lock().unwrap().clone();
    let sched = c.scheduler_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(op, _, _)| op == "create"),
        "no workflow created: {wf:?}"
    );
    assert!(
        sched.iter().any(|(op, _)| op == "create"),
        "no scheduler job created: {sched:?}"
    );

    // The workflow definition must actually carry the query — a pair that is
    // wired up but runs the wrong SQL is worse than one that fails.
    assert!(
        wf.iter().any(|(_, _, def)| def.contains("SELECT 1")),
        "the workflow does not contain the query it was given: {wf:?}"
    );

    read_sql_job(
        &c,
        pulumirpc::ReadRequest {
            id: created.id.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("read");

    delete_sql_job(
        &c,
        pulumirpc::DeleteRequest {
            id: created.id,
            properties: Some(inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("delete");

    let wf = c.workflow_log.lock().unwrap().clone();
    let sched = c.scheduler_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(op, _, _)| op == "delete"),
        "the workflow outlived the resource: {wf:?}"
    );
    assert!(
        sched.iter().any(|(op, _)| op == "delete"),
        "the scheduler job outlived the resource: {sched:?}"
    );
}

#[tokio::test]
async fn updating_the_query_rewrites_the_workflow() {
    let c = MockSchedulerClient::new();
    let mut changed = inputs();
    changed.fields.insert("sql".to_owned(), v("SELECT 2"));

    update_sql_job(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/us-central1/nightly-rollup".into(),
            olds: Some(inputs()),
            news: Some(changed),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    let wf = c.workflow_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(_, _, def)| def.contains("SELECT 2")),
        "the workflow still runs the old query: {wf:?}"
    );
}

#[tokio::test]
async fn pausing_is_an_update_not_a_replace() {
    let c = MockSchedulerClient::new();
    let mut paused = inputs();
    paused.fields.insert("paused".to_owned(), b(true));

    let diff = diff_sql_job(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(inputs()),
            old_inputs: Some(inputs()),
            news: Some(paused),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();

    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32,
        "pausing a job was reported as no change"
    );
    assert!(
        diff.replaces.is_empty(),
        "pausing a job tore it down and rebuilt it: {:?}",
        diff.replaces
    );
}

/// Region and name are baked into the resource names of both halves, so they
/// cannot be edited in place.
#[tokio::test]
async fn moving_a_job_forces_a_replace() {
    let c = MockSchedulerClient::new();
    let mut moved = inputs();
    moved.fields.insert("region".to_owned(), v("europe-west1"));

    let diff = diff_sql_job(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(inputs()),
            old_inputs: Some(inputs()),
            news: Some(moved),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();
    assert!(
        !diff.replaces.is_empty(),
        "changing region was treated as an in-place update"
    );
}

#[tokio::test]
async fn an_unchanged_job_reports_no_diff() {
    let c = MockSchedulerClient::new();
    let out = diff_sql_job(
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
        out.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );
}

#[tokio::test]
async fn preview_creates_neither_half() {
    let c = MockSchedulerClient::new();
    create_sql_job(
        &c,
        pulumirpc::CreateRequest {
            properties: Some(inputs()),
            preview: true,
            ..Default::default()
        },
    )
    .await
    .expect("preview");
    assert!(c.workflow_log.lock().unwrap().is_empty());
    assert!(c.scheduler_log.lock().unwrap().is_empty());
}

/// If the scheduler half fails, the workflow already created must be rolled
/// back — otherwise a failed deploy leaves an orphan behind that nothing owns
/// and the next attempt collides with.
#[tokio::test]
async fn a_failed_scheduler_create_rolls_back_the_workflow() {
    let c = MockSchedulerClient {
        fail_on: std::sync::Mutex::new(Some("create_scheduler_job".to_owned())),
        ..Default::default()
    };
    let out = create_sql_job(
        &c,
        pulumirpc::CreateRequest {
            properties: Some(inputs()),
            preview: false,
            ..Default::default()
        },
    )
    .await;
    assert!(out.is_err(), "a failed scheduler create reported success");

    let wf = c.workflow_log.lock().unwrap().clone();
    assert!(
        wf.iter().any(|(op, _, _)| op == "delete"),
        "the orphaned workflow was not rolled back: {wf:?}"
    );
}
