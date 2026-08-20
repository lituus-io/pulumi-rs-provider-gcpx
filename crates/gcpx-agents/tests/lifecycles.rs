// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Full lifecycles for the agent resources, against the recording double.
//!
//! The existing suites concentrate on create and on what gets sent — which is
//! where the interesting decisions are — leaving read, update and delete for
//! most of these resources exercised only by the live tests, which need a real
//! project and do not run in CI. This closes that: every resource is driven
//! through Check, Create, Read, Update and Delete, plus the paths that only
//! appear when something goes wrong.

use gcpx_agents::handlers::*;
use gcpx_agents::mock::MockAgentClient;
use prost_types::{value::Kind, ListValue, Struct, Value};
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
fn list(items: Vec<Value>) -> Value {
    Value {
        kind: Some(Kind::ListValue(ListValue { values: items })),
    }
}
fn st(pairs: Vec<(&str, Value)>) -> Struct {
    Struct {
        fields: pairs.into_iter().map(|(k, x)| (k.to_owned(), x)).collect(),
    }
}
fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value {
        kind: Some(Kind::StructValue(st(pairs))),
    }
}

fn check(news: Struct) -> pulumirpc::CheckRequest {
    pulumirpc::CheckRequest {
        news: Some(news),
        ..Default::default()
    }
}
fn create(props: Struct) -> pulumirpc::CreateRequest {
    pulumirpc::CreateRequest {
        properties: Some(props),
        preview: false,
        ..Default::default()
    }
}
fn read(id: &str) -> pulumirpc::ReadRequest {
    pulumirpc::ReadRequest {
        id: id.to_owned(),
        ..Default::default()
    }
}
fn update(id: &str, olds: Struct, news: Struct) -> pulumirpc::UpdateRequest {
    pulumirpc::UpdateRequest {
        id: id.to_owned(),
        olds: Some(olds),
        news: Some(news),
        preview: false,
        ..Default::default()
    }
}
fn delete(id: &str, props: Struct) -> pulumirpc::DeleteRequest {
    pulumirpc::DeleteRequest {
        id: id.to_owned(),
        properties: Some(props),
        ..Default::default()
    }
}

// ── Conversation ────────────────────────────────────────────────────────────

fn conversation() -> Struct {
    st(vec![
        ("project", v("p")),
        ("location", v("global")),
        ("conversationId", v("c")),
        ("agents", list(vec![v("a")])),
    ])
}

#[tokio::test]
async fn conversation_check_create_read_update_delete() {
    let c = MockAgentClient::new();

    let checked = check_conversation(&c, check(conversation()))
        .await
        .expect("check")
        .into_inner();
    assert!(checked.failures.is_empty(), "{:?}", checked.failures);

    let created = create_conversation(&c, create(conversation()))
        .await
        .expect("create")
        .into_inner();
    assert!(!created.id.is_empty());

    read_conversation(&c, read(&created.id))
        .await
        .expect("read");

    // The API has no update for a conversation; the handler must still answer
    // the RPC rather than panic, because the engine may call it after a diff
    // that resolved to nothing.
    let _ = update_conversation(&c, update(&created.id, conversation(), conversation())).await;

    delete_conversation(&c, delete(&created.id, conversation()))
        .await
        .expect("delete");
}

#[tokio::test]
async fn conversation_check_rejects_one_with_no_agents() {
    let c = MockAgentClient::new();
    let mut bad = conversation();
    bad.fields.insert("agents".to_owned(), list(vec![]));
    let out = check_conversation(&c, check(bad))
        .await
        .expect("check")
        .into_inner();
    assert!(
        !out.failures.is_empty(),
        "a conversation grounded on no agents was accepted"
    );
}

// ── IAM policy ──────────────────────────────────────────────────────────────

fn policy() -> Struct {
    st(vec![
        ("project", v("p")),
        ("location", v("global")),
        ("agentId", v("a")),
        (
            "bindings",
            list(vec![obj(vec![
                ("role", v("roles/geminidataanalytics.dataAgentUser")),
                ("members", list(vec![v("user:someone@example.com")])),
            ])]),
        ),
    ])
}

#[tokio::test]
async fn iam_policy_check_create_read_update_delete() {
    let c = MockAgentClient::new();

    let checked = check_agent_iam_policy(&c, check(policy()))
        .await
        .expect("check")
        .into_inner();
    assert!(checked.failures.is_empty(), "{:?}", checked.failures);

    let created = create_agent_iam_policy(&c, create(policy()))
        .await
        .expect("create")
        .into_inner();

    read_agent_iam_policy(&c, read(&created.id))
        .await
        .expect("read");

    let mut changed = policy();
    changed.fields.insert(
        "bindings".to_owned(),
        list(vec![obj(vec![
            ("role", v("roles/geminidataanalytics.dataAgentUser")),
            ("members", list(vec![v("user:other@example.com")])),
        ])]),
    );
    update_agent_iam_policy(&c, update(&created.id, policy(), changed))
        .await
        .expect("update");

    delete_agent_iam_policy(&c, delete(&created.id, policy()))
        .await
        .expect("delete");
}

/// A binding with no role names no permission to grant.
#[tokio::test]
async fn iam_check_rejects_a_binding_without_a_role() {
    let c = MockAgentClient::new();
    let mut bad = policy();
    bad.fields.insert(
        "bindings".to_owned(),
        list(vec![obj(vec![
            ("role", v("")),
            ("members", list(vec![v("user:someone@example.com")])),
        ])]),
    );
    let out = check_agent_iam_policy(&c, check(bad))
        .await
        .expect("check")
        .into_inner();
    assert!(
        !out.failures.is_empty(),
        "a binding with no role was accepted"
    );
}

// ── Agent engine ────────────────────────────────────────────────────────────

fn engine() -> Struct {
    st(vec![
        ("project", v("p")),
        ("location", v("us-central1")),
        ("displayName", v("engine")),
        ("description", v("an engine")),
        ("pickleUri", v("gs://b/agent.pkl")),
        ("requirementsUri", v("gs://b/requirements.txt")),
        ("pythonVersion", v("3.12")),
    ])
}

#[tokio::test]
async fn agent_engine_check_create_read_update_delete() {
    let c = MockAgentClient::new();

    let checked = check_agent_engine(&c, check(engine()))
        .await
        .expect("check")
        .into_inner();
    assert!(checked.failures.is_empty(), "{:?}", checked.failures);

    let created = create_agent_engine(&c, create(engine()))
        .await
        .expect("create")
        .into_inner();

    read_agent_engine(&c, read(&created.id))
        .await
        .expect("read");

    let mut changed = engine();
    changed
        .fields
        .insert("displayName".to_owned(), v("renamed"));
    update_agent_engine(&c, update(&created.id, engine(), changed.clone()))
        .await
        .expect("update");

    let diff = diff_agent_engine(
        &c,
        pulumirpc::DiffRequest {
            olds: Some(engine()),
            old_inputs: Some(engine()),
            news: Some(changed),
            ..Default::default()
        },
    )
    .await
    .expect("diff")
    .into_inner();
    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32,
        "renaming an engine was reported as no change"
    );

    delete_agent_engine(&c, delete(&created.id, engine()))
        .await
        .expect("delete");
}

// ── Memory ──────────────────────────────────────────────────────────────────

fn memory() -> Struct {
    st(vec![
        ("project", v("p")),
        ("location", v("us-central1")),
        ("engineId", v("e")),
        ("fact", v("prefers metric units")),
        ("scope", v("user/123")),
        ("displayName", v("units")),
    ])
}

#[tokio::test]
async fn memory_check_create_read_update_delete() {
    let c = MockAgentClient::new();

    let checked = check_memory(&c, check(memory()))
        .await
        .expect("check")
        .into_inner();
    assert!(checked.failures.is_empty(), "{:?}", checked.failures);

    let created = create_memory(&c, create(memory()))
        .await
        .expect("create")
        .into_inner();

    read_memory(&c, read(&created.id)).await.expect("read");

    let mut changed = memory();
    changed
        .fields
        .insert("fact".to_owned(), v("prefers imperial units"));
    update_memory(&c, update(&created.id, memory(), changed))
        .await
        .expect("update");

    delete_memory(&c, delete(&created.id, memory()))
        .await
        .expect("delete");
}

/// A memory with no fact stores nothing.
#[tokio::test]
async fn memory_check_rejects_an_empty_fact() {
    let c = MockAgentClient::new();
    let mut bad = memory();
    bad.fields.insert("fact".to_owned(), v(""));
    let out = check_memory(&c, check(bad))
        .await
        .expect("check")
        .into_inner();
    assert!(!out.failures.is_empty(), "an empty fact was accepted");
}

// ── Data agent: the paths the other suites do not reach ─────────────────────

fn agent() -> Struct {
    st(vec![
        ("project", v("p")),
        ("location", v("global")),
        ("agentId", v("a")),
        ("displayName", v("Agent")),
        ("systemInstruction", v("be helpful")),
        ("tables", list(vec![v("p.d.t")])),
        ("publish", b(true)),
    ])
}

/// Pins current behaviour, which is worth a second look.
///
/// Deleting an agent that is already gone fails rather than succeeding. Every
/// delete in this provider behaves this way — `verified_delete` propagates the
/// delete error too, and only treats a 404 from the *poll* as confirmation.
///
/// The cost is that an agent removed out of band makes `pulumi destroy` fail,
/// and the stack cannot be torn down without editing state by hand. The
/// argument for keeping it is that a silent success hides a resource someone
/// else deleted. Refresh is the usual answer to that, which points the other
/// way — but changing it is a decision about destroy semantics across every
/// resource, not something to slip into a test.
///
/// This asserts what happens today so a change is deliberate rather than
/// accidental, in either direction.
#[tokio::test]
async fn deleting_an_agent_that_is_already_gone_currently_fails() {
    let c = MockAgentClient::failing("delete_data_agent", "404 not found");
    let out = delete_data_agent(&c, delete("p/global/a", agent())).await;
    assert!(
        out.is_err(),
        "delete now tolerates a missing agent — if that was deliberate, this \
         test and the note above should go"
    );
}

/// A failure that is not "already gone" must not be swallowed — otherwise a
/// destroy reports success while the agent is still serving.
#[tokio::test]
async fn a_real_delete_failure_is_reported() {
    let c = MockAgentClient::failing("delete_data_agent", "403 permission denied");
    let out = delete_data_agent(&c, delete("p/global/a", agent())).await;
    assert!(out.is_err(), "a permission failure was reported as success");
}

#[tokio::test]
async fn creating_an_agent_that_already_exists_adopts_it() {
    let c = MockAgentClient::failing("create_data_agent", "409 conflict");
    let out = create_data_agent(&c, create(agent())).await;
    assert!(
        out.is_ok(),
        "an existing agent was not adopted on 409: {:?}",
        out.err()
    );
}
