// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Agent handler behaviour, end to end against a recording double.
//!
//! These assert on *what the handler sent*, not only on what it returned. For
//! these resources the request body is the product: whether a context lands in
//! staging or published, whether an IAM write carries the etag it read, whether
//! a conflict adopts or fails. Each is a decision a deploy depends on and none
//! is visible from a return value.

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

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value {
        kind: Some(Kind::StructValue(Struct {
            fields: pairs.into_iter().map(|(k, x)| (k.to_owned(), x)).collect(),
        })),
    }
}

fn props(pairs: Vec<(&str, Value)>) -> Struct {
    Struct {
        fields: pairs.into_iter().map(|(k, x)| (k.to_owned(), x)).collect(),
    }
}

/// A minimal valid agent grounded on one dbt model output.
fn agent_props(extra: Vec<(&str, Value)>) -> Struct {
    let mut pairs = vec![
        ("project", v("p")),
        ("location", v("global")),
        ("agentId", v("revenue-analyst")),
        ("systemInstruction", v("You are a revenue analyst.")),
        (
            "models",
            list(vec![obj(vec![
                ("materialization", v("table")),
                ("tableRef", v("`p.d.mart_revenue`")),
            ])]),
        ),
    ];
    pairs.extend(extra);
    props(pairs)
}

#[tokio::test]
async fn create_without_publish_writes_staging_only() {
    let client = MockAgentClient::new();
    let resp = create_data_agent(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(agent_props(vec![])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create");

    let body = client.body_for("create_data_agent").expect("body recorded");
    assert!(body.pointer("/dataAnalyticsAgent/stagingContext").is_some());
    assert!(
        body.pointer("/dataAnalyticsAgent/publishedContext")
            .is_none(),
        "an unpublished context must not go live"
    );
    assert_eq!(resp.into_inner().id, "p/global/revenue-analyst");
}

#[tokio::test]
async fn create_with_publish_writes_both_contexts() {
    let client = MockAgentClient::new();
    create_data_agent(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(agent_props(vec![("publish", b(true))])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create");

    let body = client.body_for("create_data_agent").unwrap();
    assert_eq!(
        body.pointer("/dataAnalyticsAgent/stagingContext"),
        body.pointer("/dataAnalyticsAgent/publishedContext"),
        "staging must mirror what is live"
    );
}

#[tokio::test]
async fn the_dbt_model_output_becomes_the_grounding_table() {
    // The integration that justifies this resource living in this provider:
    // the agent is grounded on a model the stack declares.
    let client = MockAgentClient::new();
    create_data_agent(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(agent_props(vec![("publish", b(true))])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let body = client.body_for("create_data_agent").unwrap();
    let table = body
        .pointer("/dataAnalyticsAgent/publishedContext/datasourceReferences/bq/tableReferences/0")
        .expect("a table reference");
    assert_eq!(table["projectId"], "p");
    assert_eq!(table["datasetId"], "d");
    assert_eq!(table["tableId"], "mart_revenue");
}

#[tokio::test]
async fn preview_sends_nothing() {
    // A preview that mutates is not a preview.
    let client = MockAgentClient::new();
    create_data_agent(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(agent_props(vec![])),
            preview: true,
            ..Default::default()
        },
    )
    .await
    .expect("preview create");
    assert!(client.ops().is_empty(), "preview must not call the API");
}

#[tokio::test]
async fn an_existing_agent_is_adopted_rather_than_failing() {
    // A 409 means someone already created it — usually a previous partial
    // deploy. Failing here would leave the stack permanently unable to proceed.
    let client = MockAgentClient::failing("create_data_agent", "409 conflict");
    create_data_agent(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(agent_props(vec![])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("conflict should adopt");

    assert_eq!(
        client.ops(),
        vec!["create_data_agent", "update_data_agent"],
        "a conflict should fall through to an update"
    );
}

#[tokio::test]
async fn update_sends_a_mask_that_covers_the_context() {
    // Without the context path the patch would report success while leaving
    // the agent's behaviour unchanged.
    let client = MockAgentClient::new();
    update_data_agent(
        &client,
        pulumirpc::UpdateRequest {
            id: "p/global/revenue-analyst".into(),
            news: Some(agent_props(vec![("displayName", v("Analyst"))])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    let body = client.body_for("update_data_agent").unwrap();
    let mask = body["__updateMask"].as_str().unwrap();
    assert!(mask.contains("dataAnalyticsAgent"), "mask was {mask}");
    assert!(mask.contains("displayName"));
}

#[tokio::test]
async fn read_reports_a_missing_agent_as_gone() {
    // An empty id tells the engine to recreate the resource. Returning an error
    // instead would wedge the stack on a resource deleted out of band.
    let client = MockAgentClient::failing("get_data_agent", "404 not found");
    let resp = read_data_agent(
        &client,
        pulumirpc::ReadRequest {
            id: "p/global/gone".into(),
            ..Default::default()
        },
    )
    .await
    .expect("a missing agent is not an error");
    assert!(resp.into_inner().id.is_empty());
}

#[tokio::test]
async fn iam_write_is_a_read_merge_write_under_the_etag() {
    // Additive is the default precisely so a grant made elsewhere survives.
    let client = MockAgentClient::with_policy(
        vec![(
            "roles/viewer".into(),
            vec!["user:existing@example.com".into()],
        )],
        "etag-from-read",
    );

    create_agent_iam_policy(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("global")),
                ("agentId", v("revenue-analyst")),
                (
                    "bindings",
                    list(vec![obj(vec![
                        ("role", v("roles/geminidataanalytics.dataAgentUser")),
                        ("members", list(vec![v("user:new@example.com")])),
                    ])]),
                ),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create iam policy");

    assert_eq!(
        client.ops(),
        vec!["get_agent_iam_policy", "set_agent_iam_policy"],
        "the policy must be read before it is written"
    );

    let body = client.body_for("set_agent_iam_policy").unwrap();
    assert_eq!(
        body["policy"]["etag"], "etag-from-read",
        "the write must carry the etag it read, or it is not compare-and-swap"
    );

    let roles: Vec<&str> = body["policy"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["role"].as_str().unwrap())
        .collect();
    assert!(
        roles.contains(&"roles/viewer"),
        "a binding this stack does not manage was revoked: {roles:?}"
    );
    assert!(roles.contains(&"roles/geminidataanalytics.dataAgentUser"));
}

#[tokio::test]
async fn an_authoritative_policy_replaces_rather_than_merges() {
    // The opposite behaviour, and the reason it is not the default.
    let client = MockAgentClient::with_policy(
        vec![(
            "roles/viewer".into(),
            vec!["user:existing@example.com".into()],
        )],
        "etag-from-read",
    );

    create_agent_iam_policy(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("global")),
                ("agentId", v("a")),
                ("authoritative", b(true)),
                (
                    "bindings",
                    list(vec![obj(vec![
                        ("role", v("roles/geminidataanalytics.dataAgentUser")),
                        ("members", list(vec![v("user:new@example.com")])),
                    ])]),
                ),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let body = client.body_for("set_agent_iam_policy").unwrap();
    let roles: Vec<&str> = body["policy"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, vec!["roles/geminidataanalytics.dataAgentUser"]);
}

#[tokio::test]
async fn deleting_the_iam_resource_reads_before_it_clears() {
    // Writing an empty policy unconditionally would revoke everyone's access,
    // including grants this stack never made.
    let client = MockAgentClient::with_policy(vec![], "etag-from-read");
    delete_agent_iam_policy(
        &client,
        pulumirpc::DeleteRequest {
            id: "p/global/a/iam".into(),
            ..Default::default()
        },
    )
    .await
    .expect("delete iam policy");

    assert_eq!(
        client.ops(),
        vec!["get_agent_iam_policy", "set_agent_iam_policy"]
    );
    assert_eq!(
        client.body_for("set_agent_iam_policy").unwrap()["policy"]["etag"],
        "etag-from-read"
    );
}

#[tokio::test]
async fn agent_engine_id_is_read_back_not_assumed() {
    // The service assigns it, so the handler must take the name it is given.
    let client = MockAgentClient::new();
    let resp = create_agent_engine(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("us-central1")),
                ("displayName", v("my-agent")),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create engine");
    assert_eq!(resp.into_inner().id, "p/us-central1/generated-id");
}

#[tokio::test]
async fn engine_secrets_are_sent_as_references() {
    // A secret value in the body would be written into Pulumi state as plain
    // text and shown in every subsequent diff.
    let client = MockAgentClient::new();
    create_agent_engine(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("us-central1")),
                ("displayName", v("my-agent")),
                (
                    "secretEnv",
                    obj(vec![("API_KEY", v("projects/p/secrets/api-key"))]),
                ),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let body = client.body_for("create_agent_engine").unwrap();
    let secret = &body["spec"]["deploymentSpec"]["secretEnv"][0];
    assert_eq!(secret["secretRef"]["secret"], "projects/p/secrets/api-key");
    assert!(secret.get("value").is_none());
    assert!(
        !serde_json::to_string(&body).unwrap().contains("\"value\""),
        "no literal secret value may appear anywhere in the body"
    );
}

#[tokio::test]
async fn conversation_create_carries_its_agents() {
    let client = MockAgentClient::new();
    create_conversation(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("global")),
                ("conversationId", v("c1")),
                (
                    "agents",
                    list(vec![v("projects/p/locations/global/dataAgents/a")]),
                ),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create conversation");

    let body = client.body_for("create_conversation").unwrap();
    assert_eq!(
        body["agents"][0],
        "projects/p/locations/global/dataAgents/a"
    );
}

#[tokio::test]
async fn memory_create_sends_the_fact_and_scope() {
    let client = MockAgentClient::new();
    create_memory(
        &client,
        pulumirpc::CreateRequest {
            properties: Some(props(vec![
                ("project", v("p")),
                ("location", v("us-central1")),
                ("engineId", v("e1")),
                ("fact", v("the user prefers metric units")),
                ("scope", obj(vec![("user_id", v("u1"))])),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("create memory");

    let body = client.body_for("create_memory").unwrap();
    assert_eq!(body["fact"], "the user prefers metric units");
    assert_eq!(body["scope"]["user_id"], "u1");
}

#[tokio::test]
async fn check_rejects_an_agent_with_no_datasources() {
    let client = MockAgentClient::new();
    let resp = check_data_agent(
        &client,
        pulumirpc::CheckRequest {
            news: Some(props(vec![
                ("project", v("p")),
                ("agentId", v("a")),
                ("systemInstruction", v("You are an analyst.")),
            ])),
            ..Default::default()
        },
    )
    .await
    .expect("check");

    let failures = resp.into_inner().failures;
    assert!(!failures.is_empty());
    assert!(failures.iter().any(|f| f.reason.contains("dbt model")));
    assert!(client.ops().is_empty(), "check must not call the API");
}
