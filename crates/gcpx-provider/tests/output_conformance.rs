// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Does a resource actually return the outputs its schema promises?
//!
//! The schema is the contract a stack is written against. `${agent.published}`
//! or `${policy.bindings}` is only meaningful if the handler stores it, and
//! nothing checks that automatically: a missing output is not a compile error,
//! not a runtime error, and not a failed deploy. It resolves to nothing and the
//! stack quietly wires up wrong.
//!
//! This drives the real create handlers against the recording doubles with every
//! declared input supplied, then holds the result against the schema. Fields the
//! service only issues later are exempt, and named individually rather than
//! waved through as a class.

use std::collections::BTreeSet;

use gcpx_agents::handlers::*;
use gcpx_agents::mock::MockAgentClient;
use prost_types::{value::Kind, ListValue, Struct, Value};
use pulumi_rs_yaml_proto::pulumirpc;
use serde_json::Value as Json;

fn schema() -> Json {
    serde_json::from_str(gcpx_provider::schema::schema_json()).expect("schema must parse")
}

fn declared_outputs(token: &str) -> BTreeSet<String> {
    schema()["resources"][token]["properties"]
        .as_object()
        .unwrap_or_else(|| panic!("{token} has no properties in the schema"))
        .keys()
        .cloned()
        .collect()
}

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

async fn create(which: &str, client: &MockAgentClient, inputs: Struct) -> Struct {
    let req = pulumirpc::CreateRequest {
        properties: Some(inputs),
        preview: false,
        ..Default::default()
    };
    let resp = match which {
        "agent" => create_data_agent(client, req).await,
        "iam" => create_agent_iam_policy(client, req).await,
        "conversation" => create_conversation(client, req).await,
        "memory" => create_memory(client, req).await,
        other => panic!("unknown resource {other}"),
    };
    resp.unwrap_or_else(|e| panic!("{which}: create failed: {e}"))
        .into_inner()
        .properties
        .unwrap_or_else(|| panic!("{which}: create returned no properties"))
}

/// Asserts every declared output is present, except those explicitly exempted.
fn assert_outputs_match_schema(token: &str, got: &Struct, exempt: &[&str]) {
    let declared = declared_outputs(token);
    let present: BTreeSet<String> = got.fields.keys().cloned().collect();
    let exempt: BTreeSet<String> = exempt.iter().map(|s| (*s).to_owned()).collect();

    let missing: Vec<&String> = declared
        .difference(&present)
        .filter(|k| !exempt.contains(*k))
        .collect();
    assert!(
        missing.is_empty(),
        "{token}: the schema promises {missing:?} but create did not store them — \
         a stack referencing those gets nothing"
    );

    // The inverse is worth knowing too: an output nobody declared cannot be
    // referenced from a stack, so storing it is dead weight at best and a sign
    // the schema and the handler have drifted at worst.
    let undeclared: Vec<&String> = present.difference(&declared).collect();
    assert!(
        undeclared.is_empty(),
        "{token}: create stored {undeclared:?}, which the schema does not declare"
    );
}

#[tokio::test]
async fn data_agent_returns_what_the_schema_promises() {
    let out = create(
        "agent",
        &MockAgentClient::new(),
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("agentId", v("a")),
            ("displayName", v("Agent")),
            ("description", v("desc")),
            ("systemInstruction", v("be helpful")),
            ("tables", list(vec![v("p.d.t")])),
            ("publish", b(true)),
            ("chartRendering", b(true)),
            ("pythonAnalysis", b(false)),
            (
                "kmsKey",
                v("projects/p/locations/global/keyRings/r/cryptoKeys/k"),
            ),
            (
                "labels",
                Value {
                    kind: Some(Kind::StructValue(st(vec![("team", v("data"))]))),
                },
            ),
            ("model", v("gemini-2.0-flash")),
            (
                "exampleQueries",
                list(vec![Value {
                    kind: Some(Kind::StructValue(st(vec![
                        ("question", v("how many outages?")),
                        ("sql", v("SELECT COUNT(*) FROM t")),
                    ]))),
                }]),
            ),
            (
                "glossaryTerms",
                list(vec![Value {
                    kind: Some(Kind::StructValue(st(vec![
                        ("displayName", v("outage")),
                        ("description", v("an unplanned service interruption")),
                    ]))),
                }]),
            ),
            (
                "models",
                list(vec![Value {
                    kind: Some(Kind::StructValue(st(vec![
                        ("tableRef", v("`p.d.stg_outages`")),
                        ("materialization", v("table")),
                    ]))),
                }]),
            ),
        ]),
    )
    .await;
    // `lookerExplores` is mutually exclusive with tables/models — the API
    // grounds an agent on one kind of source or the other, never both.
    assert_outputs_match_schema("gcpx:agent/dataAgent:DataAgent", &out, &["lookerExplores"]);
}

#[tokio::test]
async fn iam_policy_returns_what_the_schema_promises() {
    let out = create(
        "iam",
        &MockAgentClient::new(),
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("agentId", v("a")),
            ("authoritative", b(true)),
            (
                "bindings",
                list(vec![Value {
                    kind: Some(Kind::StructValue(st(vec![
                        ("role", v("roles/geminidataanalytics.dataAgentUser")),
                        ("members", list(vec![v("user:someone@example.com")])),
                    ]))),
                }]),
            ),
        ]),
    )
    .await;
    assert_outputs_match_schema(
        "gcpx:agent/dataAgentIamPolicy:DataAgentIamPolicy",
        &out,
        &[],
    );
}

#[tokio::test]
async fn conversation_returns_what_the_schema_promises() {
    let out = create(
        "conversation",
        &MockAgentClient::new(),
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("conversationId", v("c")),
            ("agents", list(vec![v("a")])),
        ]),
    )
    .await;
    // `lastUsedTime` exists only once the conversation has been used, and
    // `labels` only when the caller set some.
    assert_outputs_match_schema(
        "gcpx:agent/conversation:Conversation",
        &out,
        &["lastUsedTime", "labels"],
    );
}

#[tokio::test]
async fn memory_returns_what_the_schema_promises() {
    let out = create(
        "memory",
        &MockAgentClient::new(),
        st(vec![
            ("project", v("p")),
            ("location", v("us-central1")),
            ("engineId", v("e")),
            ("fact", v("prefers metric units")),
            ("scope", v("user/123")),
            ("displayName", v("units")),
            ("description", v("a preference")),
        ]),
    )
    .await;
    assert_outputs_match_schema("gcpx:agent/memory:Memory", &out, &[]);
}
