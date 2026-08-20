// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Deploy, change nothing, preview again: the answer must be "no diff".
//!
//! This is the property a user exercises constantly and the one a provider is
//! most likely to get wrong, because `Diff` compares the *outputs* of the last
//! deploy against the *inputs* of the next one. Every field the service issues —
//! `etag`, `name`, `createTime` — sits on one side of that comparison and can
//! never appear on the other, so a naive comparison reports a change forever.
//!
//! For `Conversation` that mistake was destructive rather than merely noisy: the
//! API has no update, so its diff declares a *replace*, and a conversation would
//! have been torn down and rebuilt on every single deploy.
//!
//! Each case drives the real create handler and feeds back exactly what it
//! stored, because that is where the computed fields are added.

use gcpx_agents::handlers::*;
use gcpx_agents::mock::MockAgentClient;
use prost_types::{value::Kind, ListValue, Struct, Value};
use pulumi_rs_yaml_proto::pulumirpc;

fn v(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_owned())),
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

fn assert_no_diff(resp: pulumirpc::DiffResponse, what: &str) {
    assert_eq!(
        resp.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32,
        "{what}: an unchanged stack reported a diff on {:?} (replaces: {:?})",
        resp.detailed_diff.keys().collect::<Vec<_>>(),
        resp.replaces
    );
    assert!(
        resp.replaces.is_empty(),
        "{what}: an unchanged stack proposed a replace of {:?}",
        resp.replaces
    );
}

/// Runs the sequence the engine actually runs — Check, then Create, then Diff —
/// and requires the last step to report nothing to do.
///
/// Going through Check matters: it is where a provider fills in the defaults it
/// intends to apply, so skipping it compares a defaulted output against an
/// input that was never defaulted and invents a difference that no real deploy
/// would see.
macro_rules! roundtrip {
    ($client:expr, $check:ident, $create:ident, $diff:ident, $inputs:expr, $what:literal) => {{
        let inputs: Struct = $check(
            &$client,
            pulumirpc::CheckRequest {
                news: Some($inputs),
                ..Default::default()
            },
        )
        .await
        .expect(concat!($what, ": check failed"))
        .into_inner()
        .inputs
        .expect(concat!($what, ": check returned no inputs"));

        let created = $create(
            &$client,
            pulumirpc::CreateRequest {
                properties: Some(inputs.clone()),
                preview: false,
                ..Default::default()
            },
        )
        .await
        .expect(concat!($what, ": create failed"))
        .into_inner()
        .properties
        .expect(concat!($what, ": create returned no properties"));

        // Both paths. `old_inputs` is what a current engine sends; its absence
        // is what state written before the field carried, and that fallback is
        // exactly where comparing against outputs used to go wrong.
        for old_inputs in [Some(inputs.clone()), None] {
            let label = if old_inputs.is_some() {
                concat!($what, " (with old_inputs)")
            } else {
                concat!($what, " (falling back to outputs)")
            };
            let resp = $diff(
                &$client,
                pulumirpc::DiffRequest {
                    olds: Some(created.clone()),
                    old_inputs,
                    news: Some(inputs.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect(concat!($what, ": diff failed"))
            .into_inner();
            assert_no_diff(resp, label);
        }
    }};
}

#[tokio::test]
async fn data_agent_reports_no_diff_when_nothing_changed() {
    let client = MockAgentClient::new();
    roundtrip!(
        client,
        check_data_agent,
        create_data_agent,
        diff_data_agent,
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("agentId", v("a")),
            ("displayName", v("Agent")),
            ("systemInstruction", v("be helpful")),
            ("tables", list(vec![v("p.d.t")])),
        ]),
        "data agent"
    );
}

#[tokio::test]
async fn iam_policy_reports_no_diff_when_nothing_changed() {
    let client = MockAgentClient::new();
    roundtrip!(
        client,
        check_agent_iam_policy,
        create_agent_iam_policy,
        diff_agent_iam_policy,
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("agentId", v("a")),
            (
                "bindings",
                list(vec![Value {
                    kind: Some(Kind::StructValue(st(vec![
                        ("role", v("roles/geminidataanalytics.dataAgentUser")),
                        ("members", list(vec![v("user:someone@example.com")])),
                    ]))),
                }])
            ),
        ]),
        "iam policy"
    );
}

/// The destructive one. Its diff declares a replace, so a false positive here
/// deletes and recreates the conversation on every deploy.
#[tokio::test]
async fn conversation_is_not_replaced_when_nothing_changed() {
    let client = MockAgentClient::new();
    roundtrip!(
        client,
        check_conversation,
        create_conversation,
        diff_conversation,
        st(vec![
            ("project", v("p")),
            ("location", v("global")),
            ("conversationId", v("c")),
            ("agents", list(vec![v("a")])),
        ]),
        "conversation"
    );
}

#[tokio::test]
async fn memory_reports_no_diff_when_nothing_changed() {
    let client = MockAgentClient::new();
    roundtrip!(
        client,
        check_memory,
        create_memory,
        diff_memory,
        st(vec![
            ("project", v("p")),
            ("location", v("us-central1")),
            ("engineId", v("e")),
            ("fact", v("the user prefers metric units")),
            ("scope", v("user/123")),
        ]),
        "memory"
    );
}

/// The inverse failure, which is quieter and worse: a real edit that the
/// provider reports as nothing to do, leaving the deployed engine running the
/// configuration the user just replaced.
#[tokio::test]
async fn changing_any_declared_input_is_noticed() {
    let client = MockAgentClient::new();
    let base = st(vec![
        ("project", v("p")),
        ("location", v("us-central1")),
        ("engineId", v("e")),
        ("fact", v("original")),
        ("scope", v("user/123")),
        ("displayName", v("before")),
        ("description", v("before")),
    ]);

    for key in ["fact", "displayName", "description", "scope"] {
        let mut news = base.clone();
        news.fields.insert(key.to_owned(), v("after"));
        let resp = diff_memory(
            &client,
            pulumirpc::DiffRequest {
                olds: Some(base.clone()),
                old_inputs: Some(base.clone()),
                news: Some(news),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            resp.changes,
            pulumirpc::diff_response::DiffChanges::DiffSome as i32,
            "editing {key} was reported as no change"
        );
    }
}
