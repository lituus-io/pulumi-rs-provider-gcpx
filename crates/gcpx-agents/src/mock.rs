// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! A recording double for the agent APIs.
//!
//! # What a double is worth here, and what it is not
//!
//! These resources send JSON to Google and interpret what comes back. A double
//! cannot tell you Google accepts the JSON — only the live suite can, and does.
//! What it *can* pin down is everything between the property map and the wire:
//! that publishing writes the published context and staging does not, that an
//! IAM write is a read-merge-write under an etag, that a 409 adopts rather than
//! fails, that a 404 on read reports the resource as gone rather than erroring.
//!
//! Those are the behaviours that decide whether a deploy is correct, and they
//! are cheap to get wrong. So the double records every call and every body, and
//! the tests assert on what was sent rather than only on what was returned.

use std::future::Future;
use std::sync::Mutex;

use gcpx_bq::mock::MockError;

use crate::ops::{DataAgentOps, VertexAgentOps};
use crate::types::{AgentEngineMeta, ConversationMeta, DataAgentMeta, IamPolicyMeta, MemoryMeta};

/// One recorded call: the operation name and the body that accompanied it.
#[derive(Debug, Clone)]
pub struct Call {
    pub op: String,
    pub target: String,
    pub body: serde_json::Value,
}

#[derive(Default)]
pub struct MockAgentClient {
    pub calls: Mutex<Vec<Call>>,
    /// Policy returned by `get_agent_iam_policy`, so merge behaviour can be
    /// tested against a policy this stack does not own.
    pub existing_policy: Mutex<IamPolicyMeta>,
    /// Agent returned by `get_data_agent`.
    pub existing_agent: Mutex<Option<DataAgentMeta>>,
    /// Method name that should fail, and the error it should fail with.
    pub fail_on: Mutex<Option<(String, String)>>,
}

impl MockAgentClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make one method fail with an error carrying `message`.
    ///
    /// The message is what classification keys on, so `"409 conflict"` exercises
    /// the adopt path and `"404 not found"` the gone path.
    pub fn failing(method: &str, message: &str) -> Self {
        Self {
            fail_on: Mutex::new(Some((method.to_owned(), message.to_owned()))),
            ..Default::default()
        }
    }

    pub fn with_policy(bindings: Vec<(String, Vec<String>)>, etag: &str) -> Self {
        Self {
            existing_policy: Mutex::new(IamPolicyMeta {
                etag: etag.to_owned(),
                version: 3,
                bindings,
            }),
            ..Default::default()
        }
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    /// The operations recorded, in order — the usual assertion.
    pub fn ops(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.op.clone())
            .collect()
    }

    /// The body sent with the first call to `op`.
    pub fn body_for(&self, op: &str) -> Option<serde_json::Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.op == op)
            .map(|c| c.body.clone())
    }

    fn record(&self, op: &str, target: &str, body: serde_json::Value) {
        self.calls.lock().unwrap().push(Call {
            op: op.to_owned(),
            target: target.to_owned(),
            body,
        });
    }

    fn guard(&self, method: &str) -> Result<(), MockError> {
        let failing = self.fail_on.lock().unwrap().clone();
        match failing {
            Some((m, message)) if m == method => Err(MockError(message)),
            _ => Ok(()),
        }
    }

    fn agent(name: &str, published: bool) -> DataAgentMeta {
        DataAgentMeta {
            name: name.to_owned(),
            published,
            ..Default::default()
        }
    }
}

#[allow(
    clippy::manual_async_fn,
    reason = "calls are recorded eagerly, before the future is returned, so a \
              dropped future still shows up in the log"
)]
impl DataAgentOps for MockAgentClient {
    type Error = MockError;

    fn create_data_agent<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a {
        self.record("create_data_agent", agent_id, body.clone());
        async move {
            self.guard("create_data_agent")?;
            let published = body
                .pointer("/dataAnalyticsAgent/publishedContext")
                .is_some();
            Ok(Self::agent(agent_id, published))
        }
    }

    fn get_data_agent<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a {
        self.record("get_data_agent", agent_id, serde_json::Value::Null);
        async move {
            self.guard("get_data_agent")?;
            Ok(self
                .existing_agent
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Self::agent(agent_id, false)))
        }
    }

    fn update_data_agent<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
        update_mask: &'a str,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a {
        let mut recorded = body.clone();
        // The mask decides which fields survive the patch, so it is recorded
        // alongside the body rather than discarded.
        recorded["__updateMask"] = serde_json::json!(update_mask);
        self.record("update_data_agent", agent_id, recorded);
        async move {
            self.guard("update_data_agent")?;
            let published = body
                .pointer("/dataAnalyticsAgent/publishedContext")
                .is_some();
            Ok(Self::agent(agent_id, published))
        }
    }

    fn delete_data_agent<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.record("delete_data_agent", agent_id, serde_json::Value::Null);
        async move { self.guard("delete_data_agent") }
    }

    fn get_agent_iam_policy<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<IamPolicyMeta, Self::Error>> + Send + 'a {
        self.record("get_agent_iam_policy", agent_id, serde_json::Value::Null);
        async move {
            self.guard("get_agent_iam_policy")?;
            Ok(self.existing_policy.lock().unwrap().clone())
        }
    }

    fn set_agent_iam_policy<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<IamPolicyMeta, Self::Error>> + Send + 'a {
        self.record("set_agent_iam_policy", agent_id, body.clone());
        async move {
            self.guard("set_agent_iam_policy")?;
            let bindings = body
                .pointer("/policy/bindings")
                .and_then(|b| b.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|b| {
                            Some((
                                b.get("role")?.as_str()?.to_owned(),
                                b.get("members")?
                                    .as_array()?
                                    .iter()
                                    .filter_map(|m| m.as_str().map(str::to_owned))
                                    .collect(),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default();
            Ok(IamPolicyMeta {
                etag: "etag-after-write".to_owned(),
                version: 3,
                bindings,
            })
        }
    }

    fn create_conversation<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        conversation_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<ConversationMeta, Self::Error>> + Send + 'a {
        self.record("create_conversation", conversation_id, body.clone());
        async move {
            self.guard("create_conversation")?;
            Ok(ConversationMeta {
                name: conversation_id.to_owned(),
                agents: body
                    .get("agents")
                    .and_then(|a| a.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                ..Default::default()
            })
        }
    }

    fn get_conversation<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        conversation_id: &'a str,
    ) -> impl Future<Output = Result<ConversationMeta, Self::Error>> + Send + 'a {
        self.record("get_conversation", conversation_id, serde_json::Value::Null);
        async move {
            self.guard("get_conversation")?;
            Ok(ConversationMeta {
                name: conversation_id.to_owned(),
                ..Default::default()
            })
        }
    }

    fn delete_conversation<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        conversation_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.record(
            "delete_conversation",
            conversation_id,
            serde_json::Value::Null,
        );
        async move { self.guard("delete_conversation") }
    }
}

#[allow(
    clippy::manual_async_fn,
    reason = "calls are recorded eagerly, before the future is returned"
)]
impl VertexAgentOps for MockAgentClient {
    type Error = MockError;

    fn create_agent_engine<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a {
        self.record("create_agent_engine", "", body.clone());
        async move {
            self.guard("create_agent_engine")?;
            Ok(AgentEngineMeta {
                // The service assigns the id, so the double does too — the
                // handler must read it back rather than assume one.
                name: "projects/p/locations/us-central1/reasoningEngines/generated-id".to_owned(),
                display_name: body
                    .get("displayName")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_owned(),
                ..Default::default()
            })
        }
    }

    fn get_agent_engine<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        engine_id: &'a str,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a {
        self.record("get_agent_engine", engine_id, serde_json::Value::Null);
        async move {
            self.guard("get_agent_engine")?;
            Ok(AgentEngineMeta {
                name: format!("projects/p/locations/us-central1/reasoningEngines/{engine_id}"),
                ..Default::default()
            })
        }
    }

    fn update_agent_engine<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
        _update_mask: &'a str,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a {
        self.record("update_agent_engine", engine_id, body.clone());
        async move {
            self.guard("update_agent_engine")?;
            Ok(AgentEngineMeta::default())
        }
    }

    fn delete_agent_engine<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        engine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.record("delete_agent_engine", engine_id, serde_json::Value::Null);
        async move { self.guard("delete_agent_engine") }
    }

    fn create_memory<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a {
        self.record("create_memory", engine_id, body.clone());
        async move {
            self.guard("create_memory")?;
            Ok(MemoryMeta {
                name: format!(
                    "projects/p/locations/us-central1/reasoningEngines/{engine_id}/memories/mem-1"
                ),
                fact: body
                    .get("fact")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_owned(),
                ..Default::default()
            })
        }
    }

    fn get_memory<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        _engine_id: &'a str,
        memory_id: &'a str,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a {
        self.record("get_memory", memory_id, serde_json::Value::Null);
        async move {
            self.guard("get_memory")?;
            Ok(MemoryMeta {
                name: memory_id.to_owned(),
                ..Default::default()
            })
        }
    }

    fn update_memory<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        _engine_id: &'a str,
        memory_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a {
        self.record("update_memory", memory_id, body.clone());
        async move {
            self.guard("update_memory")?;
            Ok(MemoryMeta::default())
        }
    }

    fn delete_memory<'a>(
        &'a self,
        _project: &'a str,
        _location: &'a str,
        _engine_id: &'a str,
        memory_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.record("delete_memory", memory_id, serde_json::Value::Null);
        async move { self.guard("delete_memory") }
    }
}
