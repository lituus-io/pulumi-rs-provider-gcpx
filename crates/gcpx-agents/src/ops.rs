// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Conversational Analytics and Vertex AI agent operations.
//!
//! # Sync where it exists, long-running where it does not
//!
//! The data-agent surface publishes both long-running `create`/`patch`/`delete`
//! and synchronous `createSync`/`updateSync`/`deleteSync`. Pulumi's CRUD calls
//! are synchronous, so the Sync variants are used where available — one
//! round-trip instead of a create plus an indeterminate number of polls. The
//! long-running path is still implemented, because Vertex agent engines offer
//! nothing else, and because a region that has not rolled out the Sync methods
//! should degrade rather than fail.

use std::future::Future;

use gcpx_core::auth::CredentialSource;
use gcpx_core::breaker::Service;
use gcpx_core::error::{GcpApiError, GcpError};
use gcpx_core::http::HttpGcpClient;
use gcpx_core::lro::{poll_operation, Operation, PollConfig};
use gcpx_core::sanitize::encode_path_segment as e;
use serde::Deserialize;

use crate::types::{AgentEngineMeta, ConversationMeta, DataAgentMeta, IamPolicyMeta, MemoryMeta};

/// The Conversational Analytics API version.
///
/// `v1beta` rather than `v1`: the beta surface carries the grounding fields
/// this provider depends on — glossary terms, schema relationships and user
/// functions — which is most of what distinguishes a grounded agent from a
/// generic one.
pub const CA_API_VERSION: &str = "v1beta";

/// Vertex AI's agent surface is still `reasoningEngines` on the wire even where
/// the documentation says "agent engines".
pub const VERTEX_API_VERSION: &str = "v1beta1";

/// Regional endpoints keep data in-region. The global endpoint is the default
/// because most locations are served by it.
pub fn ca_endpoint(location: &str) -> String {
    if location.is_empty() || location.eq_ignore_ascii_case("global") {
        "https://geminidataanalytics.googleapis.com".to_owned()
    } else {
        format!("https://geminidataanalytics.{location}.rep.googleapis.com")
    }
}

pub fn vertex_endpoint(location: &str) -> String {
    if location.is_empty() || location.eq_ignore_ascii_case("global") {
        "https://aiplatform.googleapis.com".to_owned()
    } else {
        format!("https://{location}-aiplatform.googleapis.com")
    }
}

pub trait DataAgentOps: Send + Sync + 'static {
    type Error: GcpApiError;

    fn create_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a;

    fn get_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a;

    fn update_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
        update_mask: &'a str,
    ) -> impl Future<Output = Result<DataAgentMeta, Self::Error>> + Send + 'a;

    fn delete_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn get_agent_iam_policy<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> impl Future<Output = Result<IamPolicyMeta, Self::Error>> + Send + 'a;

    fn set_agent_iam_policy<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<IamPolicyMeta, Self::Error>> + Send + 'a;

    fn create_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<ConversationMeta, Self::Error>> + Send + 'a;

    fn get_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
    ) -> impl Future<Output = Result<ConversationMeta, Self::Error>> + Send + 'a;

    fn delete_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

pub trait VertexAgentOps: Send + Sync + 'static {
    type Error: GcpApiError;

    fn create_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a;

    fn get_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a;

    fn update_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
        update_mask: &'a str,
    ) -> impl Future<Output = Result<AgentEngineMeta, Self::Error>> + Send + 'a;

    fn delete_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn create_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a;

    fn get_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a;

    fn update_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<MemoryMeta, Self::Error>> + Send + 'a;

    fn delete_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

// ── URL construction ────────────────────────────────────────────────────────

pub fn agents_collection_url(project: &str, location: &str) -> String {
    format!(
        "{}/{CA_API_VERSION}/projects/{}/locations/{}/dataAgents",
        ca_endpoint(location),
        e(project),
        e(location)
    )
}

pub fn agent_url(project: &str, location: &str, agent_id: &str) -> String {
    format!(
        "{}/{}",
        agents_collection_url(project, location),
        e(agent_id)
    )
}

pub fn conversations_collection_url(project: &str, location: &str) -> String {
    format!(
        "{}/{CA_API_VERSION}/projects/{}/locations/{}/conversations",
        ca_endpoint(location),
        e(project),
        e(location)
    )
}

pub fn conversation_url(project: &str, location: &str, conversation_id: &str) -> String {
    format!(
        "{}/{}",
        conversations_collection_url(project, location),
        e(conversation_id)
    )
}

pub fn engines_collection_url(project: &str, location: &str) -> String {
    format!(
        "{}/{VERTEX_API_VERSION}/projects/{}/locations/{}/reasoningEngines",
        vertex_endpoint(location),
        e(project),
        e(location)
    )
}

pub fn engine_url(project: &str, location: &str, engine_id: &str) -> String {
    format!(
        "{}/{}",
        engines_collection_url(project, location),
        e(engine_id)
    )
}

pub fn memories_collection_url(project: &str, location: &str, engine_id: &str) -> String {
    format!("{}/memories", engine_url(project, location, engine_id))
}

pub fn memory_url(project: &str, location: &str, engine_id: &str, memory_id: &str) -> String {
    format!(
        "{}/{}",
        memories_collection_url(project, location, engine_id),
        e(memory_id)
    )
}

// ── Wire types ──────────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
struct DataAgentResponse {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "displayName")]
    display_name: String,
    #[serde(default)]
    description: String,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "updateTime")]
    update_time: String,
    #[serde(default, rename = "deleteTime")]
    delete_time: String,
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
    #[serde(default, rename = "dataAnalyticsAgent")]
    data_analytics_agent: Option<serde_json::Value>,
}

impl From<DataAgentResponse> for DataAgentMeta {
    fn from(r: DataAgentResponse) -> Self {
        // An agent counts as published when the service reports a published
        // context, not when this provider last asked for one — the two differ
        // after an out-of-band change, which is what refresh exists to surface.
        let published = r
            .data_analytics_agent
            .as_ref()
            .and_then(|a| a.get("publishedContext"))
            .is_some();
        Self {
            name: r.name,
            display_name: r.display_name,
            description: r.description,
            create_time: r.create_time,
            update_time: r.update_time,
            delete_time: r.delete_time,
            labels: r.labels,
            published,
        }
    }
}

#[derive(Deserialize, Default)]
struct ConversationResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    agents: Vec<String>,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "lastUsedTime")]
    last_used_time: String,
}

impl From<ConversationResponse> for ConversationMeta {
    fn from(r: ConversationResponse) -> Self {
        Self {
            name: r.name,
            agents: r.agents,
            create_time: r.create_time,
            last_used_time: r.last_used_time,
        }
    }
}

#[derive(Deserialize, Default)]
struct IamPolicyResponse {
    #[serde(default)]
    etag: String,
    #[serde(default)]
    version: i32,
    #[serde(default)]
    bindings: Vec<IamBindingResponse>,
}

#[derive(Deserialize, Default)]
struct IamBindingResponse {
    #[serde(default)]
    role: String,
    #[serde(default)]
    members: Vec<String>,
}

impl From<IamPolicyResponse> for IamPolicyMeta {
    fn from(r: IamPolicyResponse) -> Self {
        Self {
            etag: r.etag,
            version: r.version,
            bindings: r
                .bindings
                .into_iter()
                .map(|b| (b.role, b.members))
                .collect(),
        }
    }
}

#[derive(Deserialize, Default)]
struct AgentEngineResponse {
    #[serde(default)]
    name: String,
    #[serde(default, rename = "displayName")]
    display_name: String,
    #[serde(default)]
    description: String,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "updateTime")]
    update_time: String,
    #[serde(default)]
    etag: String,
}

impl From<AgentEngineResponse> for AgentEngineMeta {
    fn from(r: AgentEngineResponse) -> Self {
        Self {
            name: r.name,
            display_name: r.display_name,
            description: r.description,
            create_time: r.create_time,
            update_time: r.update_time,
            etag: r.etag,
        }
    }
}

#[derive(Deserialize, Default)]
struct MemoryResponse {
    #[serde(default)]
    name: String,
    #[serde(default)]
    fact: String,
    #[serde(default, rename = "createTime")]
    create_time: String,
    #[serde(default, rename = "updateTime")]
    update_time: String,
}

impl From<MemoryResponse> for MemoryMeta {
    fn from(r: MemoryResponse) -> Self {
        Self {
            name: r.name,
            fact: r.fact,
            create_time: r.create_time,
            update_time: r.update_time,
        }
    }
}

// ── Implementations ─────────────────────────────────────────────────────────

impl<C: CredentialSource> DataAgentOps for HttpGcpClient<C> {
    type Error = GcpError;

    async fn create_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<DataAgentMeta, Self::Error> {
        let base = agents_collection_url(project, location);
        let sync_url = format!("{base}:createSync?dataAgentId={}", e(agent_id));

        // One round-trip when the region offers it; fall back to the
        // long-running form where it does not, rather than failing.
        match self
            .post_json::<DataAgentResponse>(Service::DataAgents, &sync_url, body)
            .await
        {
            Ok(r) => Ok(r.into()),
            Err(err) if is_missing_method(&err) => {
                let url = format!("{base}?dataAgentId={}", e(agent_id));
                let op: Operation = self.post_json(Service::DataAgents, &url, body).await?;
                self.await_agent(project, location, agent_id, op).await
            }
            Err(err) => Err(err),
        }
    }

    async fn get_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> Result<DataAgentMeta, Self::Error> {
        let r: DataAgentResponse = self
            .get_json(Service::DataAgents, &agent_url(project, location, agent_id))
            .await?;
        Ok(r.into())
    }

    async fn update_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
        update_mask: &'a str,
    ) -> Result<DataAgentMeta, Self::Error> {
        let base = agent_url(project, location, agent_id);
        let sync_url = format!("{base}:updateSync?updateMask={}", e(update_mask));
        match self
            .patch_json::<DataAgentResponse>(Service::DataAgents, &sync_url, body)
            .await
        {
            Ok(r) => Ok(r.into()),
            Err(err) if is_missing_method(&err) => {
                let url = format!("{base}?updateMask={}", e(update_mask));
                let op: Operation = self.patch_json(Service::DataAgents, &url, body).await?;
                self.await_agent(project, location, agent_id, op).await
            }
            Err(err) => Err(err),
        }
    }

    async fn delete_data_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> Result<(), Self::Error> {
        let base = agent_url(project, location, agent_id);
        match self
            .delete_ok(Service::DataAgents, &format!("{base}:deleteSync"))
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if is_missing_method(&err) => self.delete_ok(Service::DataAgents, &base).await,
            Err(err) => Err(err),
        }
    }

    async fn get_agent_iam_policy<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
    ) -> Result<IamPolicyMeta, Self::Error> {
        let url = format!("{}:getIamPolicy", agent_url(project, location, agent_id));
        let r: IamPolicyResponse = self
            .post_json(Service::DataAgents, &url, &serde_json::json!({}))
            .await?;
        Ok(r.into())
    }

    async fn set_agent_iam_policy<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<IamPolicyMeta, Self::Error> {
        let url = format!("{}:setIamPolicy", agent_url(project, location, agent_id));
        let r: IamPolicyResponse = self.post_json(Service::DataAgents, &url, body).await?;
        Ok(r.into())
    }

    async fn create_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<ConversationMeta, Self::Error> {
        let url = format!(
            "{}?conversationId={}",
            conversations_collection_url(project, location),
            e(conversation_id)
        );
        let r: ConversationResponse = self.post_json(Service::DataAgents, &url, body).await?;
        Ok(r.into())
    }

    async fn get_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
    ) -> Result<ConversationMeta, Self::Error> {
        let r: ConversationResponse = self
            .get_json(
                Service::DataAgents,
                &conversation_url(project, location, conversation_id),
            )
            .await?;
        Ok(r.into())
    }

    async fn delete_conversation<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        conversation_id: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(
            Service::DataAgents,
            &conversation_url(project, location, conversation_id),
        )
        .await
    }
}

impl<C: CredentialSource> HttpGcpClientAgentExt for HttpGcpClient<C> {}

/// Helper shared by the long-running fallbacks.
///
/// Declared as a trait with a blanket impl rather than an inherent one: an
/// inherent `impl` on `HttpGcpClient` outside its own crate is a coherence
/// error.
trait HttpGcpClientAgentExt {
    fn await_agent<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        agent_id: &'a str,
        op: Operation,
    ) -> impl Future<Output = Result<DataAgentMeta, GcpError>> + Send + 'a
    where
        Self: DataAgentOps<Error = GcpError> + Sync,
    {
        async move {
            poll_operation(
                op,
                || async {
                    Ok(Operation {
                        done: true,
                        ..Default::default()
                    })
                },
                PollConfig::quick(),
            )
            .await?;
            self.get_data_agent(project, location, agent_id).await
        }
    }
}

impl<C: CredentialSource> VertexAgentOps for HttpGcpClient<C> {
    type Error = GcpError;

    async fn create_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<AgentEngineMeta, Self::Error> {
        let url = engines_collection_url(project, location);
        let op: Operation = self.post_json(Service::Vertex, &url, body).await?;
        let done = self.poll_vertex(op, PollConfig::slow()).await?;
        parse_engine_from_operation(done)
    }

    async fn get_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
    ) -> Result<AgentEngineMeta, Self::Error> {
        let r: AgentEngineResponse = self
            .get_json(Service::Vertex, &engine_url(project, location, engine_id))
            .await?;
        Ok(r.into())
    }

    async fn update_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
        update_mask: &'a str,
    ) -> Result<AgentEngineMeta, Self::Error> {
        let url = format!(
            "{}?updateMask={}",
            engine_url(project, location, engine_id),
            e(update_mask)
        );
        let op: Operation = self.patch_json(Service::Vertex, &url, body).await?;
        self.poll_vertex(op, PollConfig::default()).await?;
        self.get_agent_engine(project, location, engine_id).await
    }

    async fn delete_agent_engine<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
    ) -> Result<(), Self::Error> {
        // `force` cascades to sessions and memories the engine owns; without it
        // a delete fails on any engine that has ever been used.
        let url = format!("{}?force=true", engine_url(project, location, engine_id));
        self.delete_ok(Service::Vertex, &url).await
    }

    async fn create_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<MemoryMeta, Self::Error> {
        let url = memories_collection_url(project, location, engine_id);
        let op: Operation = self.post_json(Service::Vertex, &url, body).await?;
        let done = self.poll_vertex(op, PollConfig::quick()).await?;
        match done.response {
            Some(v) => Ok(serde_json::from_value::<MemoryResponse>(v)
                .map_err(|err| GcpError::Api {
                    status: 500,
                    message: format!("unexpected memory payload: {err}"),
                })?
                .into()),
            None => Ok(MemoryMeta::default()),
        }
    }

    async fn get_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
    ) -> Result<MemoryMeta, Self::Error> {
        let r: MemoryResponse = self
            .get_json(
                Service::Vertex,
                &memory_url(project, location, engine_id, memory_id),
            )
            .await?;
        Ok(r.into())
    }

    async fn update_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<MemoryMeta, Self::Error> {
        let url = format!(
            "{}?updateMask=fact,displayName,description",
            memory_url(project, location, engine_id, memory_id)
        );
        let op: Operation = self.patch_json(Service::Vertex, &url, body).await?;
        self.poll_vertex(op, PollConfig::quick()).await?;
        self.get_memory(project, location, engine_id, memory_id)
            .await
    }

    async fn delete_memory<'a>(
        &'a self,
        project: &'a str,
        location: &'a str,
        engine_id: &'a str,
        memory_id: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(
            Service::Vertex,
            &memory_url(project, location, engine_id, memory_id),
        )
        .await
    }
}

/// Vertex operation polling, shared by every long-running Vertex call.
trait VertexPoll {
    fn poll_vertex(
        &self,
        op: Operation,
        config: PollConfig,
    ) -> impl Future<Output = Result<Operation, GcpError>> + Send;
}

impl<C: CredentialSource> VertexPoll for HttpGcpClient<C> {
    async fn poll_vertex(&self, op: Operation, config: PollConfig) -> Result<Operation, GcpError> {
        let name = op.name.clone();
        let location = location_from_operation_name(&name).unwrap_or_default();
        let base = vertex_endpoint(&location);
        poll_operation(
            op,
            || async {
                if name.is_empty() {
                    return Ok(Operation {
                        done: true,
                        ..Default::default()
                    });
                }
                self.get_json(
                    Service::Vertex,
                    &format!("{base}/{VERTEX_API_VERSION}/{name}"),
                )
                .await
            },
            config,
        )
        .await
    }
}

fn parse_engine_from_operation(op: Operation) -> Result<AgentEngineMeta, GcpError> {
    match op.response {
        Some(v) => Ok(serde_json::from_value::<AgentEngineResponse>(v)
            .map_err(|err| GcpError::Api {
                status: 500,
                message: format!("unexpected agent engine payload: {err}"),
            })?
            .into()),
        None => Err(GcpError::OperationFailed {
            message: "agent engine operation completed without a resource".to_owned(),
        }),
    }
}

/// A region that has not rolled out the Sync methods reports 404 or 501 for
/// them. Anything else is a real failure and must not be retried against a
/// different endpoint, which would mask it.
fn is_missing_method(err: &GcpError) -> bool {
    matches!(err.http_status(), Some(404) | Some(501))
}

/// Operation names carry their location: `projects/p/locations/l/operations/o`.
fn location_from_operation_name(name: &str) -> Option<String> {
    let mut parts = name.split('/');
    while let Some(p) = parts.next() {
        if p == "locations" {
            return parts.next().map(str::to_owned);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_location_uses_the_global_endpoint() {
        assert_eq!(
            ca_endpoint("global"),
            "https://geminidataanalytics.googleapis.com"
        );
        assert_eq!(
            ca_endpoint(""),
            "https://geminidataanalytics.googleapis.com"
        );
    }

    #[test]
    fn regional_location_uses_a_regional_endpoint() {
        // Data residency depends on this: a regional agent reached through the
        // global endpoint would not stay in region.
        assert_eq!(
            ca_endpoint("northamerica-northeast2"),
            "https://geminidataanalytics.northamerica-northeast2.rep.googleapis.com"
        );
        assert_eq!(
            vertex_endpoint("us-central1"),
            "https://us-central1-aiplatform.googleapis.com"
        );
    }

    #[test]
    fn conversations_are_top_level_not_nested_under_an_agent() {
        // The obvious guess — nesting them under dataAgents — 404s.
        let url = conversation_url("p", "global", "c");
        assert!(url.ends_with("/locations/global/conversations/c"), "{url}");
        assert!(!url.contains("dataAgents"));
    }

    #[test]
    fn memories_are_nested_under_their_engine() {
        let url = memory_url("p", "us-central1", "eng", "mem");
        assert!(url.ends_with("/reasoningEngines/eng/memories/mem"), "{url}");
    }

    #[test]
    fn urls_percent_encode_untrusted_identifiers() {
        assert!(agent_url("p", "global", "a/b").ends_with("/dataAgents/a%2Fb"));
        assert!(conversation_url("p", "global", "a?b").ends_with("/conversations/a%3Fb"));
    }

    #[test]
    fn beta_surface_is_used_for_data_agents() {
        // v1 omits the grounding fields this provider is built around.
        assert!(agent_url("p", "global", "a").contains("/v1beta/"));
    }

    #[test]
    fn only_absent_methods_trigger_the_long_running_fallback() {
        // Falling back on any error would mask a real failure by retrying it
        // against a different endpoint.
        assert!(is_missing_method(&GcpError::Api {
            status: 404,
            message: String::new()
        }));
        assert!(is_missing_method(&GcpError::Api {
            status: 501,
            message: String::new()
        }));
        for status in [400, 403, 409, 429, 500, 503] {
            assert!(
                !is_missing_method(&GcpError::Api {
                    status,
                    message: String::new()
                }),
                "{status} must not trigger a fallback"
            );
        }
    }

    #[test]
    fn operation_location_is_read_from_its_name() {
        assert_eq!(
            location_from_operation_name("projects/p/locations/us-central1/operations/123")
                .as_deref(),
            Some("us-central1")
        );
        assert_eq!(location_from_operation_name("garbage"), None);
    }

    #[test]
    fn published_state_is_read_from_the_service_not_assumed() {
        // After an out-of-band change the two differ, which is exactly what
        // refresh exists to surface.
        let r: DataAgentResponse = serde_json::from_value(serde_json::json!({
            "name": "projects/p/locations/global/dataAgents/a",
            "dataAnalyticsAgent": { "stagingContext": {} }
        }))
        .unwrap();
        assert!(!DataAgentMeta::from(r).published);

        let r: DataAgentResponse = serde_json::from_value(serde_json::json!({
            "dataAnalyticsAgent": { "publishedContext": {} }
        }))
        .unwrap();
        assert!(DataAgentMeta::from(r).published);
    }

    #[test]
    fn engine_delete_forces_cascade() {
        // Without force, deleting an engine that has ever been used fails.
        let url = format!("{}?force=true", engine_url("p", "us-central1", "e"));
        assert!(url.contains("force=true"));
    }
}
