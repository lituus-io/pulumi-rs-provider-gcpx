// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Pulumi CRUD for the agent resources.

use gcpx_core::error::{ClassifyStatus, GcpApiError};
use gcpx_core::handler_util::{build_check_response, build_diff_response};
use gcpx_core::output::OutputBuilder;
use gcpx_core::resource::{require_non_empty, CheckFailure, DiffResult};
use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::api_body::*;
use crate::ops::{DataAgentOps, VertexAgentOps};
use crate::parse::*;
use crate::types::*;
use crate::validate;

const CA: &str = "Conversational Analytics";
const VERTEX: &str = "Vertex AI";

// ── DataAgent ───────────────────────────────────────────────────────────────

pub async fn check_data_agent<C: DataAgentOps>(
    // Validation is pure; the signature stays uniform so dispatch treats every
    // handler alike.
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    // The borrow is scoped so the property map can be moved into the response
    // once validation has finished reading it.
    let failures = {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_data_agent(news).map_err(Status::invalid_argument)?;
        validate::validate_data_agent(&inputs)
    };
    build_check_response(req.news, failures)
}

pub async fn diff_data_agent<C: DataAgentOps>(
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    let olds = req
        .olds
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing olds"))?;
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    // Location and identity are baked into the resource name, so changing
    // either means a different agent, not a modified one.
    let mut replace_keys = Vec::new();
    for key in ["project", "location", "agentId"] {
        if gcpx_core::prost_util::get_str(&olds.fields, key)
            != gcpx_core::prost_util::get_str(&news.fields, key)
        {
            replace_keys.push(match key {
                "project" => "project",
                "location" => "location",
                _ => "agentId",
            });
        }
    }

    let old_inputs = parse_data_agent(olds).map_err(Status::internal)?;
    let new_inputs = parse_data_agent(news).map_err(Status::invalid_argument)?;

    // Everything else is a context change, which the API applies in place.
    let mut update_keys = Vec::new();
    if build_agent_body(&old_inputs) != build_agent_body(&new_inputs) {
        update_keys.push("context");
    }
    if old_inputs.publish != new_inputs.publish {
        update_keys.push("publish");
    }

    Ok(build_diff_response(&DiffResult {
        replace_keys,
        update_keys,
    }))
}

pub async fn create_data_agent<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;
    let inputs = parse_data_agent(props).map_err(Status::invalid_argument)?;
    let id = agent_resource_id(&inputs);

    if req.preview {
        return Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: Some(agent_outputs(&inputs, &DataAgentMeta::default())),
            ..Default::default()
        }));
    }

    let body = build_agent_body(&inputs);
    let meta = match client
        .create_data_agent(inputs.project, inputs.location, inputs.agent_id, &body)
        .await
    {
        Ok(m) => m,
        // An agent that already exists is adopted rather than failing the
        // deploy, matching how every other resource here handles a 409.
        Err(e) if e.is_conflict() => {
            let mask = agent_update_mask(&inputs);
            client
                .update_data_agent(
                    inputs.project,
                    inputs.location,
                    inputs.agent_id,
                    &body,
                    &mask,
                )
                .await
                .classify(CA, inputs.project, &id)?
        }
        Err(e) => {
            return Err(
                gcpx_core::error::GcpxError::classify(&e, CA, inputs.project, &id).into_status(),
            )
        }
    };

    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(agent_outputs(&inputs, &meta)),
        ..Default::default()
    }))
}

pub async fn read_data_agent<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (project, location, agent_id) = split_agent_id(&req.id)?;
    match client.get_data_agent(project, location, agent_id).await {
        Ok(meta) => {
            let mut out = OutputBuilder::new()
                .str("project", project)
                .str("location", location)
                .str("agentId", agent_id)
                .str("name", &meta.name)
                .str("displayName", &meta.display_name)
                .str("description", &meta.description)
                .str("createTime", &meta.create_time)
                .str("updateTime", &meta.update_time)
                .bool_opt("published", Some(meta.published))
                .build();
            if !meta.delete_time.is_empty() {
                out.fields.insert(
                    "deleteTime".to_owned(),
                    gcpx_core::prost_util::prost_string(&meta.delete_time),
                );
            }
            Ok(Response::new(pulumirpc::ReadResponse {
                id: req.id,
                inputs: Some(out.clone()),
                properties: Some(out),
                ..Default::default()
            }))
        }
        // An empty id tells the engine the resource is gone, so the next
        // deploy recreates it instead of trying to update nothing.
        Err(e) if e.is_not_found() => Ok(Response::new(pulumirpc::ReadResponse::default())),
        Err(e) => {
            Err(gcpx_core::error::GcpxError::classify(&e, CA, project, &req.id).into_status())
        }
    }
}

pub async fn update_data_agent<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_data_agent(news).map_err(Status::invalid_argument)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::UpdateResponse {
            properties: Some(agent_outputs(&inputs, &DataAgentMeta::default())),
            ..Default::default()
        }));
    }

    let body = build_agent_body(&inputs);
    let mask = agent_update_mask(&inputs);
    let meta = client
        .update_data_agent(
            inputs.project,
            inputs.location,
            inputs.agent_id,
            &body,
            &mask,
        )
        .await
        .classify(CA, inputs.project, &req.id)?;

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(agent_outputs(&inputs, &meta)),
        ..Default::default()
    }))
}

pub async fn delete_data_agent<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (project, location, agent_id) = split_agent_id(&req.id)?;
    client
        .delete_data_agent(project, location, agent_id)
        .await
        .classify(CA, project, &req.id)?;
    Ok(Response::new(()))
}

// ── DataAgentIamPolicy ──────────────────────────────────────────────────────

pub async fn check_agent_iam_policy<C: DataAgentOps>(
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    // The borrow is scoped so the property map can be moved into the response
    // once validation has finished reading it.
    let failures = {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_iam_policy(news).map_err(Status::invalid_argument)?;
        validate::validate_iam_policy(&inputs)
    };
    build_check_response(req.news, failures)
}

pub async fn diff_agent_iam_policy<C: DataAgentOps>(
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    let olds = req.olds.as_ref();
    let news = req.news.as_ref();
    let changed = olds.map(|o| &o.fields) != news.map(|n| &n.fields);
    Ok(build_diff_response(&DiffResult {
        replace_keys: vec![],
        update_keys: if changed { vec!["bindings"] } else { vec![] },
    }))
}

pub async fn create_agent_iam_policy<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;
    let inputs = parse_iam_policy(props).map_err(Status::invalid_argument)?;
    let id = format!(
        "{}/{}/{}/iam",
        inputs.project, inputs.location, inputs.agent_id
    );

    if req.preview {
        return Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: req.properties,
            ..Default::default()
        }));
    }

    let meta = apply_iam_policy(client, &inputs, &id).await?;
    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(iam_outputs(&inputs, &meta)),
        ..Default::default()
    }))
}

pub async fn read_agent_iam_policy<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let mut parts = req.id.splitn(4, '/');
    let (project, location, agent_id) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    let meta = client
        .get_agent_iam_policy(project, location, agent_id)
        .await
        .classify(CA, project, &req.id)?;

    let bindings: Vec<prost_types::Value> = meta
        .bindings
        .iter()
        .map(|(role, members)| {
            let refs: Vec<&str> = members.iter().map(String::as_str).collect();
            OutputBuilder::new()
                .str("role", role)
                .str_list("members", &refs)
                .build_value()
        })
        .collect();

    let out = OutputBuilder::new()
        .str("project", project)
        .str("location", location)
        .str("agentId", agent_id)
        .str("etag", &meta.etag)
        .list("bindings", bindings)
        .build();

    Ok(Response::new(pulumirpc::ReadResponse {
        id: req.id,
        inputs: Some(out.clone()),
        properties: Some(out),
        ..Default::default()
    }))
}

pub async fn update_agent_iam_policy<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_iam_policy(news).map_err(Status::invalid_argument)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::UpdateResponse {
            properties: req.news,
            ..Default::default()
        }));
    }

    let meta = apply_iam_policy(client, &inputs, &req.id).await?;
    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(iam_outputs(&inputs, &meta)),
        ..Default::default()
    }))
}

pub async fn delete_agent_iam_policy<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let mut parts = req.id.splitn(4, '/');
    let (project, location, agent_id) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );

    // Removing this resource removes the bindings it granted. The current
    // policy is read first so bindings owned by someone else survive — an
    // unconditional empty policy here would revoke everyone's access.
    let current = client
        .get_agent_iam_policy(project, location, agent_id)
        .await
        .classify(CA, project, &req.id)?;

    let body = build_iam_policy_body(&[], &current.etag);
    client
        .set_agent_iam_policy(project, location, agent_id, &body)
        .await
        .classify(CA, project, &req.id)?;
    Ok(Response::new(()))
}

/// Read the live policy, merge or replace, and write it back under its etag.
async fn apply_iam_policy<C: DataAgentOps>(
    client: &C,
    inputs: &IamPolicyInputs<'_>,
    id: &str,
) -> Result<IamPolicyMeta, Status> {
    let current = client
        .get_agent_iam_policy(inputs.project, inputs.location, inputs.agent_id)
        .await
        .classify(CA, inputs.project, id)?;

    let merged = if inputs.authoritative {
        inputs.bindings.clone()
    } else {
        validate::merge_bindings(&current, &inputs.bindings)
    };

    // The etag makes this a compare-and-swap: if the policy changed since it
    // was read, the write is rejected rather than silently clobbering it.
    let body = build_iam_policy_body(&merged, &current.etag);
    client
        .set_agent_iam_policy(inputs.project, inputs.location, inputs.agent_id, &body)
        .await
        .classify(CA, inputs.project, id)
}

// ── Conversation ────────────────────────────────────────────────────────────

pub async fn check_conversation<C: DataAgentOps>(
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_conversation(news).map_err(Status::invalid_argument)?;
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "conversationId", inputs.conversation_id);
    if inputs.agents.is_empty() {
        failures.push(CheckFailure {
            property: "agents".into(),
            reason: "a conversation must reference at least one data agent".into(),
        });
    }
    build_check_response(req.news, failures)
}

pub async fn diff_conversation<C: DataAgentOps>(
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    // The API offers no update for a conversation, so any change is a replace.
    let changed = req.olds.as_ref().map(|o| &o.fields) != req.news.as_ref().map(|n| &n.fields);
    Ok(build_diff_response(&DiffResult {
        replace_keys: if changed { vec!["agents"] } else { vec![] },
        update_keys: vec![],
    }))
}

pub async fn create_conversation<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;
    let inputs = parse_conversation(props).map_err(Status::invalid_argument)?;
    let id = format!(
        "{}/{}/{}",
        inputs.project, inputs.location, inputs.conversation_id
    );

    if req.preview {
        return Ok(Response::new(pulumirpc::CreateResponse {
            id,
            properties: req.properties,
            ..Default::default()
        }));
    }

    let body = build_conversation_body(&inputs.agents, &inputs.labels);
    let meta = client
        .create_conversation(
            inputs.project,
            inputs.location,
            inputs.conversation_id,
            &body,
        )
        .await
        .classify(CA, inputs.project, &id)?;

    let agents: Vec<&str> = meta.agents.iter().map(String::as_str).collect();
    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(
            OutputBuilder::new()
                .str("project", inputs.project)
                .str("location", inputs.location)
                .str("conversationId", inputs.conversation_id)
                .str("name", &meta.name)
                .str_list("agents", &agents)
                .str("createTime", &meta.create_time)
                .build(),
        ),
        ..Default::default()
    }))
}

pub async fn read_conversation<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (project, location, conversation_id) = split_agent_id(&req.id)?;
    match client
        .get_conversation(project, location, conversation_id)
        .await
    {
        Ok(meta) => {
            let agents: Vec<&str> = meta.agents.iter().map(String::as_str).collect();
            let out = OutputBuilder::new()
                .str("project", project)
                .str("location", location)
                .str("conversationId", conversation_id)
                .str("name", &meta.name)
                .str_list("agents", &agents)
                .str("createTime", &meta.create_time)
                .str("lastUsedTime", &meta.last_used_time)
                .build();
            Ok(Response::new(pulumirpc::ReadResponse {
                id: req.id,
                inputs: Some(out.clone()),
                properties: Some(out),
                ..Default::default()
            }))
        }
        Err(e) if e.is_not_found() => Ok(Response::new(pulumirpc::ReadResponse::default())),
        Err(e) => {
            Err(gcpx_core::error::GcpxError::classify(&e, CA, project, &req.id).into_status())
        }
    }
}

pub async fn update_conversation<C: DataAgentOps>(
    _client: &C,
    _req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    // Unreachable in practice: diff marks every change as a replacement,
    // because the API has no update method for a conversation.
    Err(Status::unimplemented(
        "conversations cannot be updated in place; change the conversationId to create a new one",
    ))
}

pub async fn delete_conversation<C: DataAgentOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (project, location, conversation_id) = split_agent_id(&req.id)?;
    client
        .delete_conversation(project, location, conversation_id)
        .await
        .classify(CA, project, &req.id)?;
    Ok(Response::new(()))
}

// ── Shared output helpers ───────────────────────────────────────────────────

fn agent_resource_id(inputs: &DataAgentInputs<'_>) -> String {
    format!("{}/{}/{}", inputs.project, inputs.location, inputs.agent_id)
}

/// Split `project/location/id`.
#[allow(
    clippy::result_large_err,
    reason = "Status is the gRPC error type; it is large by construction"
)]
fn split_agent_id(id: &str) -> Result<(&str, &str, &str), Status> {
    let mut parts = id.splitn(3, '/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(p), Some(l), Some(a)) if !p.is_empty() && !l.is_empty() && !a.is_empty() => {
            Ok((p, l, a))
        }
        _ => Err(malformed_id(id, "project/location/id")),
    }
}

/// `Status` is large, so it is constructed here rather than being threaded
/// through the return type of every small helper.
#[cold]
#[inline(never)]
fn malformed_id(id: &str, expected: &str) -> Status {
    Status::invalid_argument(format!("malformed id '{id}', expected '{expected}'"))
}

fn agent_outputs(inputs: &DataAgentInputs<'_>, meta: &DataAgentMeta) -> prost_types::Struct {
    let name = if meta.name.is_empty() {
        format!(
            "projects/{}/locations/{}/dataAgents/{}",
            inputs.project, inputs.location, inputs.agent_id
        )
    } else {
        meta.name.clone()
    };
    let tables: Vec<String> = match &inputs.context.datasources {
        Datasources::BigQuery(t) => t.iter().map(|t| t.qualified()).collect(),
        Datasources::Looker(e) => e.iter().map(|e| e.explore.to_owned()).collect(),
    };
    let table_refs: Vec<&str> = tables.iter().map(String::as_str).collect();

    OutputBuilder::new()
        .str("project", inputs.project)
        .str("location", inputs.location)
        .str("agentId", inputs.agent_id)
        .str("name", &name)
        .str_opt("displayName", inputs.display_name)
        .str_opt("description", inputs.description)
        .str("createTime", &meta.create_time)
        .str("updateTime", &meta.update_time)
        .bool_opt("published", Some(inputs.publish))
        // Surfaced so a stack can assert on exactly what the agent was
        // grounded on, which is otherwise buried in the context.
        .str_list("groundedTables", &table_refs)
        .str("datasourceKind", inputs.context.datasources.kind())
        .build()
}

fn iam_outputs(inputs: &IamPolicyInputs<'_>, meta: &IamPolicyMeta) -> prost_types::Struct {
    let bindings: Vec<prost_types::Value> = meta
        .bindings
        .iter()
        .map(|(role, members)| {
            let refs: Vec<&str> = members.iter().map(String::as_str).collect();
            OutputBuilder::new()
                .str("role", role)
                .str_list("members", &refs)
                .build_value()
        })
        .collect();

    OutputBuilder::new()
        .str("project", inputs.project)
        .str("location", inputs.location)
        .str("agentId", inputs.agent_id)
        .str("etag", &meta.etag)
        .bool_val("authoritative", inputs.authoritative)
        .list("bindings", bindings)
        .build()
}

// ── AgentEngine and Memory ──────────────────────────────────────────────────

pub async fn check_agent_engine<C: VertexAgentOps>(
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    // The borrow is scoped so the property map can be moved into the response
    // once validation has finished reading it.
    let failures = {
        let news = req
            .news
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing news"))?;
        let inputs = parse_agent_engine(news).map_err(Status::invalid_argument)?;
        validate::validate_agent_engine(&inputs)
    };
    build_check_response(req.news, failures)
}

pub async fn diff_agent_engine<C: VertexAgentOps>(
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    let olds = req
        .olds
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing olds"))?;
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let mut replace_keys = Vec::new();
    for key in ["project", "location"] {
        if gcpx_core::prost_util::get_str(&olds.fields, key)
            != gcpx_core::prost_util::get_str(&news.fields, key)
        {
            replace_keys.push(if key == "project" {
                "project"
            } else {
                "location"
            });
        }
    }
    let mut update_keys = Vec::new();
    for key in ["displayName", "description", "pickleUri", "requirementsUri"] {
        if gcpx_core::prost_util::get_str(&olds.fields, key)
            != gcpx_core::prost_util::get_str(&news.fields, key)
        {
            update_keys.push(match key {
                "displayName" => "displayName",
                "description" => "description",
                "pickleUri" => "pickleUri",
                _ => "requirementsUri",
            });
        }
    }
    Ok(build_diff_response(&DiffResult {
        replace_keys,
        update_keys,
    }))
}

pub async fn create_agent_engine<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;
    let inputs = parse_agent_engine(props).map_err(Status::invalid_argument)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::CreateResponse {
            id: String::new(),
            properties: req.properties,
            ..Default::default()
        }));
    }

    let body = build_engine_body(&inputs);
    let meta = client
        .create_agent_engine(inputs.project, inputs.location, &body)
        .await
        .classify(VERTEX, inputs.project, inputs.display_name)?;

    // The service assigns the id, so it is read back out of the resource name
    // rather than guessed.
    let engine_id = meta.name.rsplit('/').next().unwrap_or_default().to_owned();
    Ok(Response::new(pulumirpc::CreateResponse {
        id: format!("{}/{}/{}", inputs.project, inputs.location, engine_id),
        properties: Some(engine_outputs(&inputs, &meta, &engine_id)),
        ..Default::default()
    }))
}

pub async fn read_agent_engine<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (project, location, engine_id) = split_agent_id(&req.id)?;
    match client.get_agent_engine(project, location, engine_id).await {
        Ok(meta) => {
            let out = OutputBuilder::new()
                .str("project", project)
                .str("location", location)
                .str("engineId", engine_id)
                .str("name", &meta.name)
                .str("displayName", &meta.display_name)
                .str("description", &meta.description)
                .str("createTime", &meta.create_time)
                .str("updateTime", &meta.update_time)
                .build();
            Ok(Response::new(pulumirpc::ReadResponse {
                id: req.id,
                inputs: Some(out.clone()),
                properties: Some(out),
                ..Default::default()
            }))
        }
        Err(e) if e.is_not_found() => Ok(Response::new(pulumirpc::ReadResponse::default())),
        Err(e) => {
            Err(gcpx_core::error::GcpxError::classify(&e, VERTEX, project, &req.id).into_status())
        }
    }
}

pub async fn update_agent_engine<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_agent_engine(news).map_err(Status::invalid_argument)?;
    let (_, _, engine_id) = split_agent_id(&req.id)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::UpdateResponse {
            properties: req.news,
            ..Default::default()
        }));
    }

    let body = build_engine_body(&inputs);
    let meta = client
        .update_agent_engine(
            inputs.project,
            inputs.location,
            engine_id,
            &body,
            "displayName,description,spec",
        )
        .await
        .classify(VERTEX, inputs.project, &req.id)?;

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(engine_outputs(&inputs, &meta, engine_id)),
        ..Default::default()
    }))
}

pub async fn delete_agent_engine<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (project, location, engine_id) = split_agent_id(&req.id)?;
    client
        .delete_agent_engine(project, location, engine_id)
        .await
        .classify(VERTEX, project, &req.id)?;
    Ok(Response::new(()))
}

fn build_engine_body(inputs: &AgentEngineInputs<'_>) -> serde_json::Value {
    let mut spec = serde_json::Map::new();
    let mut package = serde_json::Map::new();
    if let Some(v) = inputs.pickle_uri {
        package.insert("pickleObjectGcsUri".into(), serde_json::json!(v));
    }
    if let Some(v) = inputs.requirements_uri {
        package.insert("requirementsGcsUri".into(), serde_json::json!(v));
    }
    if let Some(v) = inputs.dependency_files_uri {
        package.insert("dependencyFilesGcsUri".into(), serde_json::json!(v));
    }
    if let Some(v) = inputs.python_version {
        package.insert("pythonVersion".into(), serde_json::json!(v));
    }
    if !package.is_empty() {
        spec.insert("packageSpec".into(), serde_json::Value::Object(package));
    }

    let mut deployment = serde_json::Map::new();
    if !inputs.env.is_empty() {
        deployment.insert(
            "env".into(),
            serde_json::Value::Array(
                inputs
                    .env
                    .iter()
                    .map(|(k, v)| serde_json::json!({ "name": k, "value": v }))
                    .collect(),
            ),
        );
    }
    if !inputs.secret_env.is_empty() {
        // Secret-backed variables carry a reference, never a value: the value
        // would otherwise be written into Pulumi state in plain text.
        deployment.insert(
            "secretEnv".into(),
            serde_json::Value::Array(
                inputs
                    .secret_env
                    .iter()
                    .map(|(k, v)| {
                        serde_json::json!({
                            "name": k,
                            "secretRef": { "secret": v, "version": "latest" }
                        })
                    })
                    .collect(),
            ),
        );
    }
    if !deployment.is_empty() {
        spec.insert(
            "deploymentSpec".into(),
            serde_json::Value::Object(deployment),
        );
    }

    let mut body = serde_json::Map::new();
    body.insert("displayName".into(), serde_json::json!(inputs.display_name));
    if let Some(v) = inputs.description {
        body.insert("description".into(), serde_json::json!(v));
    }
    if !spec.is_empty() {
        body.insert("spec".into(), serde_json::Value::Object(spec));
    }
    serde_json::Value::Object(body)
}

fn engine_outputs(
    inputs: &AgentEngineInputs<'_>,
    meta: &AgentEngineMeta,
    engine_id: &str,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("location", inputs.location)
        .str("engineId", engine_id)
        .str("name", &meta.name)
        .str("displayName", inputs.display_name)
        .str_opt("description", inputs.description)
        .str("createTime", &meta.create_time)
        .str("updateTime", &meta.update_time)
        .build()
}

pub async fn check_memory<C: VertexAgentOps>(
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_memory(news).map_err(Status::invalid_argument)?;
    let mut failures = Vec::new();
    require_non_empty(&mut failures, "project", inputs.project);
    require_non_empty(&mut failures, "engineId", inputs.engine_id);
    require_non_empty(&mut failures, "fact", inputs.fact);
    build_check_response(req.news, failures)
}

pub async fn diff_memory<C: VertexAgentOps>(
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    let olds = req.olds.as_ref();
    let news = req.news.as_ref();
    let mut replace_keys = Vec::new();
    for key in ["project", "location", "engineId"] {
        if olds.and_then(|o| gcpx_core::prost_util::get_str(&o.fields, key))
            != news.and_then(|n| gcpx_core::prost_util::get_str(&n.fields, key))
        {
            replace_keys.push(match key {
                "project" => "project",
                "location" => "location",
                _ => "engineId",
            });
        }
    }
    let fact_changed = olds.and_then(|o| gcpx_core::prost_util::get_str(&o.fields, "fact"))
        != news.and_then(|n| gcpx_core::prost_util::get_str(&n.fields, "fact"));
    Ok(build_diff_response(&DiffResult {
        replace_keys,
        update_keys: if fact_changed { vec!["fact"] } else { vec![] },
    }))
}

pub async fn create_memory<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;
    let inputs = parse_memory(props).map_err(Status::invalid_argument)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::CreateResponse {
            id: String::new(),
            properties: req.properties,
            ..Default::default()
        }));
    }

    let body = build_memory_body(&inputs);
    let meta = client
        .create_memory(inputs.project, inputs.location, inputs.engine_id, &body)
        .await
        .classify(VERTEX, inputs.project, inputs.fact)?;

    let memory_id = meta.name.rsplit('/').next().unwrap_or_default().to_owned();
    Ok(Response::new(pulumirpc::CreateResponse {
        id: format!(
            "{}/{}/{}/{}",
            inputs.project, inputs.location, inputs.engine_id, memory_id
        ),
        properties: Some(memory_outputs(&inputs, &meta, &memory_id)),
        ..Default::default()
    }))
}

pub async fn read_memory<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (project, location, engine_id, memory_id) = split_memory_id(&req.id)?;
    match client
        .get_memory(project, location, engine_id, memory_id)
        .await
    {
        Ok(meta) => {
            let out = OutputBuilder::new()
                .str("project", project)
                .str("location", location)
                .str("engineId", engine_id)
                .str("memoryId", memory_id)
                .str("name", &meta.name)
                .str("fact", &meta.fact)
                .str("createTime", &meta.create_time)
                .str("updateTime", &meta.update_time)
                .build();
            Ok(Response::new(pulumirpc::ReadResponse {
                id: req.id,
                inputs: Some(out.clone()),
                properties: Some(out),
                ..Default::default()
            }))
        }
        Err(e) if e.is_not_found() => Ok(Response::new(pulumirpc::ReadResponse::default())),
        Err(e) => {
            Err(gcpx_core::error::GcpxError::classify(&e, VERTEX, project, &req.id).into_status())
        }
    }
}

pub async fn update_memory<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;
    let inputs = parse_memory(news).map_err(Status::invalid_argument)?;
    let (_, _, _, memory_id) = split_memory_id(&req.id)?;

    if req.preview {
        return Ok(Response::new(pulumirpc::UpdateResponse {
            properties: req.news,
            ..Default::default()
        }));
    }

    let body = build_memory_body(&inputs);
    let meta = client
        .update_memory(
            inputs.project,
            inputs.location,
            inputs.engine_id,
            memory_id,
            &body,
        )
        .await
        .classify(VERTEX, inputs.project, &req.id)?;

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(memory_outputs(&inputs, &meta, memory_id)),
        ..Default::default()
    }))
}

pub async fn delete_memory<C: VertexAgentOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (project, location, engine_id, memory_id) = split_memory_id(&req.id)?;
    client
        .delete_memory(project, location, engine_id, memory_id)
        .await
        .classify(VERTEX, project, &req.id)?;
    Ok(Response::new(()))
}

#[allow(
    clippy::result_large_err,
    reason = "Status is the gRPC error type; it is large by construction"
)]
fn split_memory_id(id: &str) -> Result<(&str, &str, &str, &str), Status> {
    let mut parts = id.splitn(4, '/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(p), Some(l), Some(e), Some(m)) if !m.is_empty() => Ok((p, l, e, m)),
        _ => Err(malformed_id(id, "project/location/engineId/memoryId")),
    }
}

fn build_memory_body(inputs: &MemoryInputs<'_>) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("fact".into(), serde_json::json!(inputs.fact));
    if let Some(v) = inputs.display_name {
        body.insert("displayName".into(), serde_json::json!(v));
    }
    if let Some(v) = inputs.description {
        body.insert("description".into(), serde_json::json!(v));
    }
    if !inputs.scope.is_empty() {
        body.insert(
            "scope".into(),
            serde_json::Value::Object(
                inputs
                    .scope
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), serde_json::json!(v)))
                    .collect(),
            ),
        );
    }
    serde_json::Value::Object(body)
}

fn memory_outputs(
    inputs: &MemoryInputs<'_>,
    meta: &MemoryMeta,
    memory_id: &str,
) -> prost_types::Struct {
    OutputBuilder::new()
        .str("project", inputs.project)
        .str("location", inputs.location)
        .str("engineId", inputs.engine_id)
        .str("memoryId", memory_id)
        .str("name", &meta.name)
        .str("fact", inputs.fact)
        .str("createTime", &meta.create_time)
        .str("updateTime", &meta.update_time)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_ids_round_trip() {
        assert_eq!(split_agent_id("p/global/a").unwrap(), ("p", "global", "a"));
        assert_eq!(
            split_memory_id("p/us-central1/e/m").unwrap(),
            ("p", "us-central1", "e", "m")
        );
    }

    #[test]
    fn malformed_ids_are_rejected_with_the_expected_shape() {
        for id in ["", "p", "p/global", "p//a", "/global/a"] {
            let err = split_agent_id(id).unwrap_err();
            assert!(err.message().contains("project/location/id"), "{id}");
        }
    }

    #[test]
    fn engine_body_carries_secret_references_not_values() {
        // A secret's value in the body would land in Pulumi state in plain text.
        let inputs = AgentEngineInputs {
            project: "p",
            location: "us-central1",
            engine_id: None,
            display_name: "agent",
            description: None,
            pickle_uri: Some("gs://b/agent.pkl"),
            requirements_uri: None,
            dependency_files_uri: None,
            python_version: Some("3.12"),
            env: [("LOG_LEVEL", "info")].into_iter().collect(),
            secret_env: [("API_KEY", "projects/p/secrets/key")]
                .into_iter()
                .collect(),
        };
        let body = build_engine_body(&inputs);
        assert_eq!(
            body["spec"]["packageSpec"]["pickleObjectGcsUri"],
            "gs://b/agent.pkl"
        );
        let secret = &body["spec"]["deploymentSpec"]["secretEnv"][0];
        assert_eq!(secret["secretRef"]["secret"], "projects/p/secrets/key");
        assert!(
            secret.get("value").is_none(),
            "secret value must not be sent"
        );
    }

    #[test]
    fn engine_body_omits_empty_specs() {
        let inputs = AgentEngineInputs {
            project: "p",
            location: "us-central1",
            engine_id: None,
            display_name: "agent",
            description: None,
            pickle_uri: None,
            requirements_uri: None,
            dependency_files_uri: None,
            python_version: None,
            env: Default::default(),
            secret_env: Default::default(),
        };
        let body = build_engine_body(&inputs);
        assert!(body.get("spec").is_none());
        assert_eq!(body["displayName"], "agent");
    }

    #[test]
    fn memory_body_includes_scope_when_present() {
        let inputs = MemoryInputs {
            project: "p",
            location: "us-central1",
            engine_id: "e",
            memory_id: None,
            fact: "prefers metric units",
            display_name: None,
            description: None,
            scope: [("user_id", "u1")].into_iter().collect(),
        };
        let body = build_memory_body(&inputs);
        assert_eq!(body["fact"], "prefers metric units");
        assert_eq!(body["scope"]["user_id"], "u1");
    }
}
