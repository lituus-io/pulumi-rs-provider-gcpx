// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The Pulumi resource provider: schema, dispatch, and the gRPC surface.
//!
//! Every resource's logic lives in its own crate as free functions. This crate
//! owns only the mapping from a resource URN to one of those functions, so
//! adding a resource means adding a row to [`dispatch!`] rather than touching
//! anything that already works.

use pulumi_rs_yaml_proto::pulumirpc;
use pulumi_rs_yaml_proto::pulumirpc::resource_provider_server::ResourceProvider;
use tonic::{Request, Response, Status};

use gcpx_agents::{DataAgentOps, VertexAgentOps};
use gcpx_bq::BqOps;
use gcpx_core::prost_util::resource_type_from_urn;
use gcpx_scheduler::SchedulerOps;

pub mod schema;

/// Everything a client must be able to do for the provider to serve every
/// resource. One concrete client implements all four; the bound is spelled out
/// so a partial client is a compile error rather than a runtime surprise.
pub trait GcpxClient: BqOps + SchedulerOps + DataAgentOps + VertexAgentOps {}
impl<T: BqOps + SchedulerOps + DataAgentOps + VertexAgentOps> GcpxClient for T {}

pub struct GcpxProvider<C> {
    pub client: C,
}

impl<C> GcpxProvider<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }
}

// Resource tokens. These are the public contract: they appear in every user's
// stack file and in their state, so they are never renamed.
const TABLE_SCHEMA: &str = "gcpx:bigquery/tableSchema:TableSchema";
const TABLE: &str = "gcpx:bigquery/table:Table";
const DATASET: &str = "gcpx:bigquery/dataset:Dataset";
const ROUTINE: &str = "gcpx:bigquery/routineFunction:RoutineFunction";
const DBT_PROJECT: &str = "gcpx:dbt/project:Project";
const DBT_MODEL: &str = "gcpx:dbt/model:Model";
const DBT_MACRO: &str = "gcpx:dbt/macro:Macro";
const DBT_SNAPSHOT: &str = "gcpx:dbt/snapshot:Snapshot";
const SQL_JOB: &str = "gcpx:scheduler/sqlJob:SqlJob";
const INGEST_JOB: &str = "gcpx:dataproc/ingestJob:IngestJob";
const EXPORT_JOB: &str = "gcpx:dataproc/exportJob:ExportJob";
const DATA_AGENT: &str = "gcpx:agent/dataAgent:DataAgent";
const AGENT_IAM: &str = "gcpx:agent/dataAgentIamPolicy:DataAgentIamPolicy";
const CONVERSATION: &str = "gcpx:agent/conversation:Conversation";
const AGENT_ENGINE: &str = "gcpx:agent/agentEngine:AgentEngine";
const MEMORY: &str = "gcpx:agent/memory:Memory";

/// Route a request to the handler for its resource type.
macro_rules! dispatch {
    ($self:expr, $req:expr, $( $type_const:ident => $module:path ),+ $(,)?) => {{
        let resource_type = resource_type_from_urn(&$req.urn);
        match resource_type {
            $( $type_const => $module(&$self.client, $req).await, )+
            other => Err(Status::not_found(format!(
                "unknown resource type '{other}'. This provider serves the gcpx:* types listed \
                 in its schema; check the type token for a typo."
            ))),
        }
    }};
}

#[tonic::async_trait]
impl<C: GcpxClient> ResourceProvider for GcpxProvider<C> {
    async fn handshake(
        &self,
        _request: Request<pulumirpc::ProviderHandshakeRequest>,
    ) -> Result<Response<pulumirpc::ProviderHandshakeResponse>, Status> {
        Ok(Response::new(pulumirpc::ProviderHandshakeResponse {
            accept_secrets: true,
            accept_resources: true,
            accept_outputs: false,
            supports_autonaming_configuration: false,
        }))
    }

    async fn get_schema(
        &self,
        _request: Request<pulumirpc::GetSchemaRequest>,
    ) -> Result<Response<pulumirpc::GetSchemaResponse>, Status> {
        Ok(Response::new(pulumirpc::GetSchemaResponse {
            schema: schema::schema_json().to_owned(),
        }))
    }

    async fn check_config(
        &self,
        request: Request<pulumirpc::CheckRequest>,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let req = request.into_inner();
        Ok(Response::new(pulumirpc::CheckResponse {
            inputs: req.news,
            failures: vec![],
        }))
    }

    async fn diff_config(
        &self,
        _request: Request<pulumirpc::DiffRequest>,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        Ok(Response::new(pulumirpc::DiffResponse::default()))
    }

    async fn configure(
        &self,
        _request: Request<pulumirpc::ConfigureRequest>,
    ) -> Result<Response<pulumirpc::ConfigureResponse>, Status> {
        Ok(Response::new(pulumirpc::ConfigureResponse {
            accept_secrets: true,
            supports_preview: true,
            accept_resources: true,
            accept_outputs: false,
            supports_autonaming_configuration: false,
        }))
    }

    async fn check(
        &self,
        request: Request<pulumirpc::CheckRequest>,
    ) -> Result<Response<pulumirpc::CheckResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::check_table_schema,
            TABLE        => gcpx_bq::table::handlers::check_table,
            DATASET      => gcpx_bq::dataset::handlers::check_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::check_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::check_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::check_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::check_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::check_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::check_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::check_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::check_export_job,
            DATA_AGENT   => gcpx_agents::handlers::check_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::check_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::check_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::check_agent_engine,
            MEMORY       => gcpx_agents::handlers::check_memory,
        )
    }

    async fn diff(
        &self,
        request: Request<pulumirpc::DiffRequest>,
    ) -> Result<Response<pulumirpc::DiffResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::diff_table_schema,
            TABLE        => gcpx_bq::table::handlers::diff_table,
            DATASET      => gcpx_bq::dataset::handlers::diff_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::diff_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::diff_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::diff_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::diff_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::diff_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::diff_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::diff_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::diff_export_job,
            DATA_AGENT   => gcpx_agents::handlers::diff_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::diff_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::diff_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::diff_agent_engine,
            MEMORY       => gcpx_agents::handlers::diff_memory,
        )
    }

    async fn create(
        &self,
        request: Request<pulumirpc::CreateRequest>,
    ) -> Result<Response<pulumirpc::CreateResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::create_table_schema,
            TABLE        => gcpx_bq::table::handlers::create_table,
            DATASET      => gcpx_bq::dataset::handlers::create_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::create_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::create_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::create_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::create_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::create_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::create_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::create_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::create_export_job,
            DATA_AGENT   => gcpx_agents::handlers::create_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::create_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::create_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::create_agent_engine,
            MEMORY       => gcpx_agents::handlers::create_memory,
        )
    }

    async fn read(
        &self,
        request: Request<pulumirpc::ReadRequest>,
    ) -> Result<Response<pulumirpc::ReadResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::read_table_schema,
            TABLE        => gcpx_bq::table::handlers::read_table,
            DATASET      => gcpx_bq::dataset::handlers::read_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::read_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::read_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::read_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::read_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::read_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::read_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::read_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::read_export_job,
            DATA_AGENT   => gcpx_agents::handlers::read_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::read_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::read_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::read_agent_engine,
            MEMORY       => gcpx_agents::handlers::read_memory,
        )
    }

    async fn update(
        &self,
        request: Request<pulumirpc::UpdateRequest>,
    ) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::update_table_schema,
            TABLE        => gcpx_bq::table::handlers::update_table,
            DATASET      => gcpx_bq::dataset::handlers::update_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::update_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::update_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::update_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::update_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::update_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::update_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::update_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::update_export_job,
            DATA_AGENT   => gcpx_agents::handlers::update_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::update_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::update_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::update_agent_engine,
            MEMORY       => gcpx_agents::handlers::update_memory,
        )
    }

    async fn delete(
        &self,
        request: Request<pulumirpc::DeleteRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        dispatch!(self, req,
            TABLE_SCHEMA => gcpx_bq::schema::handlers::delete_table_schema,
            TABLE        => gcpx_bq::table::handlers::delete_table,
            DATASET      => gcpx_bq::dataset::handlers::delete_dataset,
            ROUTINE      => gcpx_bq::routine::handlers::delete_routine,
            DBT_PROJECT  => gcpx_dbt::handlers::delete_dbt_project,
            DBT_MODEL    => gcpx_dbt::handlers::delete_dbt_model,
            DBT_MACRO    => gcpx_dbt::handlers::delete_dbt_macro,
            DBT_SNAPSHOT => gcpx_snapshot::handlers::delete_snapshot,
            SQL_JOB      => gcpx_scheduler::handlers::delete_sql_job,
            INGEST_JOB   => gcpx_dataproc::ingest::delete_ingest_job,
            EXPORT_JOB   => gcpx_dataproc::export::delete_export_job,
            DATA_AGENT   => gcpx_agents::handlers::delete_data_agent,
            AGENT_IAM    => gcpx_agents::handlers::delete_agent_iam_policy,
            CONVERSATION => gcpx_agents::handlers::delete_conversation,
            AGENT_ENGINE => gcpx_agents::handlers::delete_agent_engine,
            MEMORY       => gcpx_agents::handlers::delete_memory,
        )
    }

    // ── Deliberately unimplemented ──────────────────────────────────────────
    //
    // These are decisions, not gaps. Each returns `unimplemented` with a reason,
    // so a caller reaching one learns why rather than filing a bug.

    async fn parameterize(
        &self,
        _request: Request<pulumirpc::ParameterizeRequest>,
    ) -> Result<Response<pulumirpc::ParameterizeResponse>, Status> {
        Err(Status::unimplemented(
            "this provider is not parameterized: its resource set is fixed at build time",
        ))
    }

    async fn invoke(
        &self,
        _request: Request<pulumirpc::InvokeRequest>,
    ) -> Result<Response<pulumirpc::InvokeResponse>, Status> {
        Err(Status::unimplemented(
            "this provider exposes no functions; every capability is a resource",
        ))
    }

    async fn call(
        &self,
        _request: Request<pulumirpc::CallRequest>,
    ) -> Result<Response<pulumirpc::CallResponse>, Status> {
        Err(Status::unimplemented(
            "method calls apply to component resources, which this provider does not define",
        ))
    }

    async fn construct(
        &self,
        _request: Request<pulumirpc::ConstructRequest>,
    ) -> Result<Response<pulumirpc::ConstructResponse>, Status> {
        Err(Status::unimplemented(
            "this provider defines only custom resources, not components",
        ))
    }

    async fn cancel(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_plugin_info(
        &self,
        _request: Request<()>,
    ) -> Result<Response<pulumirpc::PluginInfo>, Status> {
        Ok(Response::new(pulumirpc::PluginInfo {
            // Read from the crate rather than written out by hand. A hardcoded
            // version drifts from the manifests, and the packaging that
            // installs this plugin resolves it by exactly this string.
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }))
    }

    async fn attach(
        &self,
        _request: Request<pulumirpc::PluginAttach>,
    ) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }

    async fn get_mapping(
        &self,
        _request: Request<pulumirpc::GetMappingRequest>,
    ) -> Result<Response<pulumirpc::GetMappingResponse>, Status> {
        Ok(Response::new(pulumirpc::GetMappingResponse::default()))
    }

    async fn get_mappings(
        &self,
        _request: Request<pulumirpc::GetMappingsRequest>,
    ) -> Result<Response<pulumirpc::GetMappingsResponse>, Status> {
        Ok(Response::new(pulumirpc::GetMappingsResponse::default()))
    }
}

/// Every resource type this provider serves.
///
/// Used by the schema test to prove that dispatch and the published schema
/// agree: a type in one and not the other is either an unreachable resource or
/// an undocumented one.
pub const ALL_RESOURCE_TYPES: &[&str] = &[
    TABLE_SCHEMA,
    TABLE,
    DATASET,
    ROUTINE,
    DBT_PROJECT,
    DBT_MODEL,
    DBT_MACRO,
    DBT_SNAPSHOT,
    SQL_JOB,
    INGEST_JOB,
    EXPORT_JOB,
    DATA_AGENT,
    AGENT_IAM,
    CONVERSATION,
    AGENT_ENGINE,
    MEMORY,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_tokens_are_well_formed() {
        // These strings appear in every user's stack file and state, so a typo
        // is a breaking change that no amount of testing elsewhere catches.
        for token in ALL_RESOURCE_TYPES {
            let parts: Vec<&str> = token.split(':').collect();
            assert_eq!(parts.len(), 3, "{token} should be pkg:module:Type");
            assert_eq!(parts[0], "gcpx", "{token} must be in the gcpx package");
            assert!(
                parts[2].starts_with(|c: char| c.is_ascii_uppercase()),
                "{token} type name should be capitalised"
            );
        }
    }

    #[test]
    fn resource_tokens_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for token in ALL_RESOURCE_TYPES {
            assert!(seen.insert(token), "duplicate resource token {token}");
        }
    }

    #[test]
    fn the_eleven_predecessor_types_are_all_still_served() {
        // Parity contract: dropping one of these silently breaks every stack
        // that uses it.
        for token in [
            "gcpx:bigquery/tableSchema:TableSchema",
            "gcpx:bigquery/table:Table",
            "gcpx:bigquery/dataset:Dataset",
            "gcpx:bigquery/routineFunction:RoutineFunction",
            "gcpx:dbt/project:Project",
            "gcpx:dbt/model:Model",
            "gcpx:dbt/macro:Macro",
            "gcpx:dbt/snapshot:Snapshot",
            "gcpx:scheduler/sqlJob:SqlJob",
            "gcpx:dataproc/ingestJob:IngestJob",
            "gcpx:dataproc/exportJob:ExportJob",
        ] {
            assert!(
                ALL_RESOURCE_TYPES.contains(&token),
                "{token} was dropped in the migration"
            );
        }
    }

    #[test]
    fn plugin_version_matches_the_crate() {
        assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0");
    }
}
