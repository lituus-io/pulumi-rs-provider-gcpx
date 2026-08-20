// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::ops::BqOps;
use crate::routine::api_body::{build_create_body, build_update_body};
use crate::routine::diff::compute_routine_diff;
use crate::routine::parse::{build_routine_output, parse_routine_inputs};
use crate::routine::validate::validate_routine;
use crate::types::RoutineMeta;
use gcpx_core::error::IntoStatus;
use gcpx_core::handler_util::{build_check_response, build_diff_response};
use gcpx_core::lifecycle::create_or_adopt;

pub async fn check_routine<C: BqOps>(
    // Validation and diffing are pure: no client needed, but the signature
    // stays uniform so dispatch can treat every handler alike.
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;
    let failures = validate_routine(&inputs);

    build_check_response(req.news, failures)
}

pub async fn diff_routine<C: BqOps>(
    // Validation and diffing are pure: no client needed, but the signature
    // stays uniform so dispatch can treat every handler alike.
    _client: &C,
    req: pulumirpc::DiffRequest,
) -> Result<Response<pulumirpc::DiffResponse>, Status> {
    // Compare inputs with inputs. `olds` is what the provider stored, which
    // includes fields the service assigns and defaults it applied — none of
    // which an incoming input can match, so comparing against it reports a
    // change on every preview forever. `old_inputs` is what the engine provides
    // for exactly this; the outputs remain the fallback for state written
    // before it existed.
    let prev =
        gcpx_core::prost_util::old_inputs_or_outputs(req.old_inputs.as_ref(), req.olds.as_ref())
            .ok_or_else(|| Status::invalid_argument("missing olds"))?;
    let olds = &prev;
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let old_inputs = parse_routine_inputs(olds).map_err(Status::internal)?;
    let new_inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;

    let diff = compute_routine_diff(&old_inputs, &new_inputs);

    Ok(build_diff_response(&diff))
}

pub async fn create_routine<C: BqOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;

    let inputs = parse_routine_inputs(props).map_err(Status::invalid_argument)?;
    let id = format!(
        "{}/{}/{}",
        inputs.project, inputs.dataset, inputs.routine_id
    );

    let meta = if !req.preview {
        let body = build_create_body(&inputs);
        let update_body = build_update_body(&inputs);
        create_or_adopt(
            client.create_routine(inputs.project, inputs.dataset, &body),
            || {
                client.update_routine(
                    inputs.project,
                    inputs.dataset,
                    inputs.routine_id,
                    &update_body,
                )
            },
            "routine",
        )
        .await?
    } else {
        RoutineMeta::preview(inputs.routine_id, inputs.routine_type, inputs.language)
    };

    let outputs = build_routine_output(&inputs, &meta);

    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn read_routine<C: BqOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (proj, ds, rid) =
        gcpx_core::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

    let meta = client.get_routine(proj, ds, rid).await.status_internal()?;

    // Build minimal inputs from read metadata.
    let inputs = crate::routine::types::RoutineInputs {
        project: proj,
        dataset: ds,
        routine_id: rid,
        routine_type: &meta.routine_type,
        language: &meta.language,
        definition_body: "",
        description: None,
        arguments: vec![],
        return_type: None,
        imported_libraries: vec![],
        determinism_level: None,
    };

    let outputs = build_routine_output(&inputs, &meta);
    let inputs_out = outputs.clone();

    Ok(Response::new(pulumirpc::ReadResponse {
        id: req.id,
        properties: Some(outputs),
        inputs: Some(inputs_out),
        ..Default::default()
    }))
}

pub async fn update_routine<C: BqOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let new_inputs = parse_routine_inputs(news).map_err(Status::invalid_argument)?;

    let meta = if !req.preview {
        let body = build_update_body(&new_inputs);
        client
            .update_routine(
                new_inputs.project,
                new_inputs.dataset,
                new_inputs.routine_id,
                &body,
            )
            .await
            .status_internal()?
    } else {
        RoutineMeta::preview(
            new_inputs.routine_id,
            new_inputs.routine_type,
            new_inputs.language,
        )
    };

    let outputs = build_routine_output(&new_inputs, &meta);

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn delete_routine<C: BqOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (proj, ds, rid) =
        gcpx_core::prost_util::parse_resource_id(&req.id).map_err(Status::invalid_argument)?;

    gcpx_core::lifecycle::verified_delete(
        client.delete_routine(proj, ds, rid),
        || client.get_routine(proj, ds, rid),
        10,
        std::time::Duration::from_secs(1),
    )
    .await
}
