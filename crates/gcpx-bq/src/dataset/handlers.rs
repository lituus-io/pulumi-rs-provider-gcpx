// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

use pulumi_rs_yaml_proto::pulumirpc;
use tonic::{Response, Status};

use crate::dataset::api_body::{build_create_body, build_patch_body};
use crate::dataset::diff::compute_dataset_diff;
use crate::dataset::parse::{build_dataset_output, parse_dataset_inputs};
use crate::dataset::validate::validate_dataset;
use crate::ops::BqOps;
use crate::types::DatasetMeta;
use gcpx_core::error::IntoStatus;
use gcpx_core::handler_util::{build_check_response, build_diff_response};
use gcpx_core::lifecycle::create_or_adopt;
use gcpx_core::prost_util::parse_resource_id_2;

pub async fn check_dataset<C: BqOps>(
    // Validation and diffing are pure: no client needed, but the signature
    // stays uniform so dispatch can treat every handler alike.
    _client: &C,
    req: pulumirpc::CheckRequest,
) -> Result<Response<pulumirpc::CheckResponse>, Status> {
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;
    let failures = validate_dataset(&inputs);

    build_check_response(req.news, failures)
}

pub async fn diff_dataset<C: BqOps>(
    // Validation and diffing are pure: no client needed, but the signature
    // stays uniform so dispatch can treat every handler alike.
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

    let old_inputs = parse_dataset_inputs(olds).map_err(Status::internal)?;
    let new_inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;

    let diff = compute_dataset_diff(&old_inputs, &new_inputs);

    Ok(build_diff_response(&diff))
}

pub async fn create_dataset<C: BqOps>(
    client: &C,
    req: pulumirpc::CreateRequest,
) -> Result<Response<pulumirpc::CreateResponse>, Status> {
    let props = req
        .properties
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing properties"))?;

    let inputs = parse_dataset_inputs(props).map_err(Status::invalid_argument)?;
    let id = format!("{}/{}", inputs.project, inputs.dataset_id);

    let meta = if !req.preview {
        let body = build_create_body(&inputs);
        let all_keys: &[&str] = &[
            "description",
            "friendlyName",
            "labels",
            "defaultTableExpirationMs",
            "defaultPartitionExpirationMs",
            "storageBillingModel",
            "maxTimeTravelHours",
        ];
        let patch = build_patch_body(&inputs, all_keys);
        create_or_adopt(
            client.create_dataset(inputs.project, &body),
            || client.patch_dataset(inputs.project, inputs.dataset_id, &patch),
            "dataset",
        )
        .await?
    } else {
        DatasetMeta::preview(
            inputs.dataset_id,
            inputs.location,
            inputs.storage_billing_model,
        )
    };

    let outputs = build_dataset_output(&inputs, &meta);

    Ok(Response::new(pulumirpc::CreateResponse {
        id,
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn read_dataset<C: BqOps>(
    client: &C,
    req: pulumirpc::ReadRequest,
) -> Result<Response<pulumirpc::ReadResponse>, Status> {
    let (proj, ds_id) = parse_resource_id_2(&req.id).map_err(Status::invalid_argument)?;

    let meta = client.get_dataset(proj, ds_id).await.status_internal()?;

    let inputs = crate::dataset::types::DatasetInputs {
        project: proj,
        dataset_id: ds_id,
        location: &meta.location,
        description: if meta.description.is_empty() {
            None
        } else {
            Some(&meta.description)
        },
        friendly_name: if meta.friendly_name.is_empty() {
            None
        } else {
            Some(&meta.friendly_name)
        },
        labels: meta
            .labels
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect(),
        default_table_expiration_ms: meta.default_table_expiration_ms,
        default_partition_expiration_ms: meta.default_partition_expiration_ms,
        storage_billing_model: if meta.storage_billing_model.is_empty() {
            None
        } else {
            Some(&meta.storage_billing_model)
        },
        max_time_travel_hours: meta.max_time_travel_hours,
    };

    let outputs = build_dataset_output(&inputs, &meta);
    let inputs_out = outputs.clone();

    Ok(Response::new(pulumirpc::ReadResponse {
        id: req.id,
        properties: Some(outputs),
        inputs: Some(inputs_out),
        ..Default::default()
    }))
}

pub async fn update_dataset<C: BqOps>(
    client: &C,
    req: pulumirpc::UpdateRequest,
) -> Result<Response<pulumirpc::UpdateResponse>, Status> {
    let olds = req
        .olds
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing olds"))?;
    let news = req
        .news
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("missing news"))?;

    let old_inputs = parse_dataset_inputs(olds).map_err(Status::internal)?;
    let new_inputs = parse_dataset_inputs(news).map_err(Status::invalid_argument)?;

    let diff = compute_dataset_diff(&old_inputs, &new_inputs);

    let meta = if !req.preview && !diff.update_keys.is_empty() {
        let body = build_patch_body(&new_inputs, &diff.update_keys);
        client
            .patch_dataset(new_inputs.project, new_inputs.dataset_id, &body)
            .await
            .status_internal()?
    } else {
        DatasetMeta::preview(
            new_inputs.dataset_id,
            new_inputs.location,
            new_inputs.storage_billing_model,
        )
    };

    let outputs = build_dataset_output(&new_inputs, &meta);

    Ok(Response::new(pulumirpc::UpdateResponse {
        properties: Some(outputs),
        ..Default::default()
    }))
}

pub async fn delete_dataset<C: BqOps>(
    client: &C,
    req: pulumirpc::DeleteRequest,
) -> Result<Response<()>, Status> {
    let (proj, ds_id) = parse_resource_id_2(&req.id).map_err(Status::invalid_argument)?;

    // BigQuery refuses to delete a dataset that still holds tables unless the
    // cascade is requested. Defaulting this to false matches the rest of the
    // ecosystem and keeps `pulumi destroy` from silently dropping data the
    // stack does not manage.
    let delete_contents = req
        .old_inputs
        .as_ref()
        .or(req.properties.as_ref())
        .and_then(|s| gcpx_core::prost_util::get_bool(&s.fields, "deleteContentsOnDestroy"))
        .unwrap_or(false);

    match gcpx_core::lifecycle::verified_delete(
        client.delete_dataset(proj, ds_id, delete_contents),
        || client.get_dataset(proj, ds_id),
        10,
        std::time::Duration::from_secs(1),
    )
    .await
    {
        Ok(resp) => Ok(resp),
        // The raw API error here says the dataset "still contains tables",
        // which is true but does not say what to do about it.
        Err(status) if !delete_contents && mentions_non_empty(status.message()) => {
            Err(Status::failed_precondition(format!(
                "dataset '{ds_id}' still contains tables, so it was not deleted. \
                 Either delete those resources first, or set \
                 'deleteContentsOnDestroy: true' on this Dataset to have the \
                 provider remove them with it."
            )))
        }
        Err(status) => Err(status),
    }
}

/// Whether an API message is BigQuery refusing to drop a non-empty dataset.
fn mentions_non_empty(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("still contains") || m.contains("not empty") || m.contains("deletecontents")
}
