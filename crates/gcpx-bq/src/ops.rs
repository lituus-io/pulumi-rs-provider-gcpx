// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The BigQuery operation surface, and its HTTP implementation.
//!
//! # Why `impl Future` rather than hand-written GATs
//!
//! Every method returns `impl Future` in return position (RPITIT). The compiler
//! desugars that into precisely the generic associated type one would otherwise
//! write by hand — the future is anonymous, but it is a GAT, and it is not
//! boxed. Naming those futures explicitly would need `impl Trait` in associated
//! type position, which is still unstable (rust-lang/rust#63063); the only
//! stable alternative is `Box<dyn Future>`, which trades the whole benefit for
//! a heap allocation and a vtable on every call. So RPITIT is not a shortcut
//! around GATs here — on stable it is the GAT.
//!
//! The `'a` lifetimes are load-bearing: they let callers pass borrowed strings
//! straight out of the Pulumi property map without an intervening `to_owned()`.

use std::future::Future;

use gcpx_core::auth::CredentialSource;
use gcpx_core::breaker::Service;
use gcpx_core::error::{GcpApiError, GcpError};
use gcpx_core::http::HttpGcpClient;
use gcpx_core::sanitize::encode_path_segment as e;

use crate::types::{
    convert_bq_fields, convert_dataset_response, convert_routine_response, convert_table_response,
    BqDatasetResponse, BqDryRunResponse, BqField, BqRoutineResponse, BqTableMeta, BqTableResponse,
    DatasetMeta, DryRunResult, RoutineMeta,
};

const BQ_API: &str = "https://bigquery.googleapis.com/bigquery/v2";

/// BigQuery operations, generic over implementation so handlers can be tested
/// against a double without a network.
pub trait BqOps: Send + Sync + 'static {
    type Error: GcpApiError;

    fn execute_ddl<'a>(
        &'a self,
        project: &'a str,
        ddl: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn get_table_schema<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<Vec<BqField>, Self::Error>> + Send + 'a;

    fn create_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn get_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn patch_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a;

    fn delete_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn dry_run_query<'a>(
        &'a self,
        project: &'a str,
        sql: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<DryRunResult, Self::Error>> + Send + 'a;

    fn create_dataset<'a>(
        &'a self,
        project: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    fn get_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    fn patch_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a;

    /// Delete a dataset.
    ///
    /// `delete_contents` maps to BigQuery's `deleteContents`, which is required
    /// to delete a dataset that still holds tables. Without it the API refuses,
    /// and since a dataset is usually the parent of the tables and models in
    /// the same stack, that is the ordinary case rather than an edge one.
    fn delete_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        delete_contents: bool,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    fn create_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn get_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn update_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a;

    fn delete_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Implemented here rather than in `gcpx-core` so that crate stays free of
/// BigQuery knowledge. Rust's orphan rule permits it because `BqOps` is local.
impl<C: CredentialSource> BqOps for HttpGcpClient<C> {
    type Error = GcpError;

    async fn execute_ddl<'a>(
        &'a self,
        project: &'a str,
        ddl: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> Result<(), Self::Error> {
        let url = format!("{BQ_API}/projects/{}/queries", e(project));
        let body = query_body(ddl, false, max_bytes_billed);
        let _: serde_json::Value = self.post_json(Service::BigQuery, &url, &body).await?;
        Ok(())
    }

    async fn get_table_schema<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<Vec<BqField>, Self::Error> {
        let table: BqTableResponse = self
            .get_json(Service::BigQuery, &table_url(project, dataset, table_id))
            .await?;
        Ok(table
            .schema
            .map(|s| convert_bq_fields(s.fields))
            .unwrap_or_default())
    }

    async fn create_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<BqTableMeta, Self::Error> {
        let url = format!(
            "{BQ_API}/projects/{}/datasets/{}/tables",
            e(project),
            e(dataset)
        );
        let table: BqTableResponse = self.post_json(Service::BigQuery, &url, body).await?;
        Ok(convert_table_response(table))
    }

    async fn get_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<BqTableMeta, Self::Error> {
        let table: BqTableResponse = self
            .get_json(Service::BigQuery, &table_url(project, dataset, table_id))
            .await?;
        Ok(convert_table_response(table))
    }

    async fn patch_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<BqTableMeta, Self::Error> {
        let table: BqTableResponse = self
            .patch_json(
                Service::BigQuery,
                &table_url(project, dataset, table_id),
                body,
            )
            .await?;
        Ok(convert_table_response(table))
    }

    async fn delete_table<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        table_id: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(Service::BigQuery, &table_url(project, dataset, table_id))
            .await
    }

    async fn dry_run_query<'a>(
        &'a self,
        project: &'a str,
        sql: &'a str,
        max_bytes_billed: Option<i64>,
    ) -> Result<DryRunResult, Self::Error> {
        let url = format!("{BQ_API}/projects/{}/queries", e(project));
        let body = query_body(sql, true, max_bytes_billed);

        // A dry run answers "is this SQL valid, and how much would it scan?".
        // An invalid query is a legitimate answer to that question, not a
        // transport failure, so a 4xx becomes `valid: false` and is reported
        // back to the user as a validation message rather than an error.
        match self
            .post_json::<BqDryRunResponse>(Service::BigQuery, &url, &body)
            .await
        {
            Ok(dr) => Ok(DryRunResult {
                valid: true,
                error_message: None,
                total_bytes_processed: dr
                    .statistics
                    .and_then(|s| s.total_bytes_processed.parse().ok())
                    .unwrap_or(0),
                schema: dr
                    .schema
                    .map(|s| convert_bq_fields(s.fields))
                    .unwrap_or_default(),
            }),
            Err(GcpError::Api { status, message }) if (400..500).contains(&status) => {
                Ok(DryRunResult {
                    valid: false,
                    error_message: Some(gcpx_core::error::redact(&message)),
                    total_bytes_processed: 0,
                    schema: Vec::new(),
                })
            }
            Err(e) => Err(e),
        }
    }

    async fn create_dataset<'a>(
        &'a self,
        project: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<DatasetMeta, Self::Error> {
        let url = format!("{BQ_API}/projects/{}/datasets", e(project));
        let ds: BqDatasetResponse = self.post_json(Service::BigQuery, &url, body).await?;
        Ok(convert_dataset_response(ds))
    }

    async fn get_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
    ) -> Result<DatasetMeta, Self::Error> {
        let ds: BqDatasetResponse = self
            .get_json(Service::BigQuery, &dataset_url(project, dataset_id))
            .await?;
        Ok(convert_dataset_response(ds))
    }

    async fn patch_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<DatasetMeta, Self::Error> {
        let ds: BqDatasetResponse = self
            .patch_json(Service::BigQuery, &dataset_url(project, dataset_id), body)
            .await?;
        Ok(convert_dataset_response(ds))
    }

    async fn delete_dataset<'a>(
        &'a self,
        project: &'a str,
        dataset_id: &'a str,
        delete_contents: bool,
    ) -> Result<(), Self::Error> {
        let mut url = dataset_url(project, dataset_id);
        if delete_contents {
            url.push_str("?deleteContents=true");
        }
        self.delete_ok(Service::BigQuery, &url).await
    }

    async fn create_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<RoutineMeta, Self::Error> {
        let url = format!(
            "{BQ_API}/projects/{}/datasets/{}/routines",
            e(project),
            e(dataset)
        );
        let r: BqRoutineResponse = self.post_json(Service::BigQuery, &url, body).await?;
        Ok(convert_routine_response(r))
    }

    async fn get_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> Result<RoutineMeta, Self::Error> {
        let r: BqRoutineResponse = self
            .get_json(
                Service::BigQuery,
                &routine_url(project, dataset, routine_id),
            )
            .await?;
        Ok(convert_routine_response(r))
    }

    async fn update_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
        body: &'a serde_json::Value,
    ) -> Result<RoutineMeta, Self::Error> {
        // Routines are replaced wholesale, not merged — BigQuery's routines
        // surface offers PUT, not PATCH.
        let r: BqRoutineResponse = self
            .put_json(
                Service::BigQuery,
                &routine_url(project, dataset, routine_id),
                body,
            )
            .await?;
        Ok(convert_routine_response(r))
    }

    async fn delete_routine<'a>(
        &'a self,
        project: &'a str,
        dataset: &'a str,
        routine_id: &'a str,
    ) -> Result<(), Self::Error> {
        self.delete_ok(
            Service::BigQuery,
            &routine_url(project, dataset, routine_id),
        )
        .await
    }
}

fn table_url(project: &str, dataset: &str, table_id: &str) -> String {
    format!(
        "{BQ_API}/projects/{}/datasets/{}/tables/{}",
        e(project),
        e(dataset),
        e(table_id)
    )
}

fn dataset_url(project: &str, dataset_id: &str) -> String {
    format!(
        "{BQ_API}/projects/{}/datasets/{}",
        e(project),
        e(dataset_id)
    )
}

fn routine_url(project: &str, dataset: &str, routine_id: &str) -> String {
    format!(
        "{BQ_API}/projects/{}/datasets/{}/routines/{}",
        e(project),
        e(dataset),
        e(routine_id)
    )
}

/// `maximumBytesBilled` is a string in the REST API, not a number — sending it
/// as a number is silently ignored, which removes the cost ceiling entirely.
fn query_body(sql: &str, dry_run: bool, max_bytes_billed: Option<i64>) -> serde_json::Value {
    let mut body = serde_json::json!({
        "query": sql,
        "useLegacySql": false,
    });
    if dry_run {
        body["dryRun"] = serde_json::Value::Bool(true);
    }
    if let Some(limit) = max_bytes_billed {
        body["maximumBytesBilled"] = serde_json::Value::String(limit.to_string());
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_percent_encode_path_segments() {
        // An identifier containing a slash would otherwise reshape the URL and
        // address a different resource than the one requested.
        let url = table_url("proj", "ds", "a/b");
        assert!(url.ends_with("/tables/a%2Fb"), "{url}");
        assert_eq!(url.matches("/tables/").count(), 1);
    }

    #[test]
    fn dataset_and_routine_urls_are_well_formed() {
        assert_eq!(
            dataset_url("p", "d"),
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d"
        );
        assert_eq!(
            routine_url("p", "d", "r"),
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d/routines/r"
        );
    }

    #[test]
    fn max_bytes_billed_is_serialised_as_a_string() {
        // BigQuery ignores a numeric maximumBytesBilled, which would silently
        // remove the user's cost ceiling.
        let body = query_body("SELECT 1", false, Some(1_000_000));
        assert_eq!(body["maximumBytesBilled"], serde_json::json!("1000000"));
        assert!(body["maximumBytesBilled"].is_string());
    }

    #[test]
    fn query_body_omits_absent_options() {
        let body = query_body("SELECT 1", false, None);
        assert!(body.get("maximumBytesBilled").is_none());
        assert!(body.get("dryRun").is_none());
        assert_eq!(body["useLegacySql"], serde_json::json!(false));
    }

    #[test]
    fn dry_run_flag_is_set_only_for_dry_runs() {
        assert_eq!(
            query_body("SELECT 1", true, None)["dryRun"],
            serde_json::json!(true)
        );
    }
}
