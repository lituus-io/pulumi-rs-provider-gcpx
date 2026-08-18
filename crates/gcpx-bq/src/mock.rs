// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! A recording [`BqOps`] double.
//!
//! Behind the `mock` feature rather than `#[cfg(test)]`: a `cfg(test)` item is
//! invisible outside its own crate, which is exactly why the predecessor's mock
//! could not be shared and every crate would have grown its own.
//!
//! Calls are recorded so tests can assert on *what was sent to BigQuery* —
//! the DDL text, the patch bodies — rather than only on the returned value.
//! For this provider the generated SQL is the product, so that is the assertion
//! that matters.

use std::future::Future;
use std::sync::Mutex;

use gcpx_core::error::GcpApiError;

use crate::ops::BqOps;
use crate::types::{BqField, BqTableMeta, DatasetMeta, DryRunResult, RoutineMeta};

#[derive(Debug, thiserror::Error)]
#[error("mock error: {0}")]
pub struct MockError(pub String);

impl GcpApiError for MockError {
    fn is_conflict(&self) -> bool {
        self.0.contains("409") || self.0.contains("conflict")
    }
    fn is_not_found(&self) -> bool {
        self.0.contains("404") || self.0.contains("not found")
    }
    fn is_rate_limited(&self) -> bool {
        self.0.contains("429") || self.0.contains("rate limit")
    }
    fn is_unauthenticated(&self) -> bool {
        self.0.contains("401")
    }
    fn http_status(&self) -> Option<u16> {
        [400u16, 401, 403, 404, 409, 429, 500, 503]
            .into_iter()
            .find(|code| self.0.contains(&code.to_string()))
    }
    fn api_message(&self) -> &str {
        &self.0
    }
}

#[derive(Default)]
pub struct MockBqClient {
    /// Every DDL statement executed, in order.
    pub ddl_log: Mutex<Vec<String>>,
    /// (operation, table_id) pairs for table calls.
    pub table_log: Mutex<Vec<(String, String)>>,
    pub dataset_log: Mutex<Vec<(String, String)>>,
    pub routine_log: Mutex<Vec<(String, String)>>,
    pub schema: Vec<BqField>,
    pub table_meta: Option<BqTableMeta>,
    pub dataset_meta: Option<DatasetMeta>,
    pub routine_meta: Option<RoutineMeta>,
    pub dry_run_result: Option<DryRunResult>,
    /// Name of the method that should fail, for error-path tests.
    pub fail_on: Mutex<Option<String>>,
}

impl MockBqClient {
    pub fn new(schema: Vec<BqField>) -> Self {
        Self {
            schema,
            ..Default::default()
        }
    }

    pub fn ddl_log(&self) -> Vec<String> {
        self.ddl_log.lock().unwrap().clone()
    }

    pub fn table_log(&self) -> Vec<(String, String)> {
        self.table_log.lock().unwrap().clone()
    }

    pub fn failing(method: &str) -> Self {
        Self {
            fail_on: Mutex::new(Some(method.to_owned())),
            ..Default::default()
        }
    }

    pub(crate) fn should_fail(&self, method: &str) -> bool {
        self.fail_on
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|m| m == method)
    }

    fn guard(&self, method: &'static str) -> Result<(), MockError> {
        if self.should_fail(method) {
            Err(MockError(format!("{method} failed")))
        } else {
            Ok(())
        }
    }
}

#[allow(
    clippy::manual_async_fn,
    reason = "several methods record the call eagerly, before the future is \
              returned, so that a dropped future still shows up in the log; \
              keeping every method the same shape makes that consistent"
)]
impl BqOps for MockBqClient {
    type Error = MockError;

    fn execute_ddl<'a>(
        &'a self,
        _project: &'a str,
        ddl: &'a str,
        _max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.ddl_log.lock().unwrap().push(ddl.to_owned());
        async move { self.guard("execute_ddl") }
    }

    fn get_table_schema<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _table_id: &'a str,
    ) -> impl Future<Output = Result<Vec<BqField>, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_table_schema")?;
            Ok(self.schema.clone())
        }
    }

    fn create_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), String::new()));
        async move {
            self.guard("create_table")?;
            Ok(self
                .table_meta
                .clone()
                .unwrap_or_else(|| BqTableMeta::preview("TABLE")))
        }
    }

    fn get_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_table")?;
            let mut meta = self
                .table_meta
                .clone()
                .unwrap_or_else(|| BqTableMeta::preview("TABLE"));
            meta.schema_fields.clone_from(&self.schema);
            let _ = table_id;
            Ok(meta)
        }
    }

    fn patch_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        table_id: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<BqTableMeta, Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), table_id.to_owned()));
        async move {
            self.guard("patch_table")?;
            Ok(self
                .table_meta
                .clone()
                .unwrap_or_else(|| BqTableMeta::preview("TABLE")))
        }
    }

    fn delete_table<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        table_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.table_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), table_id.to_owned()));
        async move { self.guard("delete_table") }
    }

    fn dry_run_query<'a>(
        &'a self,
        _project: &'a str,
        _sql: &'a str,
        _max_bytes_billed: Option<i64>,
    ) -> impl Future<Output = Result<DryRunResult, Self::Error>> + Send + 'a {
        async move {
            self.guard("dry_run_query")?;
            Ok(self.dry_run_result.clone().unwrap_or(DryRunResult {
                valid: true,
                error_message: None,
                total_bytes_processed: 0,
                schema: Vec::new(),
            }))
        }
    }

    fn create_dataset<'a>(
        &'a self,
        _project: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), String::new()));
        async move {
            self.guard("create_dataset")?;
            Ok(self
                .dataset_meta
                .clone()
                .unwrap_or_else(|| DatasetMeta::preview("ds", "US", None)))
        }
    }

    fn get_dataset<'a>(
        &'a self,
        _project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_dataset")?;
            Ok(self
                .dataset_meta
                .clone()
                .unwrap_or_else(|| DatasetMeta::preview(dataset_id, "US", None)))
        }
    }

    fn patch_dataset<'a>(
        &'a self,
        _project: &'a str,
        dataset_id: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<DatasetMeta, Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("patch".to_owned(), dataset_id.to_owned()));
        async move {
            self.guard("patch_dataset")?;
            Ok(self
                .dataset_meta
                .clone()
                .unwrap_or_else(|| DatasetMeta::preview(dataset_id, "US", None)))
        }
    }

    fn delete_dataset<'a>(
        &'a self,
        _project: &'a str,
        dataset_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.dataset_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), dataset_id.to_owned()));
        async move { self.guard("delete_dataset") }
    }

    fn create_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("create".to_owned(), String::new()));
        async move {
            self.guard("create_routine")?;
            Ok(self
                .routine_meta
                .clone()
                .unwrap_or_else(|| RoutineMeta::preview("r", "SCALAR_FUNCTION", "SQL")))
        }
    }

    fn get_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        async move {
            self.guard("get_routine")?;
            Ok(self
                .routine_meta
                .clone()
                .unwrap_or_else(|| RoutineMeta::preview(routine_id, "SCALAR_FUNCTION", "SQL")))
        }
    }

    fn update_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        routine_id: &'a str,
        _body: &'a serde_json::Value,
    ) -> impl Future<Output = Result<RoutineMeta, Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("update".to_owned(), routine_id.to_owned()));
        async move {
            self.guard("update_routine")?;
            Ok(self
                .routine_meta
                .clone()
                .unwrap_or_else(|| RoutineMeta::preview(routine_id, "SCALAR_FUNCTION", "SQL")))
        }
    }

    fn delete_routine<'a>(
        &'a self,
        _project: &'a str,
        _dataset: &'a str,
        routine_id: &'a str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.routine_log
            .lock()
            .unwrap()
            .push(("delete".to_owned(), routine_id.to_owned()));
        async move { self.guard("delete_routine") }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_executed_ddl_in_order() {
        let c = MockBqClient::default();
        c.execute_ddl("p", "ALTER TABLE a", None).await.unwrap();
        c.execute_ddl("p", "ALTER TABLE b", None).await.unwrap();
        assert_eq!(c.ddl_log(), vec!["ALTER TABLE a", "ALTER TABLE b"]);
    }

    #[tokio::test]
    async fn fail_on_targets_a_single_method() {
        let c = MockBqClient::failing("create_table");
        assert!(c
            .create_table("p", "d", &serde_json::json!({}))
            .await
            .is_err());
        assert!(c.get_table("p", "d", "t").await.is_ok());
    }

    #[tokio::test]
    async fn ddl_is_recorded_even_when_execution_fails() {
        // Error-path tests still need to assert on what would have been sent.
        let c = MockBqClient::failing("execute_ddl");
        let _ = c.execute_ddl("p", "DROP TABLE x", None).await;
        assert_eq!(c.ddl_log(), vec!["DROP TABLE x"]);
    }

    #[test]
    fn mock_error_classifies_by_embedded_status() {
        assert!(MockError("409 conflict".into()).is_conflict());
        assert!(MockError("404 not found".into()).is_not_found());
        assert!(MockError("429 rate limit".into()).is_rate_limited());
        assert_eq!(MockError("403 denied".into()).http_status(), Some(403));
    }
}
