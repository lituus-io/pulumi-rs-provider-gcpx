// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Live GCP tests.
//!
//! These talk to a real project and are skipped unless one is configured, so
//! `cargo test` stays offline by default:
//!
//! ```text
//! GCPX_TEST_PROJECT=my-project \
//! GCPX_TEST_LOCATION=northamerica-northeast1 \
//!   cargo test --test gcp_live -- --test-threads=1 --nocapture
//! ```
//!
//! Every test names its resources uniquely and tears them down on the way out,
//! including when an assertion fails, so a run leaves nothing behind and can be
//! repeated immediately.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use gcpx_bq::types::BqField;
use gcpx_bq::BqOps;
use gcpx_core::auth::AdcCredentials;
use gcpx_core::http::HttpGcpClient;
use gcpx_core::sanitize::bq_table_ref;

type Client = HttpGcpClient<AdcCredentials>;

fn client() -> Client {
    HttpGcpClient::new(
        HttpGcpClient::<AdcCredentials>::default_http_client().expect("http client"),
        AdcCredentials::new(),
    )
}

/// The project under test, or `None` when the suite should be skipped.
fn project() -> Option<String> {
    std::env::var("GCPX_TEST_PROJECT")
        .ok()
        .filter(|p| !p.is_empty())
}

fn location() -> String {
    std::env::var("GCPX_TEST_LOCATION").unwrap_or_else(|_| "US".to_owned())
}

/// Unique per run *and* per call, so leftovers from a failed run can never
/// collide with the next one.
fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("gcpx_live_{prefix}_{ts}_{n}")
}

/// Skip with a visible reason rather than passing silently: a suite that
/// quietly does nothing is worse than one that fails.
macro_rules! require_project {
    () => {
        match project() {
            Some(p) => p,
            None => {
                eprintln!("SKIP: set GCPX_TEST_PROJECT to run the live suite");
                return;
            }
        }
    };
}

/// Create a dataset, run `body`, then always tear it down.
///
/// The first version of this suite discarded the teardown result and left three
/// datasets behind — the same defect the provider's model delete had, and
/// exactly what a live suite is supposed to catch. Teardown now cascades and
/// panics if it fails, and a failing assertion still cleans up before the
/// panic is re-raised.
async fn with_dataset<F, Fut>(project: &str, ds: &str, body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    client()
        .create_dataset(
            project,
            &serde_json::json!({
                "datasetReference": { "datasetId": ds, "projectId": project },
                "location": location(),
            }),
        )
        .await
        .expect("create_dataset");

    let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body())).await;

    match client().delete_dataset(project, ds, true).await {
        Ok(()) => println!("cleaned up dataset {ds}"),
        Err(e) => {
            if outcome.is_ok() {
                panic!("teardown failed for dataset {ds}: {e}");
            }
            eprintln!("teardown failed for dataset {ds}: {e}");
        }
    }

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn dataset_create_read_delete() {
    let project = require_project!();
    let c = client();
    let ds = unique("ds");

    let created = c
        .create_dataset(
            &project,
            &serde_json::json!({
                "datasetReference": { "datasetId": ds, "projectId": project },
                "location": location(),
            }),
        )
        .await
        .expect("create_dataset");
    assert_eq!(created.dataset_id, ds);
    println!("created dataset {ds} in {}", created.location);

    assert_eq!(
        c.get_dataset(&project, &ds)
            .await
            .expect("get_dataset")
            .dataset_id,
        ds
    );

    // An empty dataset needs no cascade.
    c.delete_dataset(&project, &ds, false)
        .await
        .expect("delete_dataset");

    assert!(
        c.get_dataset(&project, &ds).await.is_err(),
        "dataset should be gone after delete"
    );
}

/// Regression for the defect this suite found on its first run: BigQuery
/// refuses to drop a dataset that still holds tables, so a destroy without the
/// cascade fails. Since a dataset is usually the parent of the tables in the
/// same stack, that is the ordinary case rather than an edge one.
#[tokio::test]
async fn deleting_a_non_empty_dataset_requires_the_cascade() {
    let project = require_project!();
    let c = client();
    let ds = unique("ds");

    c.create_dataset(
        &project,
        &serde_json::json!({
            "datasetReference": { "datasetId": ds, "projectId": project },
            "location": location(),
        }),
    )
    .await
    .expect("create_dataset");

    c.create_table(
        &project,
        &ds,
        &serde_json::json!({
            "tableReference": { "projectId": project, "datasetId": ds, "tableId": "occupant" },
            "schema": { "fields": [{ "name": "id", "type": "INT64" }]},
        }),
    )
    .await
    .expect("create_table");

    let refused = c.delete_dataset(&project, &ds, false).await;
    assert!(
        refused.is_err(),
        "BigQuery should refuse to drop a dataset that still holds tables"
    );
    println!("without cascade: {}", refused.unwrap_err());

    c.delete_dataset(&project, &ds, true)
        .await
        .expect("cascade delete should succeed");
    assert!(c.get_dataset(&project, &ds).await.is_err());
}

#[tokio::test]
async fn schema_evolution_applies_alter_statements() {
    let project = require_project!();
    let ds = unique("ds");
    let table = "evolving";

    with_dataset(&project, &ds, || async {
        let c = client();
        c.create_table(
            &project,
            &ds,
            &serde_json::json!({
                "tableReference": { "projectId": project, "datasetId": ds, "tableId": table },
                "schema": { "fields": [
                    { "name": "id", "type": "INT64", "mode": "NULLABLE" },
                    { "name": "old_name", "type": "STRING", "mode": "NULLABLE" },
                    { "name": "doomed", "type": "STRING", "mode": "NULLABLE" },
                ]},
            }),
        )
        .await
        .expect("create_table");

        assert_eq!(
            c.get_table_schema(&project, &ds, table).await.unwrap().len(),
            3
        );

        // Exactly the statements the schema-evolution path generates.
        let table_ref = bq_table_ref(&project, &ds, table);
        for ddl in [
            format!("ALTER TABLE {table_ref} DROP COLUMN IF EXISTS `doomed`"),
            format!("ALTER TABLE {table_ref} RENAME COLUMN `old_name` TO `new_name`"),
            format!("ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS `added` STRING OPTIONS(description='added by the live suite')"),
        ] {
            c.execute_ddl(&project, &ddl, None)
                .await
                .unwrap_or_else(|e| panic!("DDL rejected: {ddl}\n{e}"));
        }

        let after = c.get_table_schema(&project, &ds, table).await.unwrap();
        let names: Vec<&str> = after.iter().map(|f| f.name.as_str()).collect();
        println!("schema after evolution: {names:?}");
        assert!(names.contains(&"new_name"), "rename did not apply");
        assert!(names.contains(&"added"), "add did not apply");
        assert!(!names.contains(&"doomed"), "drop did not apply");
        assert!(!names.contains(&"old_name"), "old column name survived");
    })
    .await;
}

#[tokio::test]
async fn drift_detection_sees_out_of_band_changes() {
    let project = require_project!();
    let ds = unique("ds");
    let table = "drifting";

    with_dataset(&project, &ds, || async {
        let c = client();
        c.create_table(
            &project,
            &ds,
            &serde_json::json!({
                "tableReference": { "projectId": project, "datasetId": ds, "tableId": table },
                "schema": { "fields": [{ "name": "id", "type": "INT64", "mode": "NULLABLE" }]},
            }),
        )
        .await
        .expect("create_table");

        // Someone adds a column outside the stack — the situation drift
        // detection exists to surface.
        c.execute_ddl(
            &project,
            &format!(
                "ALTER TABLE {} ADD COLUMN `snuck_in` STRING",
                bq_table_ref(&project, &ds, table)
            ),
            None,
        )
        .await
        .expect("out-of-band ALTER");

        let live: Vec<BqField> = c.get_table_schema(&project, &ds, table).await.unwrap();
        let declared = [gcpx_bq::schema::types::SchemaField {
            name: "id",
            raw_type: "INT64",
            canonical_type: gcpx_bq::schema::types::normalize_type("INT64"),
            mode: "NULLABLE",
            description: "",
            alter: None,
            alter_raw: None,
            alter_from: None,
            default_value_expression: None,
            rounding_mode: None,
            fields: vec![],
        }];

        let report = gcpx_bq::schema::drift::detect_drift(&declared, &live);
        println!("drift: extra={:?}", report.extra_in_bq);
        assert!(!report.is_clean(), "drift should have been detected");
        assert_eq!(report.extra_in_bq, vec!["snuck_in"]);
    })
    .await;
}

#[tokio::test]
async fn dry_run_reports_cost_and_rejects_invalid_sql() {
    let project = require_project!();
    let c = client();

    let ok = c
        .dry_run_query(&project, "SELECT 1 AS n", None)
        .await
        .expect("dry_run_query");
    assert!(
        ok.valid,
        "valid SQL should dry-run clean: {:?}",
        ok.error_message
    );
    println!("dry run bytes: {}", ok.total_bytes_processed);

    // Invalid SQL is a validation result, not a transport failure. That
    // distinction is what lets `check` report it against the sql property
    // instead of failing the deploy with an opaque error.
    let bad = c
        .dry_run_query(&project, "SELEC 1", None)
        .await
        .expect("a syntax error should be reported, not raised");
    assert!(!bad.valid);
    let msg = bad.error_message.unwrap_or_default();
    println!(
        "dry run rejection: {}",
        msg.lines()
            .find(|l| l.contains("Syntax"))
            .unwrap_or("")
            .trim()
    );
    assert!(msg.contains("Syntax error"), "should explain the rejection");
}

/// The end-to-end claim of the dbt engine: macros expand, refs resolve, and
/// BigQuery accepts the statement that comes out.
#[tokio::test]
async fn dbt_model_ddl_executes_end_to_end() {
    let project = require_project!();
    let ds = unique("ds");

    with_dataset(&project, &ds, || async {
        let c = client();
        let upstream = bq_table_ref(&project, &ds, "stg_orders");
        c.execute_ddl(
            &project,
            &format!(
                "CREATE OR REPLACE TABLE {upstream} AS \
                 SELECT 1 AS order_id, 250 AS amount_cents"
            ),
            None,
        )
        .await
        .expect("seed upstream table");

        let mut model_refs = BTreeMap::new();
        model_refs.insert(
            "stg_orders".to_owned(),
            gcpx_dbt::types::ModelRefData {
                materialization: "table".to_owned(),
                resolved_ctes_json: "[]".to_owned(),
                resolved_body: String::new(),
                table_ref: upstream.clone(),
                resolved_ddl: String::new(),
                resolved_sql: String::new(),
                workflow_yaml: String::new(),
            },
        );
        let mut macros = BTreeMap::new();
        macros.insert(
            "cents_to_dollars".to_owned(),
            gcpx_dbt::macros::MacroDef {
                args: vec!["c".to_owned()],
                sql: "ROUND({{ c }} / 100.0, 2)".to_owned(),
            },
        );

        let sql = "{{ config(materialized='table') }}\n\
                   SELECT order_id, {{ cents_to_dollars('amount_cents') }} AS amount \
                   FROM {{ ref('stg_orders') }}";

        let preprocessed = gcpx_dbt::preprocess::preprocess(
            sql,
            &Default::default(),
            &bq_table_ref(&project, &ds, "mart_revenue"),
            false,
        )
        .expect("preprocess");

        let resolved = gcpx_dbt::resolver::resolve(
            &preprocessed,
            &project,
            &ds,
            &Default::default(),
            &model_refs,
            &macros,
        )
        .expect("resolve");

        let ddl = gcpx_dbt::resolver::generate_ddl(
            &project,
            &ds,
            "mart_revenue",
            "table",
            &resolved,
            None,
            None,
            None,
        );
        println!("generated DDL:\n{ddl}");

        c.execute_ddl(&project, &ddl, None)
            .await
            .unwrap_or_else(|e| panic!("generated DDL rejected by BigQuery:\n{ddl}\n{e}"));

        let cols: Vec<String> = c
            .get_table_schema(&project, &ds, "mart_revenue")
            .await
            .expect("model table should exist")
            .iter()
            .map(|f| f.name.clone())
            .collect();
        println!("model columns: {cols:?}");
        assert!(cols.iter().any(|c| c == "order_id"));
        assert!(
            cols.iter().any(|c| c == "amount"),
            "the macro-expanded column is missing"
        );
    })
    .await;
}

/// An incremental model renders twice from one source: once for the initial
/// build and once for the scheduled run. Both must be valid SQL, and they must
/// differ — which is the thing a single YAML-time render could not produce.
#[tokio::test]
async fn incremental_model_renders_both_passes() {
    let project = require_project!();
    let ds = unique("ds");

    with_dataset(&project, &ds, || async {
        let c = client();
        let this = bq_table_ref(&project, &ds, "events");

        // A real incremental model reads from a source, so the predicate has
        // something to attach to. (An earlier version of this fixture selected
        // constants, and BigQuery correctly rejected a WHERE with no FROM.)
        let source = bq_table_ref(&project, &ds, "raw_events");
        c.execute_ddl(
            &project,
            &format!(
                "CREATE OR REPLACE TABLE {source} AS \
                 SELECT 1 AS id, CURRENT_TIMESTAMP() AS ts"
            ),
            None,
        )
        .await
        .expect("seed source table");

        let sql = format!(
            "{{{{ config(materialized='incremental', unique_key='id') }}}}\n\
             SELECT id, ts FROM {source}\n\
             {{% if is_incremental() %}}\n\
             WHERE ts > (SELECT MAX(ts) FROM {{{{ this }}}})\n\
             {{% endif %}}"
        );
        let sql = sql.as_str();

        let first = gcpx_dbt::preprocess::preprocess(sql, &Default::default(), &this, false)
            .expect("initial render");
        let incremental = gcpx_dbt::preprocess::preprocess(sql, &Default::default(), &this, true)
            .expect("incremental render");

        assert!(
            !first.contains("WHERE"),
            "the initial build must not carry the incremental predicate"
        );
        assert!(
            incremental.contains("WHERE"),
            "the scheduled run must carry the incremental predicate"
        );
        assert!(
            !first.contains("{%") && !incremental.contains("{%"),
            "no Jinja may survive into the emitted SQL"
        );

        // Both passes have to be SQL BigQuery accepts.
        let resolved_first = gcpx_dbt::resolver::resolve(
            &first,
            &project,
            &ds,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .expect("resolve initial");
        let ddl = gcpx_dbt::resolver::generate_ddl(
            &project,
            &ds,
            "events",
            "table",
            &resolved_first,
            None,
            None,
            None,
        );
        c.execute_ddl(&project, &ddl, None)
            .await
            .unwrap_or_else(|e| panic!("initial DDL rejected:\n{ddl}\n{e}"));

        let resolved_inc = gcpx_dbt::resolver::resolve(
            &incremental,
            &project,
            &ds,
            &Default::default(),
            &Default::default(),
            &Default::default(),
        )
        .expect("resolve incremental");
        let merge = gcpx_dbt::resolver::generate_merge_ddl(
            &project,
            &ds,
            "events",
            &["id".to_owned()],
            &["id".to_owned(), "ts".to_owned()],
            &resolved_inc,
        );
        println!("incremental MERGE:\n{merge}");
        c.execute_ddl(&project, &merge, None)
            .await
            .unwrap_or_else(|e| panic!("incremental MERGE rejected:\n{merge}\n{e}"));
    })
    .await;
}

#[tokio::test]
async fn routine_lifecycle() {
    let project = require_project!();
    let ds = unique("ds");

    with_dataset(&project, &ds, || async {
        let c = client();
        let created = c
            .create_routine(
                &project,
                &ds,
                &serde_json::json!({
                    "routineReference": {
                        "projectId": project, "datasetId": ds, "routineId": "cents_to_dollars"
                    },
                    "routineType": "SCALAR_FUNCTION",
                    "language": "SQL",
                    "arguments": [{ "name": "cents", "dataType": { "typeKind": "INT64" } }],
                    "definitionBody": "ROUND(cents / 100.0, 2)",
                }),
            )
            .await
            .expect("create_routine");
        assert_eq!(created.routine_id, "cents_to_dollars");

        assert_eq!(
            c.get_routine(&project, &ds, "cents_to_dollars")
                .await
                .expect("get_routine")
                .routine_type,
            "SCALAR_FUNCTION"
        );

        c.delete_routine(&project, &ds, "cents_to_dollars")
            .await
            .expect("delete_routine");
    })
    .await;
}
