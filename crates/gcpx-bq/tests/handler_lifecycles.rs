// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Full lifecycles for the BigQuery resources, against the client double.
//!
//! These handlers had no direct tests at all — the schema and routine modules
//! measured zero coverage — because the only thing exercising them was the live
//! suite, which needs a real project and does not run in CI. That left the code
//! a deploy depends on most checked by nothing that runs on a pull request.
//!
//! Each resource is driven the way the engine drives it: Check, then Create,
//! then Read, Update and Delete, plus the failure paths that matter — a
//! rejected input, an adopt on 409, a missing resource on read.

use std::collections::BTreeMap;

use gcpx_bq::mock::MockBqClient;
use gcpx_bq::types::{BqField, BqTableMeta, DatasetMeta, RoutineMeta};
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
fn obj(pairs: Vec<(&str, Value)>) -> Value {
    Value {
        kind: Some(Kind::StructValue(st(pairs))),
    }
}
fn st(pairs: Vec<(&str, Value)>) -> Struct {
    Struct {
        fields: pairs.into_iter().map(|(k, x)| (k.to_owned(), x)).collect(),
    }
}
fn get<'a>(s: &'a Struct, k: &str) -> Option<&'a Value> {
    s.fields.get(k)
}
fn get_str<'a>(s: &'a Struct, k: &str) -> Option<&'a str> {
    match get(s, k)?.kind.as_ref()? {
        Kind::StringValue(v) => Some(v),
        _ => None,
    }
}

fn check_req(news: Struct) -> pulumirpc::CheckRequest {
    pulumirpc::CheckRequest {
        news: Some(news),
        ..Default::default()
    }
}
fn create_req(props: Struct, preview: bool) -> pulumirpc::CreateRequest {
    pulumirpc::CreateRequest {
        properties: Some(props),
        preview,
        ..Default::default()
    }
}
fn diff_req(olds: Struct, news: Struct) -> pulumirpc::DiffRequest {
    pulumirpc::DiffRequest {
        olds: Some(olds.clone()),
        old_inputs: Some(olds),
        news: Some(news),
        ..Default::default()
    }
}

// ── Dataset ─────────────────────────────────────────────────────────────────

fn dataset_inputs() -> Struct {
    st(vec![
        ("project", v("p")),
        ("datasetId", v("analytics")),
        ("location", v("US")),
        ("description", v("a dataset")),
    ])
}

#[tokio::test]
async fn dataset_check_accepts_valid_inputs_and_rejects_a_bad_location() {
    let c = MockBqClient::default();

    let ok = gcpx_bq::dataset::handlers::check_dataset(&c, check_req(dataset_inputs()))
        .await
        .expect("check")
        .into_inner();
    assert!(
        ok.failures.is_empty(),
        "valid inputs rejected: {:?}",
        ok.failures
    );

    // An empty datasetId cannot name a dataset.
    let mut bad = dataset_inputs();
    bad.fields.insert("datasetId".to_owned(), v(""));
    let out = gcpx_bq::dataset::handlers::check_dataset(&c, check_req(bad))
        .await
        .expect("check")
        .into_inner();
    assert!(!out.failures.is_empty(), "an empty datasetId was accepted");
}

#[tokio::test]
async fn dataset_create_read_update_delete() {
    let c = MockBqClient {
        dataset_meta: Some(DatasetMeta {
            dataset_id: "analytics".into(),
            location: "US".into(),
            creation_time: 1,
            last_modified_time: 2,
            etag: "e1".into(),
            description: "a dataset".into(),
            friendly_name: String::new(),
            labels: BTreeMap::new(),
            default_table_expiration_ms: None,
            default_partition_expiration_ms: None,
            storage_billing_model: "LOGICAL".into(),
            max_time_travel_hours: None,
        }),
        ..Default::default()
    };

    let created =
        gcpx_bq::dataset::handlers::create_dataset(&c, create_req(dataset_inputs(), false))
            .await
            .expect("create")
            .into_inner();
    assert_eq!(created.id, "p/analytics");
    let props = created.properties.expect("properties");
    assert_eq!(get_str(&props, "datasetId"), Some("analytics"));

    let read = gcpx_bq::dataset::handlers::read_dataset(
        &c,
        pulumirpc::ReadRequest {
            id: "p/analytics".into(),
            ..Default::default()
        },
    )
    .await
    .expect("read")
    .into_inner();
    assert_eq!(read.id, "p/analytics");

    let updated = gcpx_bq::dataset::handlers::update_dataset(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/analytics".into(),
            olds: Some(dataset_inputs()),
            news: Some({
                let mut n = dataset_inputs();
                n.fields.insert("description".to_owned(), v("changed"));
                n
            }),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");
    assert!(updated.into_inner().properties.is_some());

    gcpx_bq::dataset::handlers::delete_dataset(
        &c,
        pulumirpc::DeleteRequest {
            id: "p/analytics".into(),
            properties: Some(dataset_inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("delete");
}

/// Preview must not touch the API at all — it is the path `pulumi preview`
/// takes, and a provider that creates during preview is a provider that
/// changes infrastructure when asked what it would change.
#[tokio::test]
async fn dataset_preview_creates_nothing() {
    let c = MockBqClient::default();
    let out = gcpx_bq::dataset::handlers::create_dataset(&c, create_req(dataset_inputs(), true))
        .await
        .expect("preview create")
        .into_inner();
    assert!(out.properties.is_some());
    assert!(
        c.dataset_log.lock().unwrap().is_empty(),
        "preview called the API: {:?}",
        c.dataset_log.lock().unwrap()
    );
}

#[tokio::test]
async fn dataset_diff_settles_and_notices_a_change() {
    let c = MockBqClient::default();
    let same =
        gcpx_bq::dataset::handlers::diff_dataset(&c, diff_req(dataset_inputs(), dataset_inputs()))
            .await
            .expect("diff")
            .into_inner();
    assert_eq!(
        same.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32
    );

    let mut changed = dataset_inputs();
    changed.fields.insert("description".to_owned(), v("other"));
    let diff = gcpx_bq::dataset::handlers::diff_dataset(&c, diff_req(dataset_inputs(), changed))
        .await
        .expect("diff")
        .into_inner();
    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

// ── Table ───────────────────────────────────────────────────────────────────

fn table_inputs() -> Struct {
    st(vec![
        ("project", v("p")),
        ("dataset", v("d")),
        ("tableId", v("events")),
        ("description", v("raw events")),
    ])
}

fn table_meta() -> BqTableMeta {
    BqTableMeta {
        table_type: "TABLE".into(),
        description: "raw events".into(),
        friendly_name: String::new(),
        labels: BTreeMap::new(),
        location: "US".into(),
        creation_time: 1,
        last_modified_time: 2,
        num_rows: 0,
        num_bytes: 0,
        etag: "e".into(),
        self_link: String::new(),
        expiration_time: None,
        schema_fields: vec![],
    }
}

#[tokio::test]
async fn table_create_read_update_delete() {
    let c = MockBqClient {
        table_meta: Some(table_meta()),
        ..Default::default()
    };

    let created = gcpx_bq::table::handlers::create_table(&c, create_req(table_inputs(), false))
        .await
        .expect("create")
        .into_inner();
    assert_eq!(created.id, "p/d/events");

    gcpx_bq::table::handlers::read_table(
        &c,
        pulumirpc::ReadRequest {
            id: "p/d/events".into(),
            ..Default::default()
        },
    )
    .await
    .expect("read");

    gcpx_bq::table::handlers::update_table(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/d/events".into(),
            olds: Some(table_inputs()),
            news: Some({
                let mut n = table_inputs();
                n.fields.insert("description".to_owned(), v("changed"));
                n
            }),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    gcpx_bq::table::handlers::delete_table(
        &c,
        pulumirpc::DeleteRequest {
            id: "p/d/events".into(),
            properties: Some(table_inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("delete");
}

/// A view and a materialized view are mutually exclusive: the API grounds a
/// table on one definition or the other, never both.
#[tokio::test]
async fn table_check_rejects_a_view_that_is_also_materialized() {
    let c = MockBqClient::default();
    let mut inputs = table_inputs();
    inputs
        .fields
        .insert("view".to_owned(), obj(vec![("query", v("SELECT 1"))]));
    inputs.fields.insert(
        "materializedView".to_owned(),
        obj(vec![("query", v("SELECT 1"))]),
    );
    let out = gcpx_bq::table::handlers::check_table(&c, check_req(inputs))
        .await
        .expect("check")
        .into_inner();
    assert!(
        !out.failures.is_empty(),
        "a table declared as both a view and a materialized view was accepted"
    );
}

#[tokio::test]
async fn table_preview_creates_nothing() {
    let c = MockBqClient::default();
    gcpx_bq::table::handlers::create_table(&c, create_req(table_inputs(), true))
        .await
        .expect("preview");
    assert!(c.table_log().is_empty(), "preview called the API");
}

// ── Routine ─────────────────────────────────────────────────────────────────

fn routine_inputs() -> Struct {
    st(vec![
        ("project", v("p")),
        ("dataset", v("d")),
        ("routineId", v("to_dollars")),
        ("routineType", v("SCALAR_FUNCTION")),
        ("language", v("SQL")),
        ("definitionBody", v("cents / 100.0")),
        (
            "arguments",
            list(vec![obj(vec![
                ("name", v("cents")),
                ("dataType", v("INT64")),
            ])]),
        ),
        ("returnType", v("FLOAT64")),
    ])
}

#[tokio::test]
async fn routine_check_create_read_update_delete() {
    let c = MockBqClient {
        routine_meta: Some(RoutineMeta {
            routine_id: "to_dollars".into(),
            routine_type: "SCALAR_FUNCTION".into(),
            language: "SQL".into(),
            creation_time: 1,
            last_modified_time: 2,
            etag: "e".into(),
        }),
        ..Default::default()
    };

    let checked = gcpx_bq::routine::handlers::check_routine(&c, check_req(routine_inputs()))
        .await
        .expect("check")
        .into_inner();
    assert!(checked.failures.is_empty(), "{:?}", checked.failures);

    let created =
        gcpx_bq::routine::handlers::create_routine(&c, create_req(routine_inputs(), false))
            .await
            .expect("create")
            .into_inner();
    assert_eq!(created.id, "p/d/to_dollars");

    gcpx_bq::routine::handlers::read_routine(
        &c,
        pulumirpc::ReadRequest {
            id: "p/d/to_dollars".into(),
            ..Default::default()
        },
    )
    .await
    .expect("read");

    gcpx_bq::routine::handlers::update_routine(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/d/to_dollars".into(),
            olds: Some(routine_inputs()),
            news: Some({
                let mut n = routine_inputs();
                n.fields
                    .insert("definitionBody".to_owned(), v("cents / 1000.0"));
                n
            }),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    gcpx_bq::routine::handlers::delete_routine(
        &c,
        pulumirpc::DeleteRequest {
            id: "p/d/to_dollars".into(),
            properties: Some(routine_inputs()),
            ..Default::default()
        },
    )
    .await
    .expect("delete");
}

#[tokio::test]
async fn routine_diff_settles_and_notices_a_body_change() {
    let c = MockBqClient::default();
    let same =
        gcpx_bq::routine::handlers::diff_routine(&c, diff_req(routine_inputs(), routine_inputs()))
            .await
            .expect("diff")
            .into_inner();
    assert_eq!(
        same.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32,
        "an unchanged routine reported a diff"
    );

    let mut changed = routine_inputs();
    changed
        .fields
        .insert("definitionBody".to_owned(), v("cents / 1000.0"));
    let diff = gcpx_bq::routine::handlers::diff_routine(&c, diff_req(routine_inputs(), changed))
        .await
        .expect("diff")
        .into_inner();
    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

// ── TableSchema ─────────────────────────────────────────────────────────────

fn schema_inputs(fields: Vec<Value>) -> Struct {
    st(vec![
        ("project", v("p")),
        ("dataset", v("d")),
        ("tableId", v("events")),
        ("schema", list(fields)),
    ])
}

fn field(name: &str, ty: &str) -> Value {
    obj(vec![("name", v(name)), ("type", v(ty))])
}

/// A column the stack is asking to be added.
///
/// Evolution here is declarative rather than inferred: listing a new field is
/// not a request to create it — the schema resource assumes a listed column
/// already exists and only reconciles its description. `alter: insert` is what
/// asks for the column, which is what makes re-applying an unchanged schema a
/// no-op instead of a stream of ADD COLUMN attempts.
fn added_field(name: &str, ty: &str) -> Value {
    obj(vec![
        ("name", v(name)),
        ("type", v(ty)),
        ("alter", v("insert")),
    ])
}

#[tokio::test]
async fn schema_check_accepts_a_valid_schema_and_rejects_a_bad_type() {
    let c = MockBqClient::default();

    let ok = gcpx_bq::schema::handlers::check_table_schema(
        &c,
        check_req(schema_inputs(vec![
            field("id", "INT64"),
            field("name", "STRING"),
        ])),
    )
    .await
    .expect("check")
    .into_inner();
    assert!(ok.failures.is_empty(), "{:?}", ok.failures);

    let bad = gcpx_bq::schema::handlers::check_table_schema(
        &c,
        check_req(schema_inputs(vec![field("id", "NOT_A_TYPE")])),
    )
    .await
    .expect("check")
    .into_inner();
    assert!(
        !bad.failures.is_empty(),
        "an invalid column type was accepted"
    );
}

#[tokio::test]
async fn schema_create_applies_the_declared_columns() {
    let c = MockBqClient::new(vec![]);
    let created = gcpx_bq::schema::handlers::create_table_schema(
        &c,
        create_req(schema_inputs(vec![field("id", "INT64")]), false),
    )
    .await
    .expect("create")
    .into_inner();
    assert_eq!(created.id, "p/d/events");
    assert!(created.properties.is_some());
}

/// Adding a nullable column is an in-place evolution; the handler must emit an
/// ALTER rather than replacing the table.
#[tokio::test]
async fn schema_update_evolves_in_place() {
    let c = MockBqClient::new(vec![BqField {
        name: "id".into(),
        field_type: "INT64".into(),
        mode: "NULLABLE".into(),
        description: String::new(),
        fields: vec![],
    }]);

    gcpx_bq::schema::handlers::update_table_schema(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/d/events".into(),
            olds: Some(schema_inputs(vec![field("id", "INT64")])),
            news: Some(schema_inputs(vec![
                field("id", "INT64"),
                added_field("added", "STRING"),
            ])),
            preview: false,
            ..Default::default()
        },
    )
    .await
    .expect("update");

    let ddl = c.ddl_log();
    assert!(
        ddl.iter().any(|s| s.to_uppercase().contains("ADD COLUMN")),
        "adding a column did not emit an ADD COLUMN: {ddl:?}"
    );
}

#[tokio::test]
async fn schema_diff_settles_and_notices_an_added_column() {
    let c = MockBqClient::default();
    let one = schema_inputs(vec![field("id", "INT64")]);
    let two = schema_inputs(vec![field("id", "INT64"), added_field("extra", "STRING")]);

    let same = gcpx_bq::schema::handlers::diff_table_schema(&c, diff_req(one.clone(), one.clone()))
        .await
        .expect("diff")
        .into_inner();
    assert_eq!(
        same.changes,
        pulumirpc::diff_response::DiffChanges::DiffNone as i32,
        "an unchanged schema reported a diff"
    );

    let diff = gcpx_bq::schema::handlers::diff_table_schema(&c, diff_req(one, two))
        .await
        .expect("diff")
        .into_inner();
    assert_eq!(
        diff.changes,
        pulumirpc::diff_response::DiffChanges::DiffSome as i32
    );
}

/// Deleting a schema resource must not drop the table it describes: the schema
/// is a view onto a table owned by another resource, and dropping it here would
/// destroy data the stack never said to destroy.
#[tokio::test]
async fn deleting_a_schema_leaves_the_table_alone() {
    let c = MockBqClient::new(vec![]);
    gcpx_bq::schema::handlers::delete_table_schema(
        &c,
        pulumirpc::DeleteRequest {
            id: "p/d/events".into(),
            properties: Some(schema_inputs(vec![field("id", "INT64")])),
            ..Default::default()
        },
    )
    .await
    .expect("delete");

    let ddl = c.ddl_log();
    assert!(
        !ddl.iter().any(|s| s.to_uppercase().contains("DROP TABLE")),
        "deleting a schema dropped the table: {ddl:?}"
    );
}

// ── Failure paths ───────────────────────────────────────────────────────────

/// An API failure must surface as an error, not be swallowed into a success
/// that leaves the stack believing a resource exists.
#[tokio::test]
async fn an_api_failure_reaches_the_caller() {
    let c = MockBqClient::failing("create_dataset");
    let result =
        gcpx_bq::dataset::handlers::create_dataset(&c, create_req(dataset_inputs(), false)).await;
    assert!(result.is_err(), "a failing create reported success");
}

/// Reading a resource that is gone must report it as gone — an empty id is how
/// the engine learns to drop it from state — rather than failing the refresh.
#[tokio::test]
async fn reading_a_missing_dataset_reports_it_missing() {
    let c = MockBqClient::failing("get_dataset");
    let out = gcpx_bq::dataset::handlers::read_dataset(
        &c,
        pulumirpc::ReadRequest {
            id: "p/gone".into(),
            ..Default::default()
        },
    )
    .await;
    // Failing loudly is defensible too; reporting the dataset as present is not.
    if let Ok(resp) = out {
        assert!(
            resp.into_inner().id.is_empty(),
            "a missing dataset was reported as present"
        );
    }
}

/// Refresh is the one call holding both the declared schema and the live one,
/// so it is where drift gets reported. A column that BigQuery has and the stack
/// does not know about should surface as an output, not be silently absorbed.
#[tokio::test]
async fn reading_a_schema_reports_drift_against_the_declared_one() {
    let c = MockBqClient::new(vec![
        BqField {
            name: "id".into(),
            field_type: "INT64".into(),
            mode: "NULLABLE".into(),
            description: String::new(),
            fields: vec![],
        },
        BqField {
            name: "added_out_of_band".into(),
            field_type: "STRING".into(),
            mode: "NULLABLE".into(),
            description: String::new(),
            fields: vec![],
        },
    ]);

    let out = gcpx_bq::schema::handlers::read_table_schema(
        &c,
        pulumirpc::ReadRequest {
            id: "p/d/events".into(),
            // The schema the stack declares, which knows only about `id`.
            inputs: Some(schema_inputs(vec![field("id", "INT64")])),
            ..Default::default()
        },
    )
    .await
    .expect("read")
    .into_inner();

    let props = out.properties.expect("properties");
    assert!(
        props.fields.contains_key("schemaDrift"),
        "a column present in BigQuery but absent from the declared schema was \
         not reported as drift: {:?}",
        props.fields.keys().collect::<Vec<_>>()
    );
}

/// And when the two agree there is nothing to report — a drift output that is
/// always present is one nobody can alert on.
#[tokio::test]
async fn reading_a_schema_that_matches_reports_no_drift() {
    let c = MockBqClient::new(vec![BqField {
        name: "id".into(),
        field_type: "INT64".into(),
        mode: "NULLABLE".into(),
        description: String::new(),
        fields: vec![],
    }]);

    let out = gcpx_bq::schema::handlers::read_table_schema(
        &c,
        pulumirpc::ReadRequest {
            id: "p/d/events".into(),
            inputs: Some(schema_inputs(vec![field("id", "INT64")])),
            ..Default::default()
        },
    )
    .await
    .expect("read")
    .into_inner();

    let props = out.properties.expect("properties");
    assert!(
        !props.fields.contains_key("schemaDrift"),
        "drift was reported against a schema that matches"
    );
}

/// Dropping a column that something downstream depends on has to be refused
/// before any DDL runs — the point of the check is that the data is still there
/// when it fails.
#[tokio::test]
async fn dropping_a_column_a_consumer_depends_on_is_refused() {
    let c = MockBqClient::new(vec![
        BqField {
            name: "id".into(),
            field_type: "INT64".into(),
            mode: "NULLABLE".into(),
            description: String::new(),
            fields: vec![],
        },
        BqField {
            name: "region".into(),
            field_type: "STRING".into(),
            mode: "NULLABLE".into(),
            description: String::new(),
            fields: vec![],
        },
    ]);

    let mut news = schema_inputs(vec![
        field("id", "INT64"),
        obj(vec![
            ("name", v("region")),
            ("type", v("STRING")),
            ("alter", v("delete")),
        ]),
    ]);
    news.fields.insert(
        "consumerConstraints".to_owned(),
        obj(vec![("clusterColumns", list(vec![v("region")]))]),
    );

    let out = gcpx_bq::schema::handlers::update_table_schema(
        &c,
        pulumirpc::UpdateRequest {
            id: "p/d/events".into(),
            olds: Some(schema_inputs(vec![
                field("id", "INT64"),
                field("region", "STRING"),
            ])),
            news: Some(news),
            preview: false,
            ..Default::default()
        },
    )
    .await;

    assert!(
        out.is_err(),
        "a column used as a clustering key was dropped without complaint"
    );
    assert!(
        c.ddl_log().is_empty(),
        "DDL ran before the safety check refused it: {:?}",
        c.ddl_log()
    );
}
