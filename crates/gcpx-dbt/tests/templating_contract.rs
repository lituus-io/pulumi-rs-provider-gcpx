// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The contract between the two template layers.
//!
//! Two things render templates in a deployment, and confusing them is the
//! easiest way to break a model:
//!
//! 1. The **YAML runtime** renders Jinja in the stack file itself, once, before
//!    any resource exists. It has the project config and the environment. It
//!    does not have resource outputs, because nothing has been created yet.
//!
//! 2. **This provider** renders dbt templates in SQL, per resource, while the
//!    graph runs. It has resource outputs — which is the whole point, because
//!    every dbt construct resolves against one.
//!
//! The division is not a matter of taste. `{{ ref('x') }}` becomes the table
//! reference published by another resource, and that value does not exist when
//! the YAML is rendered. `{% if is_incremental() %}` must render *twice* from
//! one source — once without the predicate for the initial build, once with it
//! for the scheduled run — and a single render cannot produce two answers.
//!
//! SQL loaded with `fn::readFile` never passes through the YAML renderer at
//! all: that builtin is a plain file read. SQL written inline in the stack file
//! does pass through it, and survives only because the runtime classifies
//! unknown roots as passthrough. Prefer `fn::readFile`.
//!
//! What this file pins down is the provider's half: every dbt construct is
//! fully consumed, so nothing template-shaped can reach BigQuery.

use std::collections::BTreeMap;

use gcpx_dbt::macros::MacroDef;
use gcpx_dbt::preprocess::preprocess;
use gcpx_dbt::resolver::resolve;
use gcpx_dbt::types::{ModelRefData, SourceDef};

fn model_refs() -> BTreeMap<String, ModelRefData> {
    let mut m = BTreeMap::new();
    m.insert(
        "stg_orders".to_owned(),
        ModelRefData {
            materialization: "table".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: String::new(),
            table_ref: "`p.d.stg_orders`".to_owned(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    m.insert(
        "int_tmp".to_owned(),
        ModelRefData {
            materialization: "ephemeral".to_owned(),
            resolved_ctes_json: "[]".to_owned(),
            resolved_body: "SELECT 1 AS id".to_owned(),
            table_ref: String::new(),
            resolved_ddl: String::new(),
            resolved_sql: String::new(),
            workflow_yaml: String::new(),
        },
    );
    m
}

fn sources() -> BTreeMap<String, SourceDef> {
    let mut s = BTreeMap::new();
    s.insert(
        "raw".to_owned(),
        SourceDef {
            dataset: "raw_data".to_owned(),
            tables: vec!["events".to_owned()],
        },
    );
    s
}

fn macros() -> BTreeMap<String, MacroDef> {
    let mut m = BTreeMap::new();
    m.insert(
        "cents_to_dollars".to_owned(),
        MacroDef {
            args: vec!["c".to_owned()],
            sql: "ROUND({{ c }} / 100.0, 2)".to_owned(),
        },
    );
    m
}

fn vars() -> BTreeMap<String, String> {
    let mut v = BTreeMap::new();
    v.insert("start_date".to_owned(), "'2026-01-01'".to_owned());
    v
}

/// Render the way the provider does, for the initial (non-incremental) pass.
fn render(sql: &str) -> String {
    let pre = preprocess(sql, &vars(), "`p.d.this_model`", false).expect("preprocess");
    resolve(&pre, "p", "d", &sources(), &model_refs(), &macros())
        .expect("resolve")
        .to_sql()
        .into_owned()
}

/// Nothing template-shaped may reach BigQuery. This is the invariant the whole
/// boundary exists to preserve: a leaked `{{` becomes a syntax error pointing
/// at the template rather than at the mistake.
fn assert_fully_rendered(sql: &str) {
    assert!(
        !sql.contains("{{") && !sql.contains("}}"),
        "expression syntax survived rendering: {sql}"
    );
    assert!(
        !sql.contains("{%") && !sql.contains("%}"),
        "statement syntax survived rendering: {sql}"
    );
}

#[test]
fn every_dbt_construct_is_consumed_by_the_provider() {
    // One model using every construct the provider claims to handle.
    let sql = "{{ config(materialized='table', unique_key='id') }}\n\
               SELECT\n\
                 o.id,\n\
                 {{ cents_to_dollars('o.amount_cents') }} AS amount,\n\
                 e.kind\n\
               FROM {{ ref('stg_orders') }} o\n\
               JOIN {{ source('raw', 'events') }} e ON e.id = o.id\n\
               JOIN {{ ref('int_tmp') }} t ON t.id = o.id\n\
               WHERE o.created_at >= {{ var('start_date') }}\n\
                 AND o.model = '{{ this }}'";

    let out = render(sql);
    assert_fully_rendered(&out);

    // Each construct resolved to the right thing, not merely to something.
    assert!(out.contains("`p.d.stg_orders`"), "ref: {out}");
    assert!(out.contains("`p.raw_data.events`"), "source: {out}");
    assert!(
        out.contains("ROUND(o.amount_cents / 100.0, 2)"),
        "macro: {out}"
    );
    assert!(out.contains("'2026-01-01'"), "var: {out}");
    assert!(out.contains("`p.d.this_model`"), "this: {out}");
    assert!(out.contains("__dbt__cte__int_tmp"), "ephemeral ref: {out}");
    assert!(
        !out.contains("config("),
        "config must be consumed, not emitted"
    );
}

/// The construct that settles the layering question on its own: one source,
/// two renders, two different results. A renderer that runs once — which is
/// what the YAML layer is — cannot produce both.
#[test]
fn is_incremental_renders_differently_for_each_pass() {
    let sql = "SELECT * FROM {{ ref('stg_orders') }}\n\
               {% if is_incremental() %}\n\
               WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})\n\
               {% endif %}";

    // Preprocessing owns the statement tags; `ref` is resolved by the later
    // stage, so only `{% %}` is expected to be gone at this point.
    let initial = preprocess(sql, &vars(), "`p.d.this_model`", false).unwrap();
    let scheduled = preprocess(sql, &vars(), "`p.d.this_model`", true).unwrap();

    assert!(!initial.contains("WHERE"), "initial build: {initial}");
    assert!(scheduled.contains("WHERE"), "scheduled run: {scheduled}");
    assert_ne!(initial, scheduled);
    for stage in [&initial, &scheduled] {
        assert!(
            !stage.contains("{%") && !stage.contains("%}"),
            "statement syntax survived preprocessing: {stage}"
        );
    }

    // Through the full pipeline both passes are complete SQL.
    for (pass, rendered) in [
        (
            "initial",
            resolve(&initial, "p", "d", &sources(), &model_refs(), &macros()),
        ),
        (
            "scheduled",
            resolve(&scheduled, "p", "d", &sources(), &model_refs(), &macros()),
        ),
    ] {
        let sql = rendered
            .unwrap_or_else(|e| panic!("{pass} pass failed to resolve: {e}"))
            .to_sql()
            .into_owned();
        assert_fully_rendered(&sql);
        assert!(sql.contains("`p.d.stg_orders`"), "{pass}: {sql}");
    }
}

/// `ref` resolves to a value published by another resource. The provider is the
/// only layer that has it, because it does not exist until that resource is
/// created.
#[test]
fn ref_resolves_to_another_resources_output() {
    let out = render("SELECT * FROM {{ ref('stg_orders') }}");
    assert!(out.contains("`p.d.stg_orders`"));

    // And an undeclared ref is an error naming the model, not a silent
    // passthrough that would reach BigQuery as broken SQL.
    let pre = preprocess("SELECT * FROM {{ ref('nope') }}", &vars(), "`t`", false).unwrap();
    let err = resolve(&pre, "p", "d", &sources(), &model_refs(), &macros())
        .expect_err("an undeclared ref must fail");
    assert!(err.to_string().contains("nope"), "{err}");
}

#[test]
fn unknown_source_fails_by_name() {
    let pre = preprocess(
        "SELECT * FROM {{ source('missing', 't') }}",
        &vars(),
        "`t`",
        false,
    )
    .unwrap();
    let err = resolve(&pre, "p", "d", &sources(), &model_refs(), &macros())
        .expect_err("an undeclared source must fail");
    assert!(err.to_string().contains("missing"), "{err}");
}

#[test]
fn unknown_var_fails_and_lists_what_is_available() {
    // A typo'd var should say what it could have been, not render an empty
    // string into the SQL.
    let err = preprocess("SELECT {{ var('nope') }}", &vars(), "`t`", false)
        .expect_err("an unknown var must fail");
    let msg = err.to_string();
    assert!(msg.contains("nope"));
    assert!(
        msg.contains("start_date"),
        "should list available vars: {msg}"
    );
}

/// Anything the provider does not own is left exactly as it was, for a later
/// stage or for BigQuery to reject with a message about the real problem.
#[test]
fn unrecognised_constructs_are_passed_through_unchanged() {
    let sql = "SELECT 1 -- a {{ comment-like }} thing";
    let out = preprocess(sql, &vars(), "`t`", false).unwrap();
    assert!(out.contains("{{ comment-like }}"), "{out}");
}

/// SQL with no templates at all must come through byte-identical. Anything else
/// means the pipeline is rewriting queries it was not asked to touch.
#[test]
fn plain_sql_is_untouched() {
    let sql = "SELECT a, b FROM `p.d.t` WHERE c = 'x' AND d LIKE '%{%' ORDER BY a";
    let out = preprocess(sql, &vars(), "`t`", false).unwrap();
    assert_eq!(out, sql);
}

/// Macro expansion is bounded. A macro that expands to itself must fail rather
/// than run until memory is gone.
#[test]
fn recursive_macros_terminate() {
    let mut m = BTreeMap::new();
    m.insert(
        "loop".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ loop() }}".to_owned(),
        },
    );
    let err = gcpx_dbt::macros::expand_macros("SELECT {{ loop() }}", &m)
        .expect_err("a self-referential macro must not run forever");
    assert!(err.to_string().contains("recursion") || err.to_string().contains("converge"));
}

/// Every construct, rendered, must be free of template syntax — asserted over
/// the whole set rather than one at a time, so a construct added later without
/// a rendering rule fails here.
#[test]
fn no_construct_leaks_template_syntax() {
    let cases = [
        "{{ config(materialized='view') }} SELECT 1",
        "SELECT * FROM {{ ref('stg_orders') }}",
        "SELECT * FROM {{ source('raw', 'events') }}",
        "SELECT {{ cents_to_dollars('x') }}",
        "SELECT {{ var('start_date') }}",
        "SELECT '{{ this }}'",
        "SELECT 1 {% if is_incremental() %} WHERE 1=1 {% endif %}",
        "{%- if is_incremental() -%} SELECT 1 {%- endif -%}",
    ];
    for case in cases {
        let out = render(case);
        assert_fully_rendered(&out);
    }
}
