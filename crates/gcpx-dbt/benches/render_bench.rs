// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
// The template pipeline, measured.
//
// This runs once per model per deploy, and a stack can hold hundreds of models,
// so it sits on the path a user waits behind. The benchmarks scale the input
// rather than measuring one fixed case, because what matters is how the cost
// grows: a scanner that is linear in the SQL and a resolver that is linear in
// the refs stay usable on a large project, and anything quadratic does not.

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use gcpx_dbt::macros::{expand_macros, MacroDef};
use gcpx_dbt::preprocess::preprocess;
use gcpx_dbt::resolver::{generate_ddl, resolve};
use gcpx_dbt::scanner::DbtScanner;
use gcpx_dbt::types::{ModelRefData, SourceDef};

fn model_refs(count: usize) -> BTreeMap<String, ModelRefData> {
    (0..count)
        .map(|i| {
            (
                format!("model_{i}"),
                ModelRefData {
                    materialization: "table".to_owned(),
                    resolved_ctes_json: "[]".to_owned(),
                    resolved_body: String::new(),
                    table_ref: format!("`p.d.model_{i}`"),
                    resolved_ddl: String::new(),
                    resolved_sql: String::new(),
                    workflow_yaml: String::new(),
                },
            )
        })
        .collect()
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

/// A model of roughly realistic shape: some plain SQL, some refs, a macro call.
fn model_sql(refs: usize) -> String {
    let mut sql = String::from("{{ config(materialized='table') }}\nSELECT\n  a.id,\n");
    sql.push_str("  {{ cents_to_dollars('a.amount_cents') }} AS amount\n");
    sql.push_str("FROM {{ source('raw', 'events') }} a\n");
    for i in 0..refs {
        sql.push_str(&format!(
            "JOIN {{{{ ref('model_{i}') }}}} m{i} ON m{i}.id = a.id\n"
        ));
    }
    sql.push_str("WHERE a.id IS NOT NULL");
    sql
}

/// Scanning is the innermost loop; everything else runs on top of it.
fn bench_scanner(c: &mut Criterion) {
    let mut group = c.benchmark_group("scanner");
    for size in [1, 16, 128, 1024] {
        let sql = model_sql(size);
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &sql, |b, sql| {
            b.iter(|| {
                let mut n = 0usize;
                for segment in DbtScanner::new(black_box(sql)) {
                    n += black_box(&segment) as *const _ as usize & 1;
                }
                black_box(n)
            });
        });
    }
    group.finish();
}

/// Preprocessing walks the text several times; this is where a naive
/// implementation goes quadratic.
fn bench_preprocess(c: &mut Criterion) {
    let vars: BTreeMap<String, String> = [("start_date".to_owned(), "'2026-01-01'".to_owned())]
        .into_iter()
        .collect();
    let mut group = c.benchmark_group("preprocess");
    for size in [1, 16, 128, 1024] {
        let sql = model_sql(size);
        group.throughput(Throughput::Bytes(sql.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &sql, |b, sql| {
            b.iter(|| preprocess(black_box(sql), &vars, "`p.d.this`", false).unwrap());
        });
    }
    group.finish();
}

/// Resolution is linear in refs only if the lookup is; this catches a
/// regression to a scan-per-ref.
fn bench_resolve(c: &mut Criterion) {
    let sources = sources();
    let macros = macros();
    let mut group = c.benchmark_group("resolve");
    for size in [1, 16, 128, 1024] {
        let sql = model_sql(size);
        let refs = model_refs(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| resolve(black_box(&sql), "p", "d", &sources, &refs, &macros).unwrap());
        });
    }
    group.finish();
}

/// Macro expansion re-scans after each pass, so its cost depends on how many
/// passes convergence takes, not only on input size.
fn bench_macro_expansion(c: &mut Criterion) {
    let macros = macros();
    let mut group = c.benchmark_group("expand_macros");
    for calls in [1, 16, 128, 1024] {
        let sql = (0..calls)
            .map(|i| format!("{{{{ cents_to_dollars('c{i}') }}}}"))
            .collect::<Vec<_>>()
            .join(", ");
        group.throughput(Throughput::Elements(calls as u64));
        group.bench_with_input(BenchmarkId::from_parameter(calls), &sql, |b, sql| {
            b.iter(|| expand_macros(black_box(sql), &macros).unwrap());
        });
    }
    group.finish();
}

/// The whole pipeline as a deploy runs it, which is the number a user feels.
fn bench_full_pipeline(c: &mut Criterion) {
    let vars = BTreeMap::new();
    let sources = sources();
    let macros = macros();
    let mut group = c.benchmark_group("full_pipeline");
    for size in [1, 16, 128] {
        let sql = model_sql(size);
        let refs = model_refs(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let pre = preprocess(black_box(&sql), &vars, "`p.d.this`", false).unwrap();
                let resolved = resolve(&pre, "p", "d", &sources, &refs, &macros).unwrap();
                generate_ddl("p", "d", "m", "table", &resolved, None, None, None)
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_scanner,
    bench_preprocess,
    bench_resolve,
    bench_macro_expansion,
    bench_full_pipeline
);
criterion_main!(benches);
