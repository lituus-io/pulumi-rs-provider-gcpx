# pulumi-rs-provider-gcpx

[![CI](https://github.com/lituus-io/pulumi-rs-provider-gcpx/actions/workflows/ci.yml/badge.svg)](https://github.com/lituus-io/pulumi-rs-provider-gcpx/actions/workflows/ci.yml)
[![Security](https://github.com/lituus-io/pulumi-rs-provider-gcpx/actions/workflows/security.yml/badge.svg)](https://github.com/lituus-io/pulumi-rs-provider-gcpx/actions/workflows/security.yml)

A Pulumi resource provider for GCP data engineering and data agents, written in Rust.

It covers the ground the official GCP provider leaves out: BigQuery schema evolution with declarative
column operations, dbt-style SQL models whose dependency graph *is* the Pulumi graph, SCD-2 snapshots,
scheduled SQL, Dataproc Serverless ingest/export, and Conversational Analytics data agents grounded
directly on the models and functions you already declared.

The plugin binary is `pulumi-resource-gcpx` and the schema package is `gcpx`.

## Resources

| Namespace | Resources |
|---|---|
| `gcpx:bigquery` | `TableSchema`, `Table`, `Dataset`, `RoutineFunction` |
| `gcpx:dbt` | `Project`, `Model`, `Macro`, `Snapshot` |
| `gcpx:scheduler` | `SqlJob` |
| `gcpx:dataproc` | `IngestJob`, `ExportJob` |
| `gcpx:agent` | `DataAgent`, `DataAgentIamPolicy`, `Conversation`, `AgentEngine`, `Memory`, `AgentEval` |

See [SKILL.md](SKILL.md) for the full resource reference and composition patterns, and
[`examples/`](examples/) for runnable stacks.

## Layout

```
crates/
  gcpx-core/       shared transport: auth, HTTP + retry + circuit breaker, LRO polling,
                   escaping, output/JSON builders, resource lifecycle helpers
  gcpx-bq/         BigQuery: schema evolution, tables, datasets, routines
  gcpx-dbt/        dbt-style models, macros, and project context
  gcpx-scheduler/  Cloud Workflows + Cloud Scheduler orchestration, scheduled SQL
  gcpx-snapshot/   SCD-2 snapshots
  gcpx-dataproc/   Dataproc Serverless ingest and export jobs
  gcpx-agents/     Conversational Analytics data agents, Vertex AI agent engines, memory
  gcpx-provider/   Pulumi ResourceProvider implementation and dispatch
  pulumi-resource-gcpx/   plugin binary
```

Each crate carries its own tests, benchmarks, and fuzz targets.

## Development

```bash
cargo test --workspace --locked     # unit, property, regression, offline integration
cargo clippy --workspace -- -D warnings
cargo bench --workspace             # criterion; CI gates on regression
cargo build --release               # binary at target/release/pulumi-resource-gcpx
```

Real-GCP integration tests are gated behind environment variables and are not run by default:

```bash
GCPX_TEST_PROJECT=… GCPX_TEST_DATASET=… GCPX_TEST_REGION=… \
  cargo test --test gcp_integration -- --test-threads=1
```

## License

Copyright (c) 2024-2026 lituus-io. All rights reserved.

Author: terekete <spicyzhug@gmail.com>

Dual-licensed under [AGPL-3.0-or-later](LICENSE) or a commercial license. See [LICENSE](LICENSE).
