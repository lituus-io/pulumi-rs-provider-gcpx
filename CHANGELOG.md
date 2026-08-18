# Changelog

## 0.1.0

First release. Supersedes the `pulumi-resource-gcpx` distribution, which is
retired; this ships as `pulumi-rs-provider-gcpx` with the Python module
`pulumi_rs_provider_gcpx`. The plugin binary keeps the name
`pulumi-resource-gcpx` and the schema package stays `gcpx`, because Pulumi
resolves a provider by that filename and every existing stack names those types.

### Resources

Eleven carried forward unchanged, so existing stacks keep working:

- `gcpx:bigquery` — `TableSchema`, `Table`, `Dataset`, `RoutineFunction`
- `gcpx:dbt` — `Project`, `Model`, `Macro`, `Snapshot`
- `gcpx:scheduler` — `SqlJob`
- `gcpx:dataproc` — `IngestJob`, `ExportJob`

Five new, for GCP data agents:

- `gcpx:agent` — `DataAgent`, `DataAgentIamPolicy`, `Conversation`,
  `AgentEngine`, `Memory`

A `DataAgent` is grounded by wiring `models` to dbt model outputs rather than
naming tables by hand. The dependency edge that creates means an agent cannot be
published before the data it describes exists.

### Structure

One crate per resource family, so each is compiled, benchmarked, fuzzed and
coverage-gated on its own. Handlers are free functions taking a client, which
makes them testable without constructing a provider.

Rust 1.90, with a committed lockfile and a CI job that exercises the pin. The
predecessor pinned a toolchain it could not build on, because the lockfile was
excluded from version control.

### Fixed

Found by auditing the predecessor:

- Schema drift detection was 153 lines of tested code with no callers. It now
  runs during refresh and reports divergence as a `schemaDrift` output.
- Failures reached the engine as `internal` regardless of cause. They are now
  classified, so a rate limit, a missing permission and an expired credential
  each arrive as themselves with a suggested next step.
- A second token cache sat above ADC, ignored its scope argument, and assumed a
  fixed lifetime. ADC already caches per scope and tracks real expiry, so the
  cache was removed rather than repaired.
- One circuit breaker was shared across every API, so a single outage
  fast-rejected unrelated calls. Breakers are now per service and measured with
  a monotonic clock.
- Identifier escaping was bypassed in four places that built quoted references
  by hand.
- Deleting a dbt model discarded the result, so a failed drop was
  indistinguishable from success and left an orphaned table.
- The `is_incremental` block matcher accepted exactly one spelling, so any other
  formatting leaked Jinja into the emitted SQL.

Found by deploying against a real project:

- `deleteContents` was never sent, so destroying a dataset holding tables
  failed — which is the ordinary case, since a dataset is usually the parent of
  the tables in the same stack. Now controlled by `deleteContentsOnDestroy`.
- The schema declared array defaults on `networkTags`. Pulumi rejects a constant
  default on a non-scalar and fails the *whole* schema, so nothing was being
  type-checked.
- A model whose tables are created by the same stack failed its own preview,
  because the dry run reported them as missing. Not-found is now tolerated while
  real SQL errors still fail.
- `glossaryTerms` entries use `displayName`; `term` is rejected.
- The chat request carries `dataAgentContext` at the top level.
- The chat stream interleaves reasoning with the answer, separated only by
  `textType`; without that check the answer arrived prefixed with thinking.
- Not every 409 means "already exists". Cloud Scheduler answers overlapping
  mutations with 409 `ABORTED`, which is transient; those are now retried while
  an already-exists 409 still drives the adopt path.

Found by the security suite:

- Credential redaction split on whitespace and matched prefixes, so a token
  embedded in JSON — the shape API errors actually use — was not redacted. It
  now scans in place.

Found by the plugin's own tests:

- Credentials resolved at startup, which made offline validation impossible.
  They now resolve on first use, and the plugin serves with none present.
- gRPC message limits were left at tonic's 4 MiB default while the engine and
  language runtime use 512 MiB.

### Known gaps

- `schemaRelationships` and `userFunctions` are not sent. Both message names
  exist, but no accepted subfield spelling could be determined, and an
  unverified shape fails the deploy. This also means BigQuery routines cannot
  yet be wired into an agent as callable functions.
- Dataproc resources are unverified against a live project; that API was not
  enabled where the rest was tested.

### Template layering

Two renderers see a stack and both use the same delimiters. The YAML runtime
renders the stack file once, before any resource exists; this provider renders
dbt templates in SQL afterwards, per resource. Three ways to keep them apart,
in order of preference:

1. **`fn::readFile`** — the builtin is a plain file read, so SQL loaded that way
   never reaches the YAML renderer. Always works. Every example uses it.
2. **`{% raw %}…{% endraw %}`** — Jinja's own escape, honoured by both layers.
   Verified to work inline in strict and passthrough modes alike.
3. **Passthrough mode** — partial, and not something to rely on: it pre-escapes
   `{{ expressions }}` but not `{% statement tags %}`, so an inline incremental
   model fails in both modes.

This release makes the provider honour raw regions properly. Previously it left
the markers in the SQL *and* expanded the tags inside them — the worst of both
readings. Regions are now set aside before any phase runs and restored at the
end, so their contents pass through exactly as written, and a model wrapped
entirely in a raw block is rejected with an explanation rather than reaching
BigQuery as unresolved text.
