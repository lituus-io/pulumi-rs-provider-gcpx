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

### Fixed after 0.1.0

Found by holding every resource to "deploy, change nothing, preview again":

- `Diff` compared the *outputs* of the last deploy against the *inputs* of the
  next one, so every computed field — anything the service assigns, any optional
  input with a default — sat on one side of the comparison and could never
  appear on the other. Each of those read as a change on every preview, forever.
  Diffing now uses the engine's `old_inputs`, which exists for exactly this, and
  falls back to the outputs only for state written before it.
- `Conversation` made that mistake destructively: its API has no update, so its
  diff declares a *replace*, and it compared whole structs including `name` and
  `createTime`. The conversation would have been destroyed and recreated on
  every deploy.
- `AgentEngine` and `Memory` had the inverse bug — they compared a subset of
  their inputs, so editing `env`, `pythonVersion`, `scope`, `displayName` or
  `description` was reported as nothing to do and silently discarded.
- A model's detailed diff always named `sql`, whichever property had actually
  changed. It now names the property that changed.

Found by holding each resource to its own schema:

- `DataAgent` stored eight of its twenty declared outputs. `${agent.models}` —
  the documented way to ground an agent — resolved to nothing, as did `tables`,
  `systemInstruction`, `publish`, `labels`, `kmsKey`, `exampleQueries` and
  `glossaryTerms`. `Memory` was missing `scope`, `displayName` and `description`.
  Outputs are now the declared inputs as written plus whatever the service
  computed.
- `authoritative` was defaulted when writing outputs but not in `Check`, which
  is where Pulumi expects a provider to fill in the defaults it intends to apply.

Found by deploying the example into a project that already held its tables:

- Adopting an existing table sent `clustering: {"fields": []}`, which BigQuery
  reads as clustering enabled with nothing to cluster by and rejects. An empty
  list now clears the field with `null`. The create path had always guarded
  this; only the adopt path did not, so it failed exactly when a table already
  existed.

Found by using a documented feature:

- `{{ var('x') }}` and the whole `dbt_utils.*` family were rejected at `Check`
  as unregistered macros. Both are built-ins the preprocessor resolves itself,
  which validation did not know — so a project could declare `vars` and no model
  could use them.

Found by deploying a Dataproc Serverless batch, which had never been run:

- The generated workflow wrote the batch id as `"name-${short_uuid}"`. Cloud
  Workflows does not interpolate inside a quoted string — the expression has to
  be the whole value — so Dataproc received the literal text and rejected every
  batch for an invalid id. **No batch this provider submitted could ever have
  started.**
- `stagingBucket` was passed through as a `gs://` URI. The batches API validates
  it against a bare bucket-name pattern and rejects a URI. The scheme is now
  stripped, since every other bucket field on these resources is a path and
  writing this one as a URI is the natural mistake.
- `returnOutput` read `body.uuid`, but `batches.create` starts a long-running
  operation, so the response is an Operation and the uuid is under `metadata`.
  The execution failed *after* submitting the batch — the work ran, the workflow
  reported failure, and the scheduler would have retried it.
- `imageUri` was required. Dataproc Serverless runs the stock runtime for the
  declared `runtimeVersion` unless a custom image is given, so every job needed
  an image built and pushed for no reason. It is now optional, and an empty
  value omits `containerImage` entirely rather than sending `""`, which the API
  reads as an image reference and rejects.
- The workflow definition is derived from the inputs and stored nowhere, so a
  change to how it is generated was invisible to `Diff`: correcting the template
  left every deployed stack running the old one. A `definitionFingerprint`
  output now records what the generator produced, and state written without one
  is treated as stale so a corrected definition reaches existing stacks as a
  single in-place update.

Verified live: a PySpark batch submitted through `IngestJob` reaches Dataproc
Serverless, runs, and succeeds, and the stack previews clean afterwards.

### Build and CI

The release binary is 2.85 MiB, down from 4.51 MiB — 36.8% smaller. Almost all
of it is code that runs once per request: TLS, HTTP/2, gRPC, the generated proto
types. Compiling that bulk for size costs nothing that can be measured. The dbt
template pipeline is the exception and is the one measured hot path, so
`gcpx-dbt` and `gcpx-core` keep full optimisation via per-package overrides:
built for size they run about three times slower — scanning drops from ~1.4
GiB/s to ~0.5 — to save tens of kilobytes. CI now gates the size rather than
merely reporting it, so a dependency cannot quietly add a megabyte.

The coverage gate was set to 85% and the tree had never reached it — the first
run that actually executed measured 77.89%, with several BigQuery handler
modules at 0% because nothing outside the live suite touched them and that does
not run in CI. Those handlers are covered now, directly, against the client
doubles: BigQuery dataset, table, routine and schema; the five agent resources;
snapshots; and scheduled SQL. Full lifecycles — Check, Create, Read, Update,
Delete — plus the paths that only appear when something goes wrong: an adopt on
409, a delete of something already gone, a rollback when the second half of a
paired resource fails.

That took the tree from 77.89% to 85.72%, so the gate is met rather than
aspired to. Raising it further means covering the `ops.rs` modules, which are
HTTP clients and need a stubbed transport rather than a client double.

Writing those tests turned up two things worth recording. Schema evolution is
declarative, not inferred: listing a new column does not add it, `alter: insert`
does, which is what makes re-applying an unchanged schema a no-op. And deleting
a resource that is already gone fails, across every resource — `verified_delete`
propagates the delete error and only treats a 404 from the *poll* as
confirmation. That means a resource removed out of band blocks `pulumi destroy`
until state is edited by hand. The current behaviour is pinned by a test so a
change to it is deliberate; whether it should change is a decision about destroy
semantics across the provider.

Four more failures only appeared once the workflows genuinely ran — three had
been latent in them from the start:

- `cargo bench --workspace` runs the plain libtest harness of every crate with
  no criterion bench, and those reject `--save-baseline`; the run died on the
  first one. Only the criterion target is named now.
- `shasum` ships with macOS and most Linux images but not the Windows runner,
  where release packaging died after the build had already succeeded.
- The binary-size ceiling had been calibrated on a developer machine. The same
  source links materially larger on Linux — 4.28 MiB against 2.85 — so the gate
  failed a build that had regressed nothing.
- And the conformance suite, newly added here, runs under `cargo test
  --workspace`, which does not build another package's binary. It is ignored by
  default now and the conformance job asks for it explicitly, having built what
  it needs.

Three CI jobs referenced things that did not exist and would have failed on
their first run:

- The `conformance` job ran `--test conformance` against an empty directory and
  no declared target. The suite now exists and exercises the real binary: it
  launches the plugin, reads the port from stdout, and asserts the handshake
  works with no credentials present, that the schema survives the gRPC round
  trip, and that every resource token the schema advertises is dispatchable.
- The `allocations` job passed `--features alloc-ceilings`, which no crate
  declared. There is now a counting allocator behind that feature, asserting
  that scanning does not allocate and that preprocessing allocates per pass
  rather than per reference — measured at 7 allocations whether a model holds 8
  references or 256.
- `fuzz.yml` pointed at a `fuzz/` directory holding nothing. Five targets now
  cover the surfaces that take arbitrary input: the template pipeline, the
  scanner, macro expansion, the chat stream parser, and identifier quoting.

Also: `python/build/` was tracked, which put a 4.7 MB copy of the plugin binary
in the history of a repository that builds it from source. It is untracked and
ignored. The hygiene scan that should have caught tokens in it was reading the
working tree including build output — a compiled binary matches almost any short
token by accident — and now reads tracked text files, which is what ships.

### Known gaps

- `schemaRelationships` and `userFunctions` are not sent. Both message names
  exist, but no accepted subfield spelling could be determined, and an
  unverified shape fails the deploy. This also means BigQuery routines cannot
  yet be wired into an agent as callable functions.
- `IngestJob` and `ExportJob` are shaped around a JDBC source or sink: both
  require a connection string, a Secret Manager secret and a database type, and
  build a fixed argv for the PySpark script. There is no resource for a plain
  Spark batch, so running one means supplying those fields and ignoring them in
  the script.

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
