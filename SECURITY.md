# Security Policy

## Reporting Vulnerabilities

Report security vulnerabilities privately to **spicyzhug@gmail.com**. Include a description,
reproduction steps, and impact assessment. You will receive an acknowledgement within 48 hours.

Do **not** open public issues for security vulnerabilities.

## Security Measures

### Dependency auditing

- **cargo-audit** runs weekly and on every push/PR to detect known CVEs in dependencies.
- **cargo-deny** checks licenses, bans unsafe crates, and detects duplicate dependencies.
- All CI jobs build with `--locked` against a committed `Cargo.lock`, so a resolution change
  cannot slip in unreviewed.

### Static analysis

- **Clippy** runs with `-D warnings` on every push/PR.
- Security-focused Clippy lints (`unwrap_used`, `expect_used`, `panic`) run in the security
  workflow as advisory warnings.
- The crate contains no `unsafe` code.

### Injection defence

This provider generates SQL DDL, workflow YAML, and URL paths from user-supplied identifiers.
Three escaping boundaries are enforced and property-tested against adversarial input:

- `escape_bq_ident` / `bq_table_ref` — BigQuery backtick-quoted identifiers.
- `encode_path_segment` — percent-encoding for GCP REST path segments.
- YAML block-scalar escaping for generated workflow definitions.

### Fuzzing

Parsers and generators are fuzzed weekly with `cargo-fuzz`. A curated seed corpus is kept under
version control; the evolved corpus is cached between runs so coverage deepens over time.

### Credentials

Access tokens are cached per OAuth scope set and never logged, never included in error messages,
and never surfaced in resource outputs.
