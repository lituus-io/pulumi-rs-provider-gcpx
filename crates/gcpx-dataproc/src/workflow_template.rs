// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

/// Spark BigQuery connector JAR — auto-appended when BigQuery is involved.
const SPARK_BQ_JAR: &str =
    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar";

/// A fingerprint of a generated workflow definition.
///
/// The definition is derived state: it is built by this provider from the
/// resource's inputs, and none of it is stored as an input. So when the
/// generator itself changes — a fix to how the batch id is written, say — the
/// inputs are identical, `Diff` sees nothing, and the stack keeps running the
/// old definition. Recording a fingerprint alongside the workflow makes the
/// generator's own output part of what is compared, so a corrected template
/// reaches deployed stacks instead of waiting for an unrelated edit.
pub fn definition_fingerprint(definition: &str) -> String {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    definition.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Sanitize a name for use as a Dataproc batch ID.
/// Batch IDs must be lowercase, alphanumeric, and hyphens only.
pub fn sanitize_batch_id(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

/// Build the args array for an ingest job writing to GCS.
pub fn build_ingest_gcs_args<'a>(
    secret: &'a str,
    landing_bucket: &'a str,
    query: &'a str,
    jdbc_uri: &'a str,
    process_bucket: &'a str,
) -> Vec<&'a str> {
    vec![secret, landing_bucket, query, jdbc_uri, process_bucket]
}

/// Build the args array for an ingest job writing to BigQuery.
pub fn build_ingest_bq_args<'a>(
    secret: &'a str,
    query: &'a str,
    jdbc_uri: &'a str,
    process_bucket: &'a str,
    dest_project: &'a str,
    dest_dataset: &'a str,
    dest_table: &'a str,
) -> Vec<&'a str> {
    vec![
        secret,
        query,
        jdbc_uri,
        process_bucket,
        dest_project,
        dest_dataset,
        dest_table,
    ]
}

/// Build the args array for an export job.
pub fn build_export_args<'a>(
    process_bucket: &'a str,
    source_dataset: &'a str,
    source_table: &'a str,
    jdbc_uri: &'a str,
    secret: &'a str,
    dest_table: &'a str,
) -> Vec<&'a str> {
    vec![
        process_bucket,
        source_dataset,
        source_table,
        jdbc_uri,
        secret,
        dest_table,
    ]
}

/// Generate a GCP Workflow YAML for a Dataproc Serverless batch job.
///
/// The workflow:
/// 1. init: generates UUID and batchId
/// 2. spark_execution: POSTs to Dataproc Serverless batches API
/// 3. returnOutput: returns the batch UUID
///
/// `batches.create` starts a long-running operation, so the response body is an
/// Operation and not the Batch itself. The uuid lives under `metadata`; reading
/// it from the top level fails the execution *after* the batch has already been
/// submitted, which is the confusing half — the work runs, the workflow reports
/// failure, and the scheduler retries it.
#[allow(clippy::too_many_arguments)]
pub fn generate_dataproc_workflow_yaml(
    project: &str,
    region: &str,
    batch_id_prefix: &str,
    image_uri: &str,
    script_uri: &str,
    jar_uris: &[&str],
    args: &[&str],
    runtime_version: &str,
    service_account: &str,
    staging_bucket: &str,
    subnetwork_uri: &str,
    network_tags: &[&str],
    needs_bq_jar: bool,
) -> String {
    let sanitized = sanitize_batch_id(batch_id_prefix);

    // Build jarFileUris array
    let mut all_jars: Vec<&str> = jar_uris.to_vec();
    if needs_bq_jar && !all_jars.iter().any(|j| j.contains("spark-bigquery")) {
        all_jars.push(SPARK_BQ_JAR);
    }
    let jar_array = format_yaml_string_array(&all_jars);

    // Build args array
    let args_array = format_yaml_string_array(args);

    // Build networkTags array
    let tags_array = format_yaml_string_array(network_tags);

    // The batches API validates `stagingBucket` against a bare bucket-name
    // pattern and rejects a gs:// URI. Every other bucket field on these
    // resources is a path, so writing this one as a URI is the natural mistake
    // to make; strip the scheme rather than make the caller remember.
    let staging_bucket = staging_bucket
        .strip_prefix("gs://")
        .unwrap_or(staging_bucket)
        .trim_end_matches('/');

    // Dataproc Serverless runs the stock runtime unless a custom image is
    // given. Emitting `containerImage: ""` is not the same as omitting it —
    // the API reads the empty string as an image reference and rejects it — so
    // the line has to disappear entirely when there is nothing to put in it.
    let container_image_line = if image_uri.is_empty() {
        String::new()
    } else {
        format!("\n                containerImage: \"{image_uri}\"")
    };

    format!(
        r#"- init:
    assign:
        - uuid: ${{sys.get_env("GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID")}}
        - short_uuid: ${{text.substring(uuid, 0, 8)}}
        - batchId: ${{"{sanitized}-" + short_uuid}}
- spark_execution:
    call: http.post
    args:
        url: https://dataproc.googleapis.com/v1/projects/{project}/locations/{region}/batches
        query:
            batchId: ${{batchId}}
        auth:
            type: OAuth2
        body:
            pysparkBatch:
                mainPythonFileUri: "{script_uri}"
                jarFileUris: {jar_array}
                args: {args_array}
            runtimeConfig:
                version: "{runtime_version}"{container_image_line}
            environmentConfig:
                executionConfig:
                    serviceAccount: "{service_account}"
                    stagingBucket: "{staging_bucket}"
                    subnetworkUri: "{subnetwork_uri}"
                    networkTags: {tags_array}
    result: batchResult
- returnOutput:
    return: ${{batchResult.body.metadata.batchUuid}}"#
    )
}

/// Format a string slice as a YAML inline array: ["a", "b"]
fn format_yaml_string_array(items: &[&str]) -> String {
    if items.is_empty() {
        return "[]".to_owned();
    }
    let inner: Vec<String> = items.iter().map(|s| format!("\"{s}\"")).collect();
    format!("[{}]", inner.join(", "))
}

#[cfg(test)]
mod tests {

    /// `batches.create` returns a long-running Operation, not a Batch. Reading
    /// `body.uuid` fails with a KeyError after the batch has already been
    /// submitted: the work runs, the execution reports failure, and the
    /// scheduler retries it — submitting the same job again.
    #[test]
    fn the_batch_uuid_is_read_from_the_operation_metadata() {
        let yaml = generate_dataproc_workflow_yaml(
            "p",
            "r",
            "b",
            "",
            "gs://b/j.py",
            &[],
            &[],
            "2.2",
            "sa",
            "gs://s",
            "sub",
            &[],
            false,
        );
        assert!(
            yaml.contains("${batchResult.body.metadata.batchUuid}"),
            "the uuid is not read from the operation metadata:\n{yaml}"
        );
        assert!(!yaml.contains("${batchResult.body.uuid}"));
    }

    /// Cloud Workflows does not interpolate `${...}` inside a quoted string —
    /// the expression has to be the whole value. Writing the batch id as
    /// `"name-${short_uuid}"` sent the literal text to Dataproc, which rejected
    /// every batch for an invalid id. No batch this provider submitted could
    /// ever have started.
    #[test]
    fn the_batch_id_is_an_expression_not_a_quoted_string() {
        let yaml = generate_dataproc_workflow_yaml(
            "p",
            "r",
            "My Job",
            "",
            "gs://b/j.py",
            &[],
            &[],
            "2.2",
            "sa",
            "gs://s",
            "sub",
            &[],
            false,
        );
        assert!(
            yaml.contains(r#"batchId: ${"my-job-" + short_uuid}"#),
            "batch id is not an evaluated expression:\n{yaml}"
        );
        assert!(
            !yaml.contains(r#""my-job-${short_uuid}""#),
            "the literal-string form is still present"
        );
    }

    /// The batches API validates the staging bucket against a bare bucket-name
    /// pattern and rejects a gs:// URI.
    #[test]
    fn the_staging_bucket_loses_its_scheme() {
        let yaml = generate_dataproc_workflow_yaml(
            "p",
            "r",
            "b",
            "",
            "gs://b/j.py",
            &[],
            &[],
            "2.2",
            "sa",
            "gs://my-bucket/",
            "sub",
            &[],
            false,
        );
        assert!(yaml.contains(r#"stagingBucket: "my-bucket""#), "{yaml}");
    }

    /// Dataproc Serverless runs the stock runtime when no custom image is
    /// given, and `containerImage: ""` is not the same as omitting the field —
    /// the API reads the empty string as an image reference and rejects the
    /// batch. Requiring an image meant every job needed one built and pushed
    /// first, for no reason.
    #[test]
    fn no_image_means_no_container_image_line() {
        let yaml = generate_dataproc_workflow_yaml(
            "p",
            "r",
            "b",
            "",
            "gs://b/j.py",
            &[],
            &["a"],
            "2.2",
            "sa",
            "gs://s",
            "sub",
            &[],
            false,
        );
        assert!(
            !yaml.contains("containerImage"),
            "an empty image must not emit the field at all:\n{yaml}"
        );
        assert!(yaml.contains("version: \"2.2\""));
    }

    #[test]
    fn an_image_is_still_emitted_when_given() {
        let yaml = generate_dataproc_workflow_yaml(
            "p",
            "r",
            "b",
            "gcr.io/x/y:1",
            "gs://b/j.py",
            &[],
            &["a"],
            "2.2",
            "sa",
            "gs://s",
            "sub",
            &[],
            false,
        );
        assert!(yaml.contains("containerImage: \"gcr.io/x/y:1\""));
    }

    use super::*;

    #[test]
    fn test_sanitize_batch_id() {
        assert_eq!(sanitize_batch_id("my_job"), "my-job");
        assert_eq!(sanitize_batch_id("My-Job_123"), "my-job-123");
        assert_eq!(sanitize_batch_id("UPPER"), "upper");
    }

    #[test]
    fn test_generate_yaml_contains_dataproc_api() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "test-job",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &["arg1"],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &["spark"],
            false,
        );
        assert!(yaml.contains("dataproc.googleapis.com"));
    }

    #[test]
    fn test_generate_yaml_contains_batch_id() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "my_test",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &["spark"],
            false,
        );
        assert!(yaml.contains("my-test"));
    }

    #[test]
    fn test_generate_yaml_single_jar() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &["gs://jars/ojdbc.jar"],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            false,
        );
        assert!(yaml.contains("gs://jars/ojdbc.jar"));
    }

    #[test]
    fn test_generate_yaml_multiple_jars() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &["gs://jars/a.jar", "gs://jars/b.jar"],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            false,
        );
        assert!(yaml.contains("gs://jars/a.jar"));
        assert!(yaml.contains("gs://jars/b.jar"));
    }

    #[test]
    fn test_generate_yaml_no_jars() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            false,
        );
        assert!(yaml.contains("jarFileUris: []"));
    }

    #[test]
    fn test_generate_yaml_args_order() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &["first", "second", "third"],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            false,
        );
        assert!(yaml.contains(r#"args: ["first", "second", "third"]"#));
    }

    #[test]
    fn test_generate_yaml_network_tags() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &["spark", "internal"],
            false,
        );
        assert!(yaml.contains(r#"networkTags: ["spark", "internal"]"#));
    }

    #[test]
    fn test_generate_yaml_special_chars_in_query() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &[],
            &["SELECT * FROM t WHERE name = 'O\\'Brien'"],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            false,
        );
        assert!(yaml.contains("O\\'Brien"));
    }

    #[test]
    fn test_generate_yaml_auto_appends_bq_jar() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &["gs://jars/ojdbc.jar"],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            true,
        );
        assert!(yaml.contains("spark-bigquery"));
        assert!(yaml.contains("ojdbc.jar"));
    }

    #[test]
    fn test_generate_yaml_no_duplicate_bq_jar() {
        let yaml = generate_dataproc_workflow_yaml(
            "proj",
            "us-central1",
            "job",
            "gcr.io/img",
            "gs://script.py",
            &["gs://jars/spark-bigquery-custom.jar"],
            &[],
            "2.2",
            "sa@iam",
            "staging",
            "subnet",
            &[],
            true,
        );
        // Should not add duplicate spark-bigquery jar
        let count = yaml.matches("spark-bigquery").count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_build_ingest_gcs_args() {
        let args = build_ingest_gcs_args(
            "secret",
            "gs://bucket",
            "SELECT 1",
            "jdbc:oracle:",
            "process",
        );
        assert_eq!(
            args,
            vec![
                "secret",
                "gs://bucket",
                "SELECT 1",
                "jdbc:oracle:",
                "process"
            ]
        );
    }

    #[test]
    fn test_build_ingest_bq_args() {
        let args = build_ingest_bq_args(
            "secret",
            "SELECT 1",
            "jdbc:oracle:",
            "process",
            "proj",
            "ds",
            "tbl",
        );
        assert_eq!(
            args,
            vec![
                "secret",
                "SELECT 1",
                "jdbc:oracle:",
                "process",
                "proj",
                "ds",
                "tbl"
            ]
        );
    }

    #[test]
    fn test_build_export_args() {
        let args = build_export_args(
            "process",
            "ds",
            "tbl",
            "jdbc:sqlserver:",
            "secret",
            "dest.dbo.t",
        );
        assert_eq!(
            args,
            vec![
                "process",
                "ds",
                "tbl",
                "jdbc:sqlserver:",
                "secret",
                "dest.dbo.t"
            ]
        );
    }
}
