// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
// Security properties, asserted rather than assumed.
//
// This provider turns user-supplied text into SQL, into URLs, and into workflow
// definitions that later execute under a service account. Each of those is a
// boundary where a hostile — or merely unlucky — identifier can change the
// meaning of what runs. The tests here are adversarial: they try to break out,
// and fail if anything does.
//
// They run offline and need no credentials.

use gcpx_core::error::redact;
use gcpx_core::sanitize::{bq_col_ref, bq_table_ref, encode_path_segment, escape_bq_ident};

/// Identifiers that have historically broken naive quoting.
const HOSTILE: &[&str] = &[
    "a`b",                     // closes a backtick-quoted identifier
    "a``b",                    // doubled backtick
    "a\\b",                    // escape character
    "a\\`b",                   // escaped backtick
    "`; DROP TABLE users; --", // classic injection shape
    "a` UNION SELECT 1 --",
    "a'b",  // single quote
    "a\"b", // double quote
    "a\nb", // newline
    "a\rb",
    "a\tb",
    "a b",
    "a;b",
    "a--b",
    "a/*b*/c",
    "a\u{0}b",    // NUL
    "a\u{202E}b", // right-to-left override
    "a\u{FEFF}b", // zero-width no-break space
    "𝓪",          // non-BMP
    "café",       // multi-byte
    "../../etc/passwd",
    "%2e%2e%2f",
    "a?b=c",
    "a#b",
    "a&b",
];

// ── SQL identifier quoting ──────────────────────────────────────────────────

/// A quoted identifier must not be escapable.
///
/// The test is structural rather than pattern-based: after removing the escape
/// sequences the escaper is allowed to emit, no bare backtick may remain. A
/// bare backtick is what ends the identifier early and lets whatever follows be
/// read as SQL.
fn has_unescaped_backtick(escaped: &str) -> bool {
    let without_escapes = escaped.replace("\\\\", "").replace("\\`", "");
    without_escapes.contains('`')
}

#[test]
fn hostile_identifiers_cannot_escape_their_quoting() {
    for input in HOSTILE {
        let escaped = escape_bq_ident(input);
        assert!(
            !has_unescaped_backtick(&escaped),
            "identifier {input:?} escaped its quoting: {escaped:?}"
        );
    }
}

#[test]
fn table_references_stay_one_reference() {
    for input in HOSTILE {
        let reference = bq_table_ref(input, input, input);
        // A well-formed reference opens and closes exactly once.
        assert!(reference.starts_with('`') && reference.ends_with('`'));
        let interior = &reference[1..reference.len() - 1];
        assert!(
            !has_unescaped_backtick(interior),
            "table reference broke apart for {input:?}: {reference}"
        );
    }
}

#[test]
fn column_references_stay_one_reference() {
    for input in HOSTILE {
        let reference = bq_col_ref(input);
        let interior = &reference[1..reference.len() - 1];
        assert!(
            !has_unescaped_backtick(interior),
            "column reference broke apart for {input:?}: {reference}"
        );
    }
}

/// The escaper must not be its own inverse trap: escaping twice must not
/// produce something that unescapes to a different identifier.
#[test]
fn escaping_is_stable_under_repetition() {
    for input in HOSTILE {
        let once = escape_bq_ident(input).into_owned();
        let twice = escape_bq_ident(&once).into_owned();
        assert!(
            !has_unescaped_backtick(&twice),
            "double-escaping {input:?} produced an escapable string: {twice:?}"
        );
    }
}

// ── URL path encoding ───────────────────────────────────────────────────────

/// A path segment must stay one segment. An identifier that introduces a slash,
/// a query string, or a fragment would otherwise address a different resource
/// than the caller asked for.
#[test]
fn path_segments_cannot_change_the_url_shape() {
    for input in HOSTILE {
        let encoded = encode_path_segment(input);
        for forbidden in ['/', '?', '#', '&', '\\', ' ', '\n', '\r', '\t'] {
            assert!(
                !encoded.contains(forbidden),
                "encoding {input:?} left a {forbidden:?} in the path: {encoded}"
            );
        }
        assert!(
            !encoded.contains('\u{0}'),
            "encoding {input:?} left a NUL in the path"
        );
    }
}

#[test]
fn traversal_sequences_are_neutralised() {
    // A literal `../` in a path segment would climb the API's resource tree.
    for input in ["../../etc/passwd", "..%2f..", "a/../../b"] {
        let encoded = encode_path_segment(input);
        assert!(
            !encoded.contains('/'),
            "traversal survived encoding of {input:?}: {encoded}"
        );
    }
}

#[test]
fn encoding_is_reversible_to_the_original() {
    // Over-encoding is a bug too: the server must decode back to what the user
    // wrote, or the resource addressed is not the resource named.
    for input in HOSTILE {
        let encoded = encode_path_segment(input);
        let decoded = percent_decode(&encoded);
        assert_eq!(
            decoded, *input,
            "encoding {input:?} did not round-trip: {encoded}"
        );
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

// ── Credential hygiene ──────────────────────────────────────────────────────

/// Upstream error bodies echo request content, and this provider surfaces those
/// bodies to the user. Anything credential-shaped must not survive the trip.
#[test]
fn credentials_never_survive_redaction() {
    let secrets = [
        "ya29.a0AfH6SMBexampleaccesstokenvalue1234567890",
        "AIzaSyA-ExampleApiKeyValue1234567890",
        "gho_ExampleGitHubOauthToken1234567890",
        "ghp_ExamplePersonalAccessToken1234567",
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.sig",
    ];
    for secret in secrets {
        for template in [
            "request failed: {}",
            "Authorization: Bearer {}",
            "{{\"token\":\"{}\"}}",
            "line one\nline two {} line three",
        ] {
            let message = template.replace("{}", secret);
            let out = redact(&message);
            assert!(
                !out.contains(secret),
                "secret survived redaction.\n  message: {message}\n  redacted: {out}"
            );
        }
    }
}

#[test]
fn a_bearer_value_is_redacted_even_when_it_matches_no_prefix() {
    // The token after `Bearer` need not look like anything in particular.
    let out = redact("Authorization: Bearer 1//0eXaMpLeOpaqueRefreshToken");
    assert!(!out.contains("1//0eXaMpLeOpaqueRefreshToken"), "{out}");
}

#[test]
fn key_material_takes_the_whole_message_with_it() {
    // A PEM block spans many whitespace-separated tokens; scrubbing them one at
    // a time would forward most of the key.
    let pem = "config error near -----BEGIN PRIVATE KEY-----\n\
               MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQ\n\
               -----END PRIVATE KEY-----";
    let out = redact(pem);
    assert!(
        !out.contains("MIIEvQIBADANBgkqhkiG9w0"),
        "key body leaked: {out}"
    );
    assert!(!out.contains("BEGIN PRIVATE KEY"), "{out}");
}

#[test]
fn redaction_bounds_the_message_length() {
    // An unbounded upstream message would otherwise become an unbounded log
    // line, and a way to push anything into a diagnostic.
    let huge = "word ".repeat(100_000);
    assert!(redact(&huge).len() <= 520);
}

#[test]
fn ordinary_diagnostics_stay_readable() {
    // Over-redaction is its own failure: an unreadable error helps nobody.
    for message in [
        "Table example-project.ds.tbl was not found in location US",
        "Syntax error: Unexpected identifier \"SELEC\" at [1:1]",
        "Permission bigquery.tables.create denied on dataset ds",
    ] {
        assert_eq!(redact(message), message, "over-redacted: {message}");
    }
}

// ── OAuth scope isolation ───────────────────────────────────────────────────

/// Each service requests the narrowest scope it accepts. Widening one is a
/// privilege change and should be a deliberate, visible edit.
#[test]
fn services_request_least_privilege_scopes() {
    use gcpx_core::auth::ScopeSet;
    use gcpx_core::breaker::Service;
    use gcpx_core::http::scope_for;

    assert_eq!(
        scope_for(Service::BigQuery),
        ScopeSet::BigQuery,
        "BigQuery publishes a narrower scope than cloud-platform and should use it"
    );
    assert_eq!(
        ScopeSet::BigQuery.as_scopes(),
        &["https://www.googleapis.com/auth/bigquery"]
    );

    // The rest publish no narrower scope.
    for service in [
        Service::Workflows,
        Service::Scheduler,
        Service::Dataproc,
        Service::DataAgents,
        Service::Vertex,
    ] {
        assert_eq!(scope_for(service), ScopeSet::CloudPlatform, "{service:?}");
    }
}

#[test]
fn every_scope_is_a_google_oauth_scope() {
    use gcpx_core::auth::ScopeSet;
    for set in [ScopeSet::BigQuery, ScopeSet::CloudPlatform] {
        for scope in set.as_scopes() {
            assert!(
                scope.starts_with("https://www.googleapis.com/auth/"),
                "unexpected scope: {scope}"
            );
        }
    }
}

// ── Resource exhaustion ─────────────────────────────────────────────────────

/// Remote input must not be able to grow this process without bound. The chat
/// stream is the clearest case: the peer decides how much to send.
#[test]
fn an_unbounded_stream_frame_is_rejected() {
    use gcpx_agents::chat::FrameParser;
    let mut parser = FrameParser::new();
    let mut rejected = false;
    for _ in 0..256 {
        if parser.push(&vec![b'{'; 64 * 1024]).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected, "a frame that never closes must be rejected");
}

#[test]
fn macro_expansion_cannot_run_away() {
    use gcpx_dbt::macros::{expand_macros, MacroDef};
    use std::collections::BTreeMap;

    // Self-reference, mutual reference, and a fan-out that doubles each pass.
    let mut macros = BTreeMap::new();
    macros.insert(
        "loop".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ loop() }}".to_owned(),
        },
    );
    macros.insert(
        "ping".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ pong() }}".to_owned(),
        },
    );
    macros.insert(
        "pong".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ ping() }}".to_owned(),
        },
    );
    macros.insert(
        "grow".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ grow() }} {{ grow() }}".to_owned(),
        },
    );

    for entry in ["{{ loop() }}", "{{ ping() }}", "{{ grow() }}"] {
        let result = expand_macros(entry, &macros);
        assert!(result.is_err(), "{entry} should not expand indefinitely");
    }
}

#[test]
fn oversized_macro_input_is_rejected() {
    use gcpx_dbt::macros::{expand_macros, MacroDef};
    use std::collections::BTreeMap;
    let mut macros = BTreeMap::new();
    macros.insert(
        "noop".to_owned(),
        MacroDef {
            args: vec![],
            sql: "1".to_owned(),
        },
    );
    let huge = "x".repeat(2 * 1024 * 1024);
    assert!(
        expand_macros(&huge, &macros).is_err(),
        "input beyond the cap should be rejected rather than processed"
    );
}

// ── Generated statements ────────────────────────────────────────────────────

/// Whatever the identifiers, generated DDL must reference exactly the table it
/// was asked to, and must not acquire extra statements.
#[test]
fn generated_ddl_cannot_be_extended_through_identifiers() {
    use gcpx_bq::schema::ddl::build_batch_ddl;
    use gcpx_bq::schema::types::DdlOp;

    for hostile in HOSTILE {
        let ops = vec![
            DdlOp::DropColumn { name: hostile },
            DdlOp::AddColumn {
                name: hostile,
                field_type: "STRING",
                mode: "NULLABLE",
                description: hostile,
                default_value_expression: None,
                rounding_mode: None,
            },
        ];
        for statement in build_batch_ddl(hostile, hostile, hostile, &ops, &[]) {
            // Every statement this builder emits is a single ALTER TABLE.
            assert!(
                statement.starts_with("ALTER TABLE "),
                "unexpected statement shape for {hostile:?}: {statement}"
            );
            assert_eq!(
                statement.matches("ALTER TABLE ").count(),
                1,
                "a second statement was smuggled in via {hostile:?}: {statement}"
            );
        }
    }
}

#[test]
fn snapshot_sql_quotes_every_identifier_it_interpolates() {
    use gcpx_snapshot::ddl::generate_snapshot_create_ddl;
    use gcpx_snapshot::types::SnapshotInputs;

    for hostile in HOSTILE {
        let inputs = SnapshotInputs {
            project: "p",
            region: "us-central1",
            dataset: "d",
            name: "snap",
            source_sql: "SELECT 1 AS id, CURRENT_TIMESTAMP() AS ts",
            unique_key: hostile,
            strategy: "timestamp",
            updated_at: hostile,
            schedule: "0 2 * * *",
            time_zone: "UTC",
            service_account: "sa@example.iam.gserviceaccount.com",
            description: None,
            paused: None,
            invalidate_hard_deletes: None,
            auto_optimize: Some(true),
            source_schema: None,
        };
        let ddl = generate_snapshot_create_ddl(&inputs);
        // The identifier appears only inside backticks; the escaped form is
        // what must be present, never the raw one standing alone.
        let escaped = escape_bq_ident(hostile);
        assert!(
            ddl.contains(&format!("`{escaped}`")),
            "identifier {hostile:?} was not quoted in the snapshot DDL"
        );
    }
}
