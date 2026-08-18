// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Replay of a real chat stream.
//!
//! The fixture is an actual response captured from the live API — a deployed
//! agent answering "What was total revenue by region?" over its grounded table.
//! Only the project id is scrubbed.
//!
//! Replaying it offline is what makes the stream parser testable in CI without
//! credentials, and it is how the shape stays honest: the first version of this
//! code guessed the wrong pointers for both the generated SQL and the answer,
//! and only a real response showed it.

use gcpx_agents::chat::{ChatOutcome, FrameParser};

const FIXTURE: &str = include_str!("fixtures/chat_stream.json");

/// Feed the fixture through the parser the way bytes arrive from the network.
fn parse_in_chunks(chunk_size: usize) -> Vec<serde_json::Value> {
    let mut parser = FrameParser::new();
    let mut out = Vec::new();
    for chunk in FIXTURE.as_bytes().chunks(chunk_size) {
        out.extend(parser.push(chunk).expect("parse"));
    }
    out
}

fn outcome_from(values: &[serde_json::Value]) -> ChatOutcome {
    let mut o = ChatOutcome::default();
    for v in values {
        o.absorb(v);
    }
    o
}

#[test]
fn a_real_stream_parses_into_its_messages() {
    let msgs = parse_in_chunks(FIXTURE.len());
    assert_eq!(msgs.len(), 10, "the captured stream carries ten messages");
}

/// The property that matters for a network parser: where the chunk boundaries
/// fall must not change the result. Exercised across sizes that split inside
/// strings, inside escapes, and mid-UTF-8.
#[test]
fn chunking_never_changes_the_result() {
    let whole = parse_in_chunks(FIXTURE.len());
    for size in [1, 2, 3, 7, 13, 64, 512, 4096] {
        assert_eq!(
            parse_in_chunks(size),
            whole,
            "chunk size {size} produced a different parse"
        );
    }
}

#[test]
fn the_generated_sql_is_extracted() {
    let o = outcome_from(&parse_in_chunks(97));
    let sql = o.sql_text();
    assert!(
        !sql.is_empty(),
        "no SQL captured from a stream that contains it"
    );
    assert!(sql.to_uppercase().contains("SELECT"), "{sql}");
    assert!(sql.contains("daily_revenue"), "{sql}");
}

/// Table assertions are how a golden query says "the agent used the right
/// data", so this has to work on a real stream, not just a synthetic one.
#[test]
fn table_references_are_detected_in_real_sql() {
    let o = outcome_from(&parse_in_chunks(64));
    assert!(o.references_table("daily_revenue"));
    assert!(o.references_table("example-project.gcpx_quickstart.daily_revenue"));
    assert!(!o.references_table("some_other_table"));
}

/// The correction a real response forced: the stream interleaves reasoning with
/// the answer, both as `text.parts`, and only `textType` separates them.
#[test]
fn reasoning_is_kept_out_of_the_answer() {
    let o = outcome_from(&parse_in_chunks(31));

    assert!(
        !o.answer.contains("Analyzing context"),
        "the agent's thinking leaked into the answer: {}",
        o.answer
    );
    assert!(
        o.answer.contains("west") || o.answer.contains("revenue"),
        "the answer is missing its content: {}",
        o.answer
    );
    assert!(
        !o.thoughts.is_empty(),
        "the reasoning should still be captured, just separately"
    );
    assert!(o.thoughts.iter().any(|t| t.contains("Analyzing context")));
}

#[test]
fn a_clean_stream_reports_no_error() {
    let o = outcome_from(&parse_in_chunks(256));
    assert!(o.error.is_none(), "unexpected error: {:?}", o.error);
}

/// An error arrives as a message in the same array, so it has to be recognised
/// rather than parsed as an answer.
#[test]
fn an_error_message_in_the_stream_is_recognised() {
    // Exactly the shape the API returned for a malformed chat request.
    let stream = r#"[{
      "error": { "code": 400, "message": "Invalid resource field value in the request.", "status": "INVALID_ARGUMENT" }
    }]"#;
    let mut parser = FrameParser::new();
    let msgs = parser.push(stream.as_bytes()).unwrap();
    let o = outcome_from(&msgs);
    assert_eq!(
        o.error.as_deref(),
        Some("Invalid resource field value in the request.")
    );
}

/// The request shape, which took three attempts to get right against the live
/// API and fails with a message naming neither the field nor the expected form.
#[test]
fn the_chat_request_puts_the_agent_context_at_the_top_level() {
    let body = gcpx_agents::chat::build_chat_body(
        "p",
        "global",
        "projects/p/locations/global/dataAgents/a",
        "what is revenue?",
    );
    assert_eq!(
        body["dataAgentContext"]["dataAgent"],
        "projects/p/locations/global/dataAgents/a"
    );
    assert!(
        body.get("conversationReference").is_none(),
        "nesting the agent context is rejected by the API"
    );
    assert_eq!(body["parent"], "projects/p/locations/global");
    assert_eq!(
        body["messages"][0]["userMessage"]["text"],
        "what is revenue?"
    );
}
