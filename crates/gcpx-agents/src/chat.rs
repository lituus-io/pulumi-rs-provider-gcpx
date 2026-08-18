// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The streaming chat surface.
//!
//! `:chat` answers a question with a *stream* of messages: the agent's plan,
//! the SQL it generated, the data it retrieved, and finally its answer. The
//! response is an incrementally-delivered JSON array, and it is unbounded in
//! principle — an agent asked a broad question can emit a great deal.
//!
//! So it is never collected into a `String` first. Buffering would hand a
//! remote peer control of this process's memory, and would also defeat the
//! point: the interesting content (the generated SQL) usually arrives long
//! before the stream ends.
//!
//! [`FrameParser`] pulls whole JSON values out of a byte stream as they
//! complete. It is a small state machine rather than a `serde` call because
//! chunk boundaries fall anywhere — including inside a string, inside an
//! escape sequence, or between the two bytes of a UTF-8 character.

use gcpx_core::error::GcpError;

/// Hard ceiling on a single JSON value.
///
/// Bounds memory even when the peer never closes a value. 8 MiB is far above
/// any real message and far below anything that threatens the process.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Pulls complete JSON values out of a streamed JSON array.
#[derive(Default)]
pub struct FrameParser {
    buf: Vec<u8>,
    /// Brace/bracket nesting depth of the value being accumulated.
    depth: usize,
    in_string: bool,
    escaped: bool,
    /// Byte offset where the current value began, once one has started.
    value_start: Option<usize>,
    overflowed: bool,
}

impl FrameParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk and return every value that completed within it.
    ///
    /// A chunk may contain no complete value, several, or the tail of one that
    /// began in an earlier chunk.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<serde_json::Value>, GcpError> {
        if self.overflowed {
            return Err(self.overflow_error());
        }

        let mut out = Vec::new();
        for &byte in chunk {
            // Only an opening brace starts a message. The array's own brackets
            // and the commas between elements are structural and are skipped
            // rather than buffered — treating `[` as a value start would make
            // the entire array one frame, which is the opposite of streaming.
            if self.value_start.is_none() && byte != b'{' {
                continue;
            }

            if self.value_start.is_none() {
                self.value_start = Some(self.buf.len());
            }
            self.buf.push(byte);

            if self.buf.len() > MAX_FRAME_BYTES {
                self.overflowed = true;
                return Err(self.overflow_error());
            }

            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self.depth.saturating_sub(1);
                    if self.depth == 0 {
                        let start = self.value_start.take().unwrap_or(0);
                        // Only valid UTF-8 can be valid JSON; a split multi-byte
                        // character cannot reach here because the value is only
                        // parsed once its braces balance.
                        if let Ok(text) = std::str::from_utf8(&self.buf[start..]) {
                            if let Ok(value) = serde_json::from_str(text) {
                                out.push(value);
                            }
                        }
                        self.buf.clear();
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }

    fn overflow_error(&self) -> GcpError {
        GcpError::Api {
            status: 413,
            message: format!("chat response frame exceeded {MAX_FRAME_BYTES} bytes"),
        }
    }

    /// Whether a partially-accumulated value is still pending.
    pub fn is_mid_value(&self) -> bool {
        self.value_start.is_some()
    }
}

/// What the provider needs out of a chat stream.
///
/// The wire carries considerably more; only the parts an evaluation asserts on
/// are extracted, so a new message type upstream cannot break parsing.
#[derive(Debug, Default, Clone)]
pub struct ChatOutcome {
    /// SQL the agent generated, in order.
    pub generated_sql: Vec<String>,
    /// Natural-language text the agent produced.
    pub answer: String,
    /// Error text, when the agent reported one.
    pub error: Option<String>,
}

impl ChatOutcome {
    /// Fold one streamed message into the outcome.
    pub fn absorb(&mut self, value: &serde_json::Value) {
        if let Some(err) = value.pointer("/error/message").and_then(|v| v.as_str()) {
            self.error = Some(err.to_owned());
        }
        // The SQL appears under systemMessage/data/generatedSql in the shapes
        // observed; the alternate spelling is accepted so a rename upstream
        // degrades to "no SQL captured" rather than a hard failure.
        for pointer in [
            "/systemMessage/data/generatedSql",
            "/systemMessage/data/query/sql",
            "/generatedSql",
        ] {
            if let Some(sql) = value.pointer(pointer).and_then(|v| v.as_str()) {
                if !sql.is_empty() {
                    self.generated_sql.push(sql.to_owned());
                }
            }
        }
        for pointer in ["/systemMessage/text/parts", "/text/parts"] {
            if let Some(parts) = value.pointer(pointer).and_then(|v| v.as_array()) {
                for part in parts.iter().filter_map(|p| p.as_str()) {
                    if !self.answer.is_empty() {
                        self.answer.push(' ');
                    }
                    self.answer.push_str(part);
                }
            }
        }
    }

    /// All generated SQL as one string, for matching against expectations.
    pub fn sql_text(&self) -> String {
        self.generated_sql.join("\n")
    }

    /// Whether the SQL references a table, matched on the unqualified name so a
    /// user can write `mart_revenue` rather than the full path.
    pub fn references_table(&self, table: &str) -> bool {
        let needle = table.rsplit('.').next().unwrap_or(table);
        if needle.is_empty() {
            return false;
        }
        self.sql_text()
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
    }
}

/// The request body for one question.
pub fn build_chat_body(
    project: &str,
    location: &str,
    agent: &str,
    question: &str,
) -> serde_json::Value {
    serde_json::json!({
        "parent": format!("projects/{project}/locations/{location}"),
        "messages": [{ "userMessage": { "text": question } }],
        "conversationReference": {
            "dataAgentContext": { "dataAgent": agent }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(parser: &mut FrameParser, chunks: &[&str]) -> Vec<serde_json::Value> {
        let mut all = Vec::new();
        for c in chunks {
            all.extend(parser.push(c.as_bytes()).unwrap());
        }
        all
    }

    #[test]
    fn extracts_values_from_a_single_chunk() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"a":1},{"b":2}]"#]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["a"], 1);
        assert_eq!(out[1]["b"], 2);
    }

    #[test]
    fn reassembles_a_value_split_across_chunks() {
        // The defining case: chunk boundaries fall wherever the network puts
        // them, not where the JSON structure would prefer.
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"a"#, r#"":1,"b":"#, r#"2}]"#]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["a"], 1);
        assert_eq!(out[0]["b"], 2);
    }

    #[test]
    fn splits_inside_a_string_do_not_confuse_the_parser() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"sql":"SELECT {"#, r#"} FROM t"}]"#]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["sql"], "SELECT {} FROM t");
    }

    #[test]
    fn braces_inside_strings_are_not_structural() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"q":"a } b ] c"},{"n":1}]"#]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["q"], "a } b ] c");
    }

    #[test]
    fn escaped_quotes_do_not_end_a_string() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"q":"say \"hi\" }"}]"#]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["q"], r#"say "hi" }"#);
    }

    #[test]
    fn escaped_backslash_before_a_quote_still_closes_the_string() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"q":"back\\"},{"n":2}]"#]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["q"], "back\\");
    }

    #[test]
    fn nested_objects_complete_only_at_the_outer_brace() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"a":{"b":{"c":1}}}]"#]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["a"]["b"]["c"], 1);
    }

    #[test]
    fn split_utf8_sequences_survive_reassembly() {
        // A multi-byte character can be cut in half by a chunk boundary; the
        // value is only decoded once its braces balance, so it reassembles.
        let json = r#"[{"q":"café ☕"}]"#.as_bytes();
        let mut p = FrameParser::new();
        let mut out = Vec::new();
        for byte in json {
            out.extend(p.push(&[*byte]).unwrap());
        }
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["q"], "café ☕");
    }

    #[test]
    fn incomplete_trailing_value_yields_nothing_and_does_not_panic() {
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"a":1},{"b":"#]);
        assert_eq!(out.len(), 1);
        assert!(p.is_mid_value(), "the partial value is still pending");
    }

    #[test]
    fn oversized_frame_is_rejected_rather_than_buffered() {
        // A peer that never closes a value must not be able to grow this
        // process without bound.
        let mut p = FrameParser::new();
        let mut err = None;
        for _ in 0..200 {
            let chunk = vec![b'{'; 64 * 1024];
            if let Err(e) = p.push(&chunk) {
                err = Some(e);
                break;
            }
        }
        let err = err.expect("oversized frame should have been rejected");
        assert!(err.to_string().contains("exceeded"));
    }

    #[test]
    fn malformed_json_is_skipped_without_failing_the_stream() {
        // One unparseable message must not discard the rest of the answer.
        let mut p = FrameParser::new();
        let out = collect(&mut p, &[r#"[{"a":},{"b":2}]"#]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["b"], 2);
    }

    #[test]
    fn outcome_captures_sql_and_answer_text() {
        let mut o = ChatOutcome::default();
        o.absorb(&serde_json::json!({
            "systemMessage": { "data": { "generatedSql": "SELECT * FROM mart_revenue" } }
        }));
        o.absorb(&serde_json::json!({
            "systemMessage": { "text": { "parts": ["Revenue was", "1.2M"] } }
        }));
        assert_eq!(o.sql_text(), "SELECT * FROM mart_revenue");
        assert_eq!(o.answer, "Revenue was 1.2M");
        assert!(o.error.is_none());
    }

    #[test]
    fn outcome_records_a_reported_error() {
        let mut o = ChatOutcome::default();
        o.absorb(&serde_json::json!({ "error": { "message": "permission denied" } }));
        assert_eq!(o.error.as_deref(), Some("permission denied"));
    }

    #[test]
    fn table_reference_matches_on_the_unqualified_name() {
        // Users write the model name; the agent emits a fully-qualified one.
        let mut o = ChatOutcome::default();
        o.absorb(&serde_json::json!({
            "systemMessage": { "data": { "generatedSql": "SELECT 1 FROM `p.d.mart_revenue`" } }
        }));
        assert!(o.references_table("mart_revenue"));
        assert!(o.references_table("p.d.mart_revenue"));
        assert!(!o.references_table("stg_orders"));
        assert!(!o.references_table(""));
    }

    #[test]
    fn chat_body_addresses_the_agent_and_location() {
        let body = build_chat_body(
            "p",
            "global",
            "projects/p/locations/global/dataAgents/a",
            "hi",
        );
        assert_eq!(body["parent"], "projects/p/locations/global");
        assert_eq!(body["messages"][0]["userMessage"]["text"], "hi");
        assert_eq!(
            body["conversationReference"]["dataAgentContext"]["dataAgent"],
            "projects/p/locations/global/dataAgents/a"
        );
    }

    use proptest::prelude::*;

    proptest! {
        /// Arbitrary bytes must never panic the parser: the stream is remote
        /// input, and a panic here would take down the plugin mid-deploy.
        #[test]
        fn parser_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let mut p = FrameParser::new();
            let _ = p.push(&bytes);
        }

        /// Chunking must not change the result: the same bytes split anywhere
        /// yield the same values.
        #[test]
        fn chunking_is_irrelevant_to_the_outcome(split in 1usize..40) {
            let json = r#"[{"a":1,"s":"x{y}z"},{"b":[1,2,3]},{"c":{"d":"e"}}]"#;
            let mut whole = FrameParser::new();
            let expected = whole.push(json.as_bytes()).unwrap();

            let mut chunked = FrameParser::new();
            let mut got = Vec::new();
            for piece in json.as_bytes().chunks(split) {
                got.extend(chunked.push(piece).unwrap());
            }
            prop_assert_eq!(expected, got);
        }
    }
}
