// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Identifier quoting, the boundary between a user's string and generated SQL.
//!
//! Every DDL statement this provider emits interpolates names that came from a
//! stack file. If a name can carry a backtick out of its quoting it can carry a
//! statement with it, so the property is not "looks escaped" but "the result is
//! exactly one identifier, whatever went in".
#![no_main]

use gcpx_core::sanitize::{bq_col_ref, bq_table_ref, escape_bq_ident};
use libfuzzer_sys::fuzz_target;

/// Counts backticks that are not themselves escaped.
fn unescaped_backticks(s: &str) -> usize {
    let b = s.as_bytes();
    let mut count = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2, // whatever follows is escaped
            b'`' => {
                count += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    count
}

fuzz_target!(|data: &[u8]| {
    let Ok(name) = std::str::from_utf8(data) else {
        return;
    };

    // Escaping alone must leave nothing that can close a quote.
    let escaped = escape_bq_ident(name);
    assert_eq!(
        unescaped_backticks(&escaped),
        0,
        "an unescaped backtick survived escaping: {escaped:?}"
    );

    // A column reference is one identifier: the only unescaped backticks are
    // the pair that delimits it.
    let col = bq_col_ref(name);
    assert!(col.starts_with('`') && col.ends_with('`'));
    assert_eq!(
        unescaped_backticks(&col),
        2,
        "a column reference did not stay a single identifier: {col:?}"
    );

    // And a table reference is three, one pair around the whole thing.
    let table = bq_table_ref(name, name, name);
    assert_eq!(
        unescaped_backticks(&table),
        2,
        "a table reference did not stay a single reference: {table:?}"
    );
});
