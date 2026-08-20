// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The scanner, which every other phase is built on.
//!
//! It yields borrowed slices of its input, so beyond not panicking it has a
//! property worth asserting: the segments it produces must account for the
//! whole input and nothing but the input. A scanner that drops a span silently
//! drops SQL.
#![no_main]

use gcpx_dbt::scanner::{DbtScanner, DbtSegment};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };

    let mut plain = String::new();
    for segment in DbtScanner::new(sql) {
        match segment {
            DbtSegment::Sql(s) => {
                // Every literal span must be a slice of the input, not a
                // rewritten copy.
                assert!(
                    sql.contains(s),
                    "the scanner produced SQL text that is not in its input"
                );
                plain.push_str(s);
            }
            DbtSegment::Ref { model } => assert!(sql.contains(model)),
            DbtSegment::Source { source, table } => {
                assert!(sql.contains(source) && sql.contains(table));
            }
            DbtSegment::Config { raw_args } => assert!(sql.contains(raw_args)),
            DbtSegment::Call { name, raw_args } => {
                assert!(sql.contains(name) && sql.contains(raw_args));
            }
        }
    }

    // Literal text can never exceed the input it was sliced from.
    assert!(plain.len() <= sql.len());
});
