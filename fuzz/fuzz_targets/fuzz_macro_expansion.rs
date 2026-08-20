// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Macro expansion, which is the one phase that can feed itself.
//!
//! A macro body may contain another macro call, so expansion re-scans until it
//! converges. A macro that expands to something containing itself does not
//! converge, and the interesting question is whether that is caught or whether
//! it runs until the process dies.
#![no_main]

use std::collections::BTreeMap;

use gcpx_dbt::macros::{expand_macros, MacroDef};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // Split the input into a macro body and the SQL using it, so the fuzzer can
    // drive both sides rather than only the call site.
    let (body, sql) = text.split_at(text.len() / 2);

    let mut macros = BTreeMap::new();
    macros.insert(
        "m".to_owned(),
        MacroDef {
            args: vec!["x".to_owned()],
            sql: body.to_owned(),
        },
    );
    // A deliberately self-referential definition: expansion must terminate,
    // with an error rather than by exhausting memory.
    macros.insert(
        "recurse".to_owned(),
        MacroDef {
            args: vec![],
            sql: "{{ recurse() }}".to_owned(),
        },
    );

    let _ = expand_macros(sql, &macros);
});
