// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The whole template pipeline on arbitrary SQL.
//!
//! This code rewrites text that becomes a BigQuery statement, and it runs on
//! whatever a user puts in a stack file. A panic here aborts the plugin
//! mid-deploy; a hang stalls it. Both are reachable from ordinary input,
//! because "ordinary input" includes half-written SQL and unbalanced tags.
#![no_main]

use std::collections::BTreeMap;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let mut vars = BTreeMap::new();
    vars.insert("start_date".to_owned(), "'2026-01-01'".to_owned());
    vars.insert("days".to_owned(), "90".to_owned());

    // Both passes: the incremental branch is a different path through the tag
    // matcher, and it is the one that rewrites structure rather than values.
    let _ = gcpx_dbt::preprocess::preprocess(sql, &vars, "`p.d.t`", false);
    let _ = gcpx_dbt::preprocess::preprocess(sql, &vars, "`p.d.t`", true);

    // Raw regions are set aside and restored around everything else, so their
    // round trip has to hold for any input — a region that comes back altered
    // is silently changed SQL.
    if let Ok(protected) = gcpx_dbt::preprocess::protect_raw_regions(sql) {
        let restored = gcpx_dbt::preprocess::restore_raw_regions(&protected);
        assert_eq!(
            restored, sql,
            "protecting and restoring raw regions altered the SQL"
        );
    }
});
