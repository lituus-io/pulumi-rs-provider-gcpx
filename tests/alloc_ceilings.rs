// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! Allocation ceilings for the template pipeline.
//!
//! Wall-clock benchmarks on a shared CI runner are noisy enough that a real
//! regression hides inside the variance, and the usual response — widening the
//! threshold until it stops flaking — leaves nothing being measured. Allocation
//! counts are deterministic: the same input allocates the same number of times
//! on every machine and every run, so a change in memory behaviour can be gated
//! exactly rather than probabilistically.
//!
//! The numbers below are ceilings, not targets. They exist to catch the kind of
//! change that turns one pass over the SQL into three, or starts cloning a
//! string per reference. Improving on them is welcome; the ceiling should be
//! lowered when it happens, which is the point of asserting an upper bound
//! rather than an equality.
//!
//! Behind a feature because a counting global allocator would otherwise be
//! installed for every test binary in the workspace.
#![cfg(feature = "alloc-ceilings")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// Safety: every method delegates to the system allocator unchanged; the counters
// are the only addition and they do not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(new_size.saturating_sub(layout.size()), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Runs `f` and reports (allocations, bytes) attributable to it.
fn measure<T>(f: impl FnOnce() -> T) -> (usize, usize) {
    let a0 = ALLOCATIONS.load(Ordering::Relaxed);
    let b0 = BYTES.load(Ordering::Relaxed);
    let out = f();
    let a = ALLOCATIONS.load(Ordering::Relaxed) - a0;
    let b = BYTES.load(Ordering::Relaxed) - b0;
    drop(out);
    (a, b)
}

fn assert_ceiling(what: &str, got: usize, ceiling: usize) {
    assert!(
        got <= ceiling,
        "{what}: {got} allocations exceeds the ceiling of {ceiling}. \
         If this is a deliberate trade, raise the ceiling in the same commit \
         and say what bought it."
    );
    // A ceiling nothing approaches has stopped measuring anything. This is not
    // a failure, only a note in the output.
    if ceiling > 4 && got * 4 < ceiling {
        println!("note: {what} used {got} of {ceiling} — the ceiling could come down");
    }
}

fn model_sql(refs: usize) -> String {
    let mut sql = String::from("{{ config(materialized='table') }}\nSELECT a.id\n");
    sql.push_str("FROM {{ source('raw', 'events') }} a\n");
    for i in 0..refs {
        sql.push_str(&format!(
            "JOIN {{{{ ref('m{i}') }}}} x{i} ON x{i}.id = a.id\n"
        ));
    }
    sql
}

/// One test, deliberately.
///
/// The counter is a process-wide global, and the test harness runs tests in
/// parallel by default — so a second measuring test running concurrently is
/// counted by the first. Splitting these into separate `#[test]` functions
/// produced numbers four times too high and an entirely fictional conclusion
/// about the scanner allocating per segment. Keeping every measurement in a
/// single sequential test removes the race rather than documenting it.
#[test]
fn the_template_pipeline_stays_within_its_allocation_ceilings() {
    use gcpx_dbt::preprocess::preprocess;
    use gcpx_dbt::scanner::DbtScanner;

    // Warm anything lazily initialised so it is not attributed to the first
    // measurement.
    let warm = model_sql(4);
    let _ = DbtScanner::new(&warm).count();
    let _ = preprocess(&warm, &BTreeMap::new(), "`p.d.t`", false).unwrap();

    // Scanning yields borrowed slices of the input — every field of a segment
    // is a `&str` — so iterating must not allocate at all, however many
    // references the model holds.
    let sql = model_sql(64);
    let (scan_allocs, _) = measure(|| DbtScanner::new(&sql).count());
    assert_ceiling("scanning a 64-reference model", scan_allocs, 1);

    // Preprocessing rewrites the text, so it allocates. What matters is that
    // the count tracks the number of passes over the SQL and not the number of
    // references: a per-reference allocation is how this turns quadratic.
    let vars = BTreeMap::new();
    // Built outside the measurement: constructing the input allocates once per
    // reference, and counting that as the pipeline's cost hides what is being
    // measured behind the fixture.
    let small_sql = model_sql(8);
    let large_sql = model_sql(256);
    let (small, _) = measure(|| preprocess(&small_sql, &vars, "`p.d.t`", false).unwrap());
    let (large, _) = measure(|| preprocess(&large_sql, &vars, "`p.d.t`", false).unwrap());
    println!("preprocess: {small} allocations at 8 refs, {large} at 256");
    assert_eq!(
        small, large,
        "preprocessing allocated {small} times for 8 references and {large} for 256 — \
         it is meant to be one allocation per pass over the text, independent of \
         how many references the model holds"
    );
    assert!(
        large <= small * 4,
        "preprocess allocated {small} for 8 references and {large} for 256 — \
         32x the input for {}x the allocations means a per-reference copy",
        large / small.max(1)
    );
    // Measured at 7 — one per pass over the text, and identical at 8
    // references and at 256. The ceiling leaves room for one more pass being
    // added deliberately, and nothing else.
    assert_ceiling("preprocessing a 256-reference model", large, 12);
}
