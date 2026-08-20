// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The agent chat stream parser.
//!
//! This reads a JSON array that arrives in arbitrary chunks off the network, so
//! it must produce the same messages regardless of where the chunk boundaries
//! fall. That invariant is the reason this is fuzzed rather than unit tested:
//! the failure only appears at boundaries nobody thought to write down — and
//! one already did, when `[` was treated as the start of a value and the whole
//! array arrived as a single frame.
#![no_main]

use gcpx_agents::chat::FrameParser;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // All at once.
    let mut whole = FrameParser::new();
    let a = whole.push(data);

    // The same bytes, one at a time. Chunking is the network's choice, not the
    // message's, so it must not change what is parsed.
    let mut split = FrameParser::new();
    let mut b = Vec::new();
    let mut split_failed = false;
    for byte in data {
        match split.push(std::slice::from_ref(byte)) {
            Ok(mut msgs) => b.append(&mut msgs),
            Err(_) => {
                split_failed = true;
                break;
            }
        }
    }

    if let (Ok(a), false) = (a, split_failed) {
        assert_eq!(
            a, b,
            "the messages parsed depend on how the stream happened to be chunked"
        );
    }
});
