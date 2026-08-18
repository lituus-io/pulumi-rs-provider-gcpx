#!/usr/bin/env bash
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Build the plugin binary and stage it inside the Python package, so the wheel
# carries it. Set CARGO_BUILD_TARGET to cross-compile.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$SCRIPT_DIR/python/pulumi_rs_provider_gcpx/bin"

mkdir -p "$BIN_DIR"

TARGET_ARGS=()
RELEASE_DIR="$ROOT/target/release"
if [[ -n "${CARGO_BUILD_TARGET:-}" ]]; then
    TARGET_ARGS=(--target "$CARGO_BUILD_TARGET")
    RELEASE_DIR="$ROOT/target/$CARGO_BUILD_TARGET/release"
fi

echo "Building pulumi-resource-gcpx..."
cargo build --release --locked --manifest-path "$ROOT/Cargo.toml" \
    -p pulumi-resource-gcpx "${TARGET_ARGS[@]}"

SUFFIX=""
case "${CARGO_BUILD_TARGET:-$(uname -s)}" in
    *windows*|MINGW*|MSYS*) SUFFIX=".exe" ;;
esac

cp "$RELEASE_DIR/pulumi-resource-gcpx$SUFFIX" "$BIN_DIR/"
chmod +x "$BIN_DIR/pulumi-resource-gcpx$SUFFIX" 2>/dev/null || true

echo "Staged:"
ls -lh "$BIN_DIR"/pulumi-resource-gcpx*
