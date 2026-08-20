// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The plugin's contract with the engine, exercised against the real binary.
//!
//! Everything else in this repo tests handlers directly. That skips the part
//! the engine actually depends on: launching a process, reading a port from its
//! stdout, and speaking gRPC to it. Each of those has a failure mode that no
//! unit test can reach — a port printed after a slow startup step, a message
//! larger than tonic's default cap, a version string that drifts from the
//! package — and each one breaks every deploy rather than one resource.
//!
//! So this runs the real binary and talks to it — which means the binary has to
//! exist. `cargo test --workspace` does not build another package's binary, so
//! these are `#[ignore]`d and run explicitly by the conformance job, which
//! builds it first. Without that they fail on every platform for a missing
//! file, saying nothing about the protocol they exist to check.
//!
//!     cargo build -p pulumi-resource-gcpx
//!     cargo test --test conformance -- --ignored

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use pulumi_rs_yaml_proto::pulumirpc;

/// Launches the plugin and returns its address, keeping the child alive.
struct Plugin {
    child: std::process::Child,
    endpoint: String,
}

impl Drop for Plugin {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn binary() -> std::path::PathBuf {
    // Built by the harness into the same target directory as this test.
    let mut dir = std::env::current_exe().expect("test binary path");
    dir.pop(); // deps/
    dir.pop(); // debug/ or release/
    dir.join("pulumi-resource-gcpx")
}

fn start() -> Plugin {
    let bin = binary();
    assert!(
        bin.exists(),
        "the plugin binary is not built at {}; run `cargo build -p pulumi-resource-gcpx` first",
        bin.display()
    );

    let mut child = Command::new(&bin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // No credentials: the plugin must serve without them, because the engine
        // launches it for validation and preview too.
        .env_remove("GOOGLE_APPLICATION_CREDENTIALS")
        .spawn()
        .expect("plugin failed to start");

    let stdout = child.stdout.take().expect("piped stdout");
    let mut line = String::new();
    BufReader::new(stdout)
        .read_line(&mut line)
        .expect("plugin printed no port");

    let port: u16 = line
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("first stdout line must be a bare port, got {line:?}"));
    assert!(port > 0, "port must be a real bound port, not zero");

    Plugin {
        child,
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

async fn connect(
    p: &Plugin,
) -> pulumirpc::resource_provider_client::ResourceProviderClient<tonic::transport::Channel> {
    // The engine raises this cap on its side; the client here has to as well or
    // it cannot receive a schema the server is willing to send.
    let channel = tonic::transport::Endpoint::from_shared(p.endpoint.clone())
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("plugin did not accept a connection on the port it printed");
    pulumirpc::resource_provider_client::ResourceProviderClient::new(channel)
        .max_decoding_message_size(gcpx_core::MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(gcpx_core::MAX_GRPC_MESSAGE_BYTES)
}

/// The handshake: a port on stdout, before anything slow happens.
///
/// The engine blocks on this line. Resolving credentials or opening a socket
/// first would put a network round-trip in front of every single invocation,
/// and would make the plugin unusable offline — which is how validation runs.
#[tokio::test]
#[ignore = "needs the plugin binary built; run via the conformance job"]
async fn the_plugin_prints_a_port_and_serves_without_credentials() {
    let plugin = start();
    let mut client = connect(&plugin).await;
    let info = client
        .get_plugin_info(())
        .await
        .expect("GetPluginInfo failed on a plugin with no credentials");
    assert_eq!(
        info.into_inner().version,
        env!("CARGO_PKG_VERSION"),
        "the reported version must match the package, or the engine caches the wrong plugin"
    );
}

/// The schema is larger than tonic's 4 MiB default, and the failure when it is
/// not raised is opaque: "decoded message length too large", surfacing during
/// validation rather than deploy.
#[tokio::test]
#[ignore = "needs the plugin binary built; run via the conformance job"]
async fn the_schema_survives_the_grpc_round_trip() {
    let plugin = start();
    let mut client = connect(&plugin).await;
    let resp = client
        .get_schema(pulumirpc::GetSchemaRequest {
            version: 0,
            ..Default::default()
        })
        .await
        .expect("GetSchema failed — check the message size limits on both sides")
        .into_inner();

    let schema: serde_json::Value =
        serde_json::from_str(&resp.schema).expect("the schema must be valid JSON");
    assert_eq!(
        schema["name"], "gcpx",
        "the package name is the public contract"
    );
    assert_eq!(schema["version"], env!("CARGO_PKG_VERSION"));
}

/// Every resource the schema advertises must actually be dispatchable.
///
/// A token present in the schema but missing from dispatch is a resource a user
/// can write into a stack and never deploy; the reverse is a resource that
/// exists but that no stack can reference. Both are silent until someone tries.
#[tokio::test]
#[ignore = "needs the plugin binary built; run via the conformance job"]
async fn every_advertised_resource_token_is_dispatchable() {
    let plugin = start();
    let mut client = connect(&plugin).await;
    let resp = client
        .get_schema(pulumirpc::GetSchemaRequest {
            version: 0,
            ..Default::default()
        })
        .await
        .expect("GetSchema")
        .into_inner();
    let schema: serde_json::Value = serde_json::from_str(&resp.schema).expect("valid JSON");

    let tokens: Vec<String> = schema["resources"]
        .as_object()
        .expect("the schema declares resources")
        .keys()
        .cloned()
        .collect();
    assert!(
        !tokens.is_empty(),
        "the schema advertises no resources at all"
    );

    for token in &tokens {
        // Check with empty properties: the answer may well be "these inputs are
        // invalid", which is fine — what must not happen is "unknown resource",
        // which means the token is advertised but not wired up.
        let resp = client
            .check(pulumirpc::CheckRequest {
                urn: format!("urn:pulumi:dev::t::{token}::probe"),
                news: Some(prost_types::Struct::default()),
                ..Default::default()
            })
            .await;
        match resp {
            Ok(_) => {}
            Err(status) => {
                let msg = status.message().to_ascii_lowercase();
                assert!(
                    !msg.contains("unknown resource") && !msg.contains("unsupported"),
                    "{token} is advertised in the schema but not dispatchable: {msg}"
                );
            }
        }
    }
}
