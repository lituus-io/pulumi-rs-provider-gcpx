// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>

//! The gcpx Pulumi plugin.
//!
//! The engine launches this binary, reads a port number from its stdout, and
//! then speaks gRPC to it. Two details of that handshake are load-bearing and
//! easy to get wrong, so they are spelled out below.

// The provider is short-lived and allocation-heavy — parsing property maps,
// building JSON — which is exactly the profile mimalloc suits.
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::net::SocketAddr;

use gcpx_core::auth::AdcCredentials;
use gcpx_core::http::HttpGcpClient;
use gcpx_core::MAX_GRPC_MESSAGE_BYTES;
use pulumi_rs_yaml_proto::pulumirpc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The engine blocks reading this port from stdout, so it is printed and
    // flushed before anything else happens. Any work done first — especially
    // anything touching the network — is time the engine spends waiting.
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    {
        use std::io::Write;
        let port = listener.local_addr()?.port();
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{port}")?;
        stdout.flush()?;
    }

    // Credentials resolve on first use, not here. The plugin is launched for
    // validation and preview as well as deploy, and those paths may never call
    // a Google API at all — so authenticating up front would make offline
    // validation impossible and put a round-trip in front of every invocation.
    let client = HttpGcpClient::new(
        HttpGcpClient::<AdcCredentials>::default_http_client()?,
        AdcCredentials::new(),
    );
    let provider = gcpx_provider::GcpxProvider::new(client);

    // tonic defaults to a 4 MiB cap in both directions. Provider schemas and
    // large resource registrations exceed it, and the failure is opaque
    // ("decoded message length too large") and lands during validation, not
    // just deploy. The engine and the language runtime already raise this
    // limit; the server side has to agree or the raise achieves nothing.
    let service = pulumirpc::resource_provider_server::ResourceProviderServer::new(provider)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);

    Server::builder()
        .add_service(service)
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;

    Ok(())
}
