#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! # razor-rpc
//!
//! This crate provides a high-level remote API call interface for `razor-rpc`.
//! It is part of a modular, pluggable RPC for high throughput scenarios that supports various async runtimes.
//!
//! If you are looking for streaming interface, use [razor-stream](https://docs.rs/razor-stream) instead.
//!
//! ## Feature
//!
//! - Independent from async runtime (with plugins)
//! - With service trait very similar to grpc / tarpc (stream in API interface is not supported
//! currently)
//! - Support latest `impl Future` definition of rust since 1.75, also support legacy `async_trait`
//! wrapper
//! - Each method can have different custom error type (requires the type implements
//! [RpcErrCodec](crate::error::RpcErrCodec))
//! - based on [razor-stream](https://docs.rs/razor-stream): Full duplex in each connection, with sliding window threshold, allow maximizing throughput and lower cpu usage.
//!
//! (Warning: The API and feature is still evolving, might changed in the future)
//!
//! ## Components
//!
//! `razor-rpc` is built from a collection of crates that provide different functionalities:
//!
//! - Async runtime support by [`orb`](https://docs.rs/orb):
//!   - [`orb-tokio`](https://docs.rs/orb-tokio): A runtime adapter for the `tokio` runtime.
//!   - [`orb-smol`](https://docs.rs/orb-smol): A runtime adapter for the `smol` runtime.
//! - codec [`razor-rpc-codec`](https://docs.rs/razor-rpc-codec): Provides codecs for serialization, such as `msgpack`.
//! - transports:
//!   - [`razor-rpc-tcp`](https://docs.rs/razor-rpc-tcp): A TCP transport implementation.
//!
//! ## Usage
//!
//! 1. Choose your async runtime, and the codec.
//! 2. Choose underlying transport, like [`razor-rpc-tcp`](https://docs.rs/razor-rpc-tcp)
//! 3. define your service trait, the client is also generated along with the trait.
//!    Also see the [error module](crate::error) for details on built-in error types and custom error type examples.
//! 4. impl your service trait at server-side
//! 5. Initialize ServerFacts (with configuration and runtime)
//! 6. choose request dispatch method: [crate::server::dispatch]
//! 7. Start listening for connection
//! 8. Initialize ClientFacts (with configuration, runtime, and codec)
//! 9. Setup a connection pool: [ClientPool](crate::client::ClientPool) or
//!    [FailoverPool](crate::client::FailoverPool)
//! ## Example
//!
//! ```rust
//! use razor_rpc::client::{endpoint_async, APIClientReq, ClientConfig};
//! use razor_rpc::server::{service, ServerConfig};
//! use razor_rpc::error::RpcError;
//! use razor_rpc_tcp::{TcpClient, TcpServer};
//! use nix::errno::Errno;
//! use std::future::Future;
//! use std::sync::Arc;
//!
//! // 1. Choose the async runtime, and the codec
//! type OurRt = orb_tokio::TokioRT;
//! type OurCodec = razor_rpc_codec::MsgpCodec;
//! // 2. Choose transport
//! type ServerProto = TcpServer<OurRt>;
//! type ClientProto = TcpClient<OurRt>;
//!
//! // 3. Define a service trait, and generate the client struct
//! #[endpoint_async(CalculatorClient)]
//! pub trait CalculatorService {
//!     // Method with unit error type using impl Future
//!     fn add(&self, args: (i32, i32)) -> impl Future<Output = Result<i32, RpcError<()>>> + Send;
//!
//!     // Method with string error type using impl Future
//!     fn div(&self, args: (i32, i32)) -> impl Future<Output = Result<i32, RpcError<String>>> + Send;
//!
//!     // Method with errno error type using impl Future
//!     fn might_fail_with_errno(&self, value: i32) -> impl Future<Output = Result<i32, RpcError<Errno>>> + Send;
//! }
//!
//! // 4. Server implementation, can use Arc with internal context, but we are a simple demo
//! #[derive(Clone)]
//! pub struct CalculatorServer;
//!
//! #[service]
//! impl CalculatorService for CalculatorServer {
//!     async fn add(&self, args: (i32, i32)) -> Result<i32, RpcError<()>> {
//!         let (a, b) = args;
//!         Ok(a + b)
//!     }
//!
//!     async fn div(&self, args: (i32, i32)) -> Result<i32, RpcError<String>> {
//!         let (a, b) = args;
//!         if b == 0 {
//!             Err(RpcError::User("division by zero".to_string()))
//!         } else {
//!             Ok(a / b)
//!         }
//!     }
//!
//!     async fn might_fail_with_errno(&self, value: i32) -> Result<i32, RpcError<Errno>> {
//!         if value < 0 {
//!             Err(RpcError::User(Errno::EINVAL))
//!         } else {
//!             Ok(value * 2)
//!         }
//!     }
//! }
//!
//! async fn setup_server() -> std::io::Result<String> {
//!     // 5. Server setup with default ServerFacts
//!     use razor_rpc::server::{RpcServer, ServerDefault};
//!     let server_config = ServerConfig::default();
//!     let mut server = RpcServer::new(ServerDefault::new(server_config, OurRt::new_multi_thread(8)));
//!     // 6. dispatch
//!     use razor_rpc::server::dispatch::Inline;
//!     let disp = Inline::<OurCodec, _>::new(CalculatorServer);
//!     // 7. Start listening
//!     let actual_addr = server.listen::<ServerProto, _>("127.0.0.1:8082", disp).await?;
//!     Ok(actual_addr)
//! }
//!
//! async fn use_client(server_addr: &str) {
//!     use razor_rpc::client::*;
//!     // 8. ClientFacts
//!     let mut client_config = ClientConfig::default();
//!     client_config.task_timeout = 5;
//!     let rt = OurRt::new_multi_thread(8);
//!     type OurFacts = APIClientDefault<OurRt, OurCodec>;
//!     let client_facts = OurFacts::new(client_config, rt);
//!     // 9. Create client connection pool
//!     let pool: ClientPool<OurFacts, ClientProto> =
//!         client_facts.create_pool_async::<ClientProto>(server_addr);
//!     let client = CalculatorClient::new(pool);
//!     // Call methods with different error types
//!     if let Ok(r) = client.add((10, 20)).await {
//!         assert_eq!(r, 30);
//!     }
//!     // This will return a string error, but connect might fail, who knows
//!     if let Err(e) = client.div((10, 0)).await {
//!         println!("error occurred: {}", e);
//!     }
//! }
//! ```

pub mod client;
pub mod server;

// re-export for macros, so that user don't need to use multiple crates
pub use razor_stream::{Codec, error};
