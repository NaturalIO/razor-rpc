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
//! - With service trait very similar to grpc / tarpc (stream in API interface is not supported currently)
//! - Support latest `impl Future` definition of rust since 1.75, also support legacy `async_trait` wrapper
//! - Each method can have different custom error type (requires the type implements
//!   [RpcErrCodec](crate::error::RpcErrCodec))
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
#![doc = include_str!("../docs/api_design.md")]

pub mod client;
pub mod server;

// re-export for macros, so that user don't need to use multiple crates
pub use razor_stream::{Codec, error};
