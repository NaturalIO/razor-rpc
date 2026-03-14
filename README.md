# razor-rpc

A modular, pluggable RPC for high throughput scenario, supports various runtimes,
with a low-level streaming interface, and high-level remote API call interface.

## Components

`razor-rpc` is built from a collection of crates that provide different functionalities:

- Async runtime support by [`orb`](https://docs.rs/orb):
  - [`orb-tokio`](https://docs.rs/orb-tokio): A runtime adapter for the `tokio` runtime.
  - [`orb-smol`](https://docs.rs/orb-smol): A runtime adapter for the `smol` runtime.
- codec  [`razor-rpc-codec`](https://docs.rs/razor-rpc-codec): Provides codecs for serialization, such as `msgpack`.
- transports:
  - [`razor-rpc-tcp`](https://docs.rs/razor-rpc-tcp): A TCP transport implementation.

## Streaming interface

`razor-stream` <https://docs.rs/razor-stream>

The interface is designed to optimize throughput and lower
CPU consumption for high-performance services.

Each connection is a full-duplex, multiplexed stream.
There's a `seq` ID assigned to a packet to track
a request and response. The timeout of a packet is checked in batches every second.
We utilize the [crossfire](https://docs.rs/crossfire) channel for parallelizing the work with
coroutines.

With an `ClientStream` (used in `ClientPool` and `FailoverPool`), the request sends in sequence, flush in batches,
 and wait a sliding window throttler controlling the number of in-flight packets.
An internal timer then registers the request through a channel, and when the response is received,
 it can optionally notify the user through a user-defined channel or another mechanism.

In an `RpcServer`, for each connection, there is one coroutine to read requests and one
coroutine to write responses. Requests can be dispatched with a user-defined
`Dispatch` trait implementation.

### Builtin connection pools:

  - [ClientPool](https://docs.rs/razor-stream/latest/razor_stream/client/struct.ClientPool.html): Auto scalable pool:
  - [FailoverPool](https://docs.rs/razor-stream/latest/razor_stream/client/struct.FailoverPool.html): Fault-tolerance pool

## API call interface

Detail document and example refer to the `razor-rpc` crate <https://docs.rs/razor-rpc>

- Independent from async runtime (with plugins)
- With service trait very similar to grpc / tarpc (stream in API interface is not supported
currently)
- Support latest `impl Future` definition of rust since 1.75, also support legacy `async_trait`
wrapper
- Each method can have different custom error type (requires the type implements [RpcErrCodec](https://docs.rs/razor-stream/latest/razor_stream/error/trait.RpcErrCodec.html))
- based on razor-stream`: Full duplex in each connection, with sliding window threshold, allow maximizing throughput and lower cpu usage.
