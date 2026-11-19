# The razor-rpc design

## Design goal

A modular, pluggable RPC that supports various async runtimes.

* interface types:
  - stream: Interface for stream processing.
  - api: Interface for remote function call.
* transport types:
  - TCP transport: optimized for high throughput internal network.
  - RDMA transport: optimized for low latency internal network.
  - QUIC transport: optimized for high throughput public network.
* codec:
  - Msgpack
  - bincode
* runtime: provides AsyncIO trait for runtime adapter
  - tokio
  - smol (async_io)

## Stream interface

### Stream protocol

src/stream/proto.rs: Defines general proto

`RpcAction`: A numeric action is for fewer type variants, while a string action is for the implementation of the API interface.

Request format: Each request package has a fixed-length header (ReqHead), followed by a structured encoded "msg", and then optionally followed by a "blob" (which is allocated as an owned buffer).

Response format: Each response package has a fixed-length header (RespHead), followed by a structured encoded "msg", and then optionally followed by a "blob" (which is allocated as an owned buffer)

### Stream Client


### Stream Server

src/stream/server.rs:

`ServerFact` is the interface for a user to assign or customize with various plugins (transport, codec, etc.).

src/stream/server_impl.rs:

`RpcServer` is the server implementation, which listens for and accepts connections through the transport. The new connection is processed by the ServerHandle::run interface. We typically use the `ServerHandleTaskStream` implementation, which utilizes a coroutine to read and a coroutine to write, so that the throughput can use at most 2 CPU cores with one connection.

We intend to support service-level graceful restart and graceful shutdown.

A packet received by the server should have gone through the following steps:

    decode -> request task -> dispatch -> processed -> response task -> encode -> write

#### decode

An enum task should implement ServerTaskDecode::decode_req(action ... ), and recognize the sub-task type by RpcAction.

For numeric actions, which are more used in the stream interface, it is suitable to use a generated match to call the sub-type's decode_req.

The macro can be:

```

#[server_task_enum]
pub enum MyServerTask {
    #[action=xxx]
    Type1(SubType1)
    #[action=xxx]
    Type2(SubType2)
}
```

(action may be a number or a string)

For SubType1, we need to generate MyServerTask::From<SubType1>. If a user processes using SubType and calls subtype::set_result(), it should be transformed via into() into the supertype to call RespNoti.done().

If a user uses the ServerTaskVariant/ServerTaskVariantFull generic as a subtype, which has already implemented ServerTaskEncode/Encode, they cannot implement their own methods due to the "Orphan Rule" limitation of Rust. They should implement wrapper types with the new-type pattern.

So the macro is on MyServerTask:

* Need to assign an action for the variants for ServerTaskDecode::decode_req, which provides get_action(&self) for the user

* Need to implement From for variants

For a string, which might be used in the API interface, it can be first matched in a HashMap to decide which class to handle, and decoded according to the string (class::method) to a specified Req type.


#### Dispatch

The dispatcher is responsible for decoding the task and dispatching it to the backend for processing. (There may be a struct like a worker pool in other threads/coroutines to handle this).

The codec is intended to support encryption, so there should be a shared state, which needs to be initialized as an Arc<Codec> and shared between the connection reader and writer. Therefore, the dispatcher should be an Arc shared between the reader and writer.

We set RespTask in ServerFact as an associated type, to defined the task type for the RespNoti channel but we leave ReqDispatch to be inferred from
` fn new_dispatcher(&self) -> impl ReqDispatch<Self::RespReceiver>``, because, like ReqDispatchClosure, it usually comes with a closure capturing the context about how to dispatch the task. This may be a Fn or a struct defined by the user.
