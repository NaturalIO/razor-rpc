## The API interface

### 1. Service

A Service in `razor-rpc` follows these principles:
- Called with immutable `&self` (server-side requires `Sync`)
- Client and server share the same trait definition for compile-time checks
- Service methods return `Result<T, RpcError<E>>` where `E: RpcErrCodec`
- Methods should be `async fn` or return `impl Future`
- Compatible with GRPC naming conventions (`service` in PascalCase, `method` in snake_case)

We supports rust 1.75 `AFIT` (Async fn in Traits) `RPITIT` (Return Position Impl Trait in Traits), and legacy `#[async_trait]`.

The best practice is to define service interface in a separate "proto" crate shared between server and client.

While you still have an optional not to shared code (when you have a public client and intend to keep some private methods), then you can optionally use `#[service]` on `impl` block and `#[method]` to mark service methods.
(Refer to example in [`server::service`](crate::server::service)).
 
### 2. Client-Side

See [`client`](crate::client) module for more details.

Key components for the client:

**ClientCallers**

The following structs impl both [ClientCaller](crate::client::ClientCaller) (async) and [ClientCallerBlocking](crate::client::ClientCallerBlocking).

- **[`ClientPool`](crate::client::ClientPool)**: Maintains a pool of worker connections
- **[`FailoverPool`](crate::client::FailoverPool)**: Load balancing and failover, maintains multiple `ClientPool`

**Endpoint**

Endpoints are wrapper structs around `ClientCaller`

- **[`AsyncEndpoint`](crate::client::AsyncEndpoint)**: This trait defines `async fn call` - a wrapper around `ClientCaller`
- **[`BlockingEndpoint`](crate::client::BlockingEndpoint)**: This trait defines synchronous `fn call`

**Client**

For async context, we provide macro: **[`#[endpoint_async]`](crate::client::endpoint_async)** - Applied to a user defined trait to generate a client struct by specified name. 

For example: 
```ignore
#[endpoint_async(CalculatorClient)]
pub trait CalculatorService {
    ...
}
```

The generated client implements the trait. and a new function to wrap a generic ClientCaller.

blocking-context is not implemented yet.


### 3. Server-Side

When apply [`#[service]`](crate::server::service) on a user defined trait, it will parse all async fn method and impl [ServiceStatic](crate::server::ServiceStatic) trait on it. 

Its `serve(req)` method will:
  - decode the request argument type from [APIServerReq](crate::server::task::APIServerReq)
  - call the method in itself, to get a response
  - set_result or set_error, and encode a [APIServerResp](crate::server::task::APIServerResp) contains message bytes or an error
  - send the Response through RPC channel.

**Static dispatch**

When you listen on a specified port, and bind it with only one Service trait, when it is static dispatch.

**Dynamic dispatch**

There's slight cost to call method on trait object, but this is very trivial compare to network transmission.

`Arc<dyn ServiceDyn>` have auto impl `ServiceStatic`.

- **[`ServiceMuxDyn`](crate::server::ServiceMuxDyn)**: Dynamic service multiplexer using `HashMap<&'static str, Arc<dyn ServiceDyn>>`
- macro **[`service_mux_struct`](crate::server::service_mux_struct)** :
  Applied to a struct to implement the `ServiceStatic` trait, acting as a service dispatcher. Each field should hold a service that implements `ServiceStatic` (typically wrapped in `Arc`). The macro routes requests based on the `req.service` field matching the struct field names.

See [`server`](crate::server) module for more details.

## Example Usage

Steps:

1. Choose your async runtime, and the codec.
2. Choose underlying transport, like [`razor-rpc-tcp`](https://docs.rs/razor-rpc-tcp)
3. define your service trait, the client is also generated along with the trait.
   Also see the [error module](crate::error) for details on built-in error types and custom error type examples.
4. impl your service trait at server-side
5. Initialize ServerFacts (with configuration and runtime)
6. choose request dispatch method: [crate::server::dispatch]
7. Start listening for connection
8. Initialize ClientFacts (with configuration, runtime, and codec)
9. Setup a connection pool: [ClientPool](crate::client::ClientPool) or
   [FailoverPool](crate::client::FailoverPool)

The code:

```rust
use razor_rpc::client::{endpoint_async, APIClientReq, ClientConfig};
use razor_rpc::server::{service, ServerConfig};
use razor_rpc::error::RpcError;
use razor_rpc_tcp::{TcpClient, TcpServer};
use nix::errno::Errno;
use std::future::Future;
use std::sync::Arc;

// 1. Choose the async runtime, and the codec
type OurRt = orb_tokio::TokioRT;
type OurCodec = razor_rpc_codec::MsgpCodec;
// 2. Choose transport
type ServerProto = TcpServer<OurRt>;
type ClientProto = TcpClient<OurRt>;

// 3. Define a service trait, and generate the client struct
#[endpoint_async(CalculatorClient)]
pub trait CalculatorService {
    // Method with unit error type using impl Future
    fn add(&self, args: (i32, i32)) -> impl Future<Output = Result<i32, RpcError<()>>> + Send;

    // Method with string error type using impl Future
    fn div(&self, args: (i32, i32)) -> impl Future<Output = Result<i32, RpcError<String>>> + Send;

    // Method with errno error type using impl Future
    fn might_fail_with_errno(&self, value: i32) -> impl Future<Output = Result<i32, RpcError<Errno>>> + Send;
}

// 4. Server implementation, can use Arc with internal context, but we are a simple demo
#[derive(Clone)]
pub struct CalculatorServer;

#[service]
impl CalculatorService for CalculatorServer {
    async fn add(&self, args: (i32, i32)) -> Result<i32, RpcError<()>> {
        let (a, b) = args;
        Ok(a + b)
    }

    async fn div(&self, args: (i32, i32)) -> Result<i32, RpcError<String>> {
        let (a, b) = args;
        if b == 0 {
            Err(RpcError::User("division by zero".to_string()))
        } else {
            Ok(a / b)
        }
    }

    async fn might_fail_with_errno(&self, value: i32) -> Result<i32, RpcError<Errno>> {
        if value < 0 {
            Err(RpcError::User(Errno::EINVAL))
        } else {
            Ok(value * 2)
        }
    }
}

async fn setup_server() -> std::io::Result<String> {
    // 5. Server setup with default ServerFacts
    use razor_rpc::server::{RpcServer, ServerDefault};
    let server_config = ServerConfig::default();
    let mut server = RpcServer::new(ServerDefault::new(server_config, OurRt::new_multi_thread(8)));
    // 6. dispatch
    use razor_rpc::server::dispatch::Inline;
    let disp = Inline::<OurCodec, _>::new(CalculatorServer);
    // 7. Start listening
    let actual_addr = server.listen::<ServerProto, _>("127.0.0.1:8082", disp).await?;
    Ok(actual_addr)
}

async fn use_client(server_addr: &str) {
    use razor_rpc::client::*;
    // 8. ClientFacts
    let mut client_config = ClientConfig::default();
    client_config.task_timeout = 5;
    let rt = OurRt::new_multi_thread(8);
    type OurFacts = APIClientDefault<OurRt, OurCodec>;
    let client_facts = OurFacts::new(client_config, rt);
    // 9. Create client connection pool
    let pool: ClientPool<OurFacts, ClientProto> =
        client_facts.create_pool_async::<ClientProto>(server_addr);
    let client = CalculatorClient::new(pool);
    //  You will have to import CalculatorService trait to call its methods
    use CalculatorService;
    // Call methods with different error types
    if let Ok(r) = client.add((10, 20)).await {
        assert_eq!(r, 30);
    }
    // This will return a string error, but connect might fail, who knows
    if let Err(e) = client.div((10, 0)).await {
        println!("error occurred: {}", e);
    }
}
```
