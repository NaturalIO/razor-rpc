## The API interface

### 1. Client-Side

See [`client`](crate::client) module for more details.

Key components for the client:

**Connections Pool**:

The following are type alias from `razor-stream` crate:

- [`APIConnPool`](crate::client::APIConnPool): Maintains a pool of worker connections
- [`APIFailoverPool`](crate::client::APIFailoverPool): Load balancing and failover, maintains multiple `ConnPool`

We have added a helper trait [`APIClientCaller`](crate::client::APIClientCaller), which defines a helper function for a service.method call.
This trait is used in proc-macro `#[endpoint_async]` generated  code for a service trait.

**Client**

Because client should defined by user to add their service method, we provide macro
**[`endpoint_client!`](crate::client::endpoint_client)** to generates a wrapper struct with generic over the `APIClientCaller` connection pools, which have a new() method:

For example:

```text
endpoint_client!(YourClient);
```

Generated code:
```text
pub struct #client_name<C>
where
    C: razor_rpc::client::APIClientCaller,
{
    caller: C,
}

impl<C> #client_name<C>
where
    C: razor_rpc::client::APIClientCaller,
{
    pub fn new(caller: C) -> Self {
        ...
    }
}
```

NOTE: blocking-context is not implemented yet.

### 2. Service

A Service in `razor-rpc` follows these principles:
- Called with immutable `&self` (server-side requires `Sync`)
- Client and server share the same trait definition for compile-time checks
- Compatible with GRPC naming conventions (`service` in PascalCase, `method` in snake_case)
- Methods should be `async fn` or return `impl Future`
- Methods can return **custom error type**. All method should return `Result<T, RpcError<E>>` where `E: RpcErrCodec`, refer to doc: [error module](crate::error). 

We supports rust 1.75 `AFIT` (Async fn in Traits) `RPITIT` (Return Position Impl Trait in Traits), and legacy `#[async_trait]`.

The best practice is to define service interface in a separate "proto" crate shared between server and client.
You will need to apply [`#[endpoint_async]`](crate::client::endpoint_async) macro to impl the trait with your client.

**NOTE**: You can apply multiple service traits to a client.

```text
#[endpoint_async(YourClient)]
pub trait YourService {
    ...
}
```


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

## Example Usage (using ConnPool)

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
9. Setup a connection pool: [ConnPool](crate::client::ConnPool) or
   [FailoverPool](crate::client::FailoverPool)

The code:

```rust
use razor_rpc::client::{endpoint_client, endpoint_async, APIFact, APIConnPool, ClientConfig};
use razor_rpc::server::{service, ServerConfig};
use razor_rpc::error::RpcError;
use razor_rpc_tcp::{TcpClient, TcpServer};
use nix::errno::Errno;
use std::future::Future;
use std::sync::Arc;

// 1. Choose the async runtime, and the codec
type RT = orb_tokio::TokioRT;
type Codec = razor_rpc_codec::MsgpCodec;
// 2. Choose transport
type ServerProto = TcpServer<RT>;
type ClientProto = TcpClient<RT>;

// 3. Define the client struct and service trait
endpoint_client!(CalculatorClient);

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
    let rt = RT::new_multi_thread(8);
    let mut server = RpcServer::new(ServerDefault::new(server_config));
    // 6. dispatch
    use razor_rpc::server::dispatch::Inline;
    let disp = Inline::<Codec, _>::new(CalculatorServer);
    // 7. Start listening
    let actual_addr = server.listen::<RT, ServerProto, _>(rt, "127.0.0.1:8082", disp).await?;
    Ok(actual_addr)
}

async fn use_client(server_addr: &str) {
    use razor_rpc::client::*;
    // 8. ClientFacts
    let mut client_config = ClientConfig::default();
    client_config.task_timeout = 5;
    let rt = RT::new_multi_thread(8);
    let factory = APIFact::<Codec>::new(client_config);
    // 9. Create client connection pool
    let pool: APIConnPool<Codec, ClientProto> = factory.new_conn_pool::<ClientProto, RT>(&rt, server_addr);
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

## Stateful Leader-Follower Service Example

For services with leader-follower architecture (e.g., distributed KV store, Raft cluster), use `APIFailoverPool` with `stateless=false` to maintain leader affinity and handle redirect errors.

```rust
use razor_rpc::client::{endpoint_client, endpoint_async, APIFact, APIFailoverPool, ClientConfig};
use razor_rpc::error::{RpcErrCodec, RpcError, EncodedErr};
use razor_rpc_tcp::{TcpClient, TcpServer};
use std::future::Future;
use std::sync::Arc;

// Define cluster error types
const REDIRECT_PREFIX: &str = "redirect_";

#[derive(Debug, Clone, PartialEq)]
pub enum ClusterErr {
    /// Redirect to leader at specific address
    Redirect(String),
    /// Retry to next node (e.g., node shutting down)
    RetryNext,
    /// Internal error, don't retry
    Internal,
}

impl RpcErrCodec for ClusterErr {
    fn encode<C: razor_rpc::Codec>(&self, _codec: &C) -> EncodedErr {
        match self {
            Self::Redirect(addr) => EncodedErr::Buf(format!("{}{}", REDIRECT_PREFIX, addr).into_bytes()),
            Self::RetryNext => EncodedErr::Static("retry_next"),
            Self::Internal => EncodedErr::Static("internal"),
        }
    }

    fn decode<C: razor_rpc::Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
        if let Err(bytes) = buf {
            let s = unsafe { std::str::from_utf8_unchecked(bytes) };
            if s.starts_with(REDIRECT_PREFIX) {
                Ok(Self::Redirect(s[REDIRECT_PREFIX.len()..].to_string()))
            } else if s == "retry_next" {
                Ok(Self::RetryNext)
            } else if s == "internal" {
                Ok(Self::Internal)
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }

    fn should_failover(&self) -> Result<Option<&str>, ()> {
        match self {
            Self::Redirect(addr) => Ok(Some(addr)),  // Retry to specific leader
            Self::RetryNext => Ok(None),              // Retry to next node
            Self::Internal => Err(()),                // Don't retry
        }
    }
}

// Client definition
endpoint_client!(KVClient);

#[endpoint_async(KVClient)]
pub trait KVService {
    // Note: endpoint_async macro requires exactly one parameter besides &self
    fn put(&self, kv: (String, String)) 
        -> impl Future<Output = Result<(), RpcError<ClusterErr>>> + Send;
    fn get(&self, key: String) 
        -> impl Future<Output = Result<Option<String>, RpcError<String>>> + Send;
}

// Client initialization with failover pool
type RT = orb_tokio::TokioRT;
type Codec = razor_rpc_codec::MsgpCodec;
type FailoverCaller = razor_rpc::client::APIFailoverPool<Codec, TcpClient<RT>>;

impl KVClient<FailoverCaller> {
    pub fn new_cluster_client(
        config: ClientConfig, 
        addrs: Vec<String>, 
        rt: &RT
    ) -> Self {
        let fact = APIFact::<Codec>::new(config);
        // stateless=false: maintain leader affinity for stateful service
        let pool = fact.new_failover::<TcpClient<RT>, RT>(rt, addrs, false, 3);
        KVClient::new(pool)
    }
}

// Usage
async fn example() {
    let rt = RT::new_multi_thread(8);
    let config = ClientConfig::default();
    let addrs = vec![
        "127.0.0.1:8080".to_string(),
        "127.0.0.1:8081".to_string(),
        "127.0.0.1:8082".to_string(),
    ];
    
    let client = KVClient::new_cluster_client(config, addrs, &rt);
    
    // Write goes to leader (with automatic redirect if needed)
    client.put(("key1".to_string(), "value1".to_string())).await.unwrap();
    
    // Read can go to any node
    let value = client.get("key1".to_string()).await.unwrap();
}
```

Key points:
- Use `should_failover()` to control retry behavior: `Ok(Some(addr))` for redirect, `Ok(None)` for retry to next node, `Err(())` to stop retrying
- Set `stateless=false` in `new_failover()` for stateful services to maintain leader affinity
- The client automatically handles redirects and retries based on error type
