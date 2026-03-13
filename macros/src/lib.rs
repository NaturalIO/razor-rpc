extern crate proc_macro;
use proc_macro::TokenStream;

mod endpoint_async;
mod endpoint_client;
mod service;
mod service_mux_struct;

/// The `#[service]` macro is applied to an `impl` block to automatically generate the `ServiceTrait` implementation for the type.
///
/// - When applied to an inherent `impl` block (without `impl Trait`), methods intended as service methods should be marked with `#[method]`.
/// - When applied to a trait `impl` block, all methods defined in the trait will be registered as service methods.
/// - All service methods must return `Result<T, RpcError<E>>`, where `E` is a user-defined error type that implements `RpcErrCodec`.
///
/// The service method recognizes:
/// - `fn` (which is considered non-blocking)
/// - `async fn`
/// - `impl Future`
/// - trait methods wrapped by `async_trait`
///
/// # Usage
///
/// Without `impl Trait` (inherent implementation):
///
/// ```rust
/// use razor_rpc::server::{service, method};
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
/// use razor_rpc::server::ServiceStatic;
/// use razor_rpc_codec::MsgpCodec;
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct AddArgs {
///     a: i32,
///     b: i32,
/// }
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct AddResp {
///     result: i32,
/// }
///
/// pub struct CalculatorService;
///
/// #[service]
/// impl CalculatorService {
///     #[method]
///     async fn add(&self, args: AddArgs) -> Result<AddResp, RpcError<()>> {
///         Ok(AddResp { result: args.a + args.b })
///     }
/// }
///
/// // This will generate a ServiceStatic implementation that can be used as:
/// // let service = CalculatorService;
/// // service.serve(request).await;
/// ```
///
/// With trait implementation using `impl Future`:
///
/// ```rust
/// use razor_rpc::server::service;
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
/// use std::future::Future;
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct EchoArgs {
///     message: String,
/// }
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct EchoResp {
///     echoed: String,
/// }
///
/// pub trait EchoService: Send + Sync {
///     fn echo(&self, args: EchoArgs) -> impl Future<Output = Result<EchoResp, RpcError<()>>> + Send;
/// }
///
/// pub struct EchoServiceImpl;
///
/// #[service]
/// impl EchoService for EchoServiceImpl {
///     fn echo(&self, args: EchoArgs) -> impl Future<Output = Result<EchoResp, RpcError<()>>> + Send {
///         async move {
///             Ok(EchoResp { echoed: args.message })
///         }
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn service(_attr: TokenStream, item: TokenStream) -> TokenStream {
    service::service(_attr, item)
}

/// A marker attribute for methods in an inherent `impl` block that should be exposed as RPC methods.
/// This is not needed when using a trait-based implementation.
///
/// Refer to document of attr macro `#[service]`
#[proc_macro_attribute]
pub fn method(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// The `#[service_mux_struct]` macro is applied to a **struct** to implement `ServiceTrait` on it.
/// It acts as a dispatcher, routing `serve()` calls to the correct service based on the `req.service` field.
///
/// Each field in the struct must hold a service that implements `ServiceTrait` (e.g., wrapped in an `Arc`).
/// The macro generates a `serve` implementation that matches `req.service` against the field names of the struct.
///
/// # Usage
///
/// ```rust
/// use razor_rpc::server::{service, service_mux_struct, method};
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
/// use std::sync::Arc;
///
/// // Define request/response types
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct MathArgs {
///     value: i32,
/// }
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct MathResp {
///     result: i32,
/// }
///
/// // Define services
/// pub struct AddService;
/// #[service]
/// impl AddService {
///     #[method]
///     async fn add(&self, args: MathArgs) -> Result<MathResp, RpcError<()>> {
///         Ok(MathResp { result: args.value + 1 })
///     }
/// }
///
/// pub struct MultiplyService;
/// #[service]
/// impl MultiplyService {
///     #[method]
///     async fn multiply(&self, args: MathArgs) -> Result<MathResp, RpcError<()>> {
///         Ok(MathResp { result: args.value * 2 })
///     }
/// }
///
/// // Create a service multiplexer
/// #[service_mux_struct]
/// pub struct ServiceMux {
///     pub add: Arc<AddService>,
///     pub multiply: Arc<MultiplyService>,
/// }
///
/// // Usage:
/// // let mux = ServiceMux {
/// //     add: Arc::new(AddService),
/// //     multiply: Arc::new(MultiplyService),
/// // };
/// ```
#[proc_macro_attribute]
pub fn service_mux_struct(_attr: TokenStream, item: TokenStream) -> TokenStream {
    service_mux_struct::service_mux_struct(_attr, item)
}

/// The `endpoint_client!` macro generates a client struct that can be used to make remote API calls.
///
/// This macro should be used first to define the client struct, then use `#[endpoint_async]`
/// to implement service traits for the client.
///
/// # Usage
///
/// ```rust
/// use razor_rpc::client::endpoint_client;
///
/// endpoint_client!(MyClient);
///
/// // This generates MyClient<C> struct with:
/// // - new(caller: C) constructor
/// // - Clone implementation
/// // - AsyncEndpoint<C> implementation
/// ```
#[proc_macro]
pub fn endpoint_client(input: TokenStream) -> TokenStream {
    endpoint_client::endpoint_client(input)
}

/// The `#[endpoint_async]` macro applies to a trait to implement it for a client struct.
///
/// The client struct must be defined beforehand using `#[endpoint_client(ClientName)]`.
///
/// Rules:
///     - trait can be wrapped async_trait, optionally.
///     - If trait is not async_trait, then the method should use the signature `impl Future + Send`
///     - No fn is allowed.
///     - All method should have one and only argument, and return type should be `Result<Resp, RpcError<E>>`, where `E: RpcErrCodec`
///
/// # Usage
///
/// ```rust
/// use razor_rpc::client::{endpoint_client, endpoint_async};
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
/// use std::future::Future;
///
/// #[derive(Debug, Deserialize, Serialize)]
/// pub struct AddArgs {
///     pub a: i32,
///     pub b: i32,
/// }
///
/// #[derive(Debug, Deserialize, Serialize, Default)]
/// pub struct AddResp {
///     pub result: i32,
/// }
///
/// // Step 1: Define the client struct
/// endpoint_client!(CalculatorClient);
///
/// // Step 2: Define a service trait and implement it for the client
/// #[endpoint_async(CalculatorClient)]
/// pub trait CalculatorService {
///     fn add(&self, args: AddArgs) -> impl Future<Output = Result<AddResp, RpcError<()>>> + Send;
/// }
///
/// // Usage:
/// // let client = CalculatorClient::new(caller);
/// // let result = client.add(AddArgs { a: 1, b: 2 }).await;
/// ```
///
/// The macro also supports `async fn` methods when used with `#[async_trait]`.
#[proc_macro_attribute]
pub fn endpoint_async(attr: TokenStream, item: TokenStream) -> TokenStream {
    endpoint_async::endpoint_async(attr, item)
}
