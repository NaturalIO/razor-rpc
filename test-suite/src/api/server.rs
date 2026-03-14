use super::service::*;
use nix::errno::Errno;
use razor_rpc::error::RpcError;
use razor_rpc::server::{ServiceMuxDyn, dispatch::Inline, service, service_mux_struct};
use razor_rpc_codec::MsgpCodec;
use razor_rpc_tcp::TcpServer;
use razor_stream::server::{RpcServer, ServerConfig};
use rstest::*;
use std::sync::Arc;

pub type APIServer = razor_rpc::server::ServerDefault;

#[derive(Clone, Debug)]
pub struct CalServer();

#[service]
#[async_trait::async_trait]
impl CalService for CalServer {
    async fn inc(&self, y: isize) -> Result<isize, RpcError<()>> {
        Ok(y + 1)
    }

    async fn add(&self, args: (isize, isize)) -> Result<isize, RpcError<()>> {
        let (a, b) = args;
        Ok(a + b)
    }

    async fn div(&self, args: (isize, isize)) -> Result<isize, RpcError<String>> {
        let (a, b) = args;
        if b == 0 {
            return Err(RpcError::User("divide by zero".to_string()));
        }
        return Ok(a / b);
    }
}

#[derive(Clone, Debug)]
pub struct EchoServer();

#[service]
impl EchoService for EchoServer {
    async fn repeat(&self, msg: String) -> Result<String, RpcError<()>> {
        return Ok(msg);
    }

    async fn io_error(&self, _msg: String) -> Result<(), RpcError<Errno>> {
        return Err(RpcError::User(Errno::EIO));
    }
}

// Create an API server with the given services
pub fn create_api_server(config: ServerConfig) -> RpcServer<APIServer> {
    // NOTE: Do not new rt to the client, pass a handle from TestRunner.
    // since client may be drop by test logic, it's not allow
    // to drop a tokio runtime inside async code.
    let facts = APIServer::new(config);
    let server = RpcServer::new(facts);
    server
}

// Add services to the server and start listening
pub async fn listen_with_services(
    mut server: RpcServer<APIServer>, bind_addr: &str, cal_server: CalServer,
    echo_server: EchoServer, rt: &crate::RT,
) -> Result<(RpcServer<APIServer>, String), Box<dyn std::error::Error>> {
    // Create service mux and add services
    let mut service_mux = ServiceMuxDyn::<MsgpCodec>::new();
    service_mux.add(Arc::new(cal_server));
    service_mux.add(Arc::new(echo_server));

    // Create dispatcher
    let dispatch = Inline::new(Arc::new(service_mux));

    // Listen on the address
    let actual_addr = server
        .listen::<crate::RT, TcpServer<crate::RT>, _>(rt.clone(), bind_addr, dispatch)
        .await?;

    Ok((server, actual_addr))
}

// Fixture for service_mux_struct testing
#[fixture]
pub fn cal_server() -> CalServer {
    CalServer {}
}

#[fixture]
pub fn echo_server() -> EchoServer {
    EchoServer {}
}

// Create service mux dynamic dispatch
pub fn create_service_mux_dispatch(
    cal_server: CalServer, echo_server: EchoServer,
) -> impl razor_stream::server::dispatch::Dispatch {
    let mut service_mux = ServiceMuxDyn::<MsgpCodec>::new();
    service_mux.add(Arc::new(cal_server));
    service_mux.add(Arc::new(echo_server));

    Inline::new(Arc::new(service_mux))
}

// Create service mux struct dispatch
pub fn create_service_mux_struct_dispatch(
    cal_server: CalServer, echo_server: EchoServer,
) -> impl razor_stream::server::dispatch::Dispatch {
    #[service_mux_struct]
    #[derive(Clone)]
    struct TestServiceMux {
        cal: Arc<CalServer>,
        echo: Arc<EchoServer>,
    }

    let service_mux = TestServiceMux { cal: Arc::new(cal_server), echo: Arc::new(echo_server) };

    Inline::<MsgpCodec, TestServiceMux>::new(service_mux)
}

// Fixture that returns a service mux dispatch
#[fixture]
pub fn service_mux_dispatch() -> ServiceMuxDyn<MsgpCodec> {
    ServiceMuxDyn::<MsgpCodec>::new()
}
