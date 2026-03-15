use crate::*;
use nix::errno::Errno;
use razor_rpc::client::{APIConnPool, APIFact, ClientConfig, endpoint_async, endpoint_client};
use razor_rpc::error::RpcError;
use razor_rpc::server::{
    ServerConfig, ServiceMuxDyn, dispatch::Inline, service, service_mux_struct,
};
use razor_rpc_codec::MsgpCodec;
use razor_rpc_tcp::{TcpClient, TcpServer};
use razor_stream::server::RpcServer;
use rstest::*;
use std::future::Future;
use std::sync::Arc;

// =============================================================================
// Service Traits and Client
// =============================================================================

// Single client that implements both services
endpoint_client!(MyClient);

#[endpoint_async(MyClient)]
#[async_trait::async_trait]
pub trait CalService {
    async fn inc(&self, y: isize) -> Result<isize, RpcError<()>>;
    async fn add(&self, args: (isize, isize)) -> Result<isize, RpcError<()>>;
    async fn div(&self, args: (isize, isize)) -> Result<isize, RpcError<String>>;
}

#[endpoint_async(MyClient)]
pub trait EchoService {
    fn repeat(&self, msg: String) -> impl Future<Output = Result<String, RpcError<()>>> + Send;
    fn io_error(&self, _msg: String) -> impl Future<Output = Result<(), RpcError<Errno>>> + Send;
}

pub type PoolCaller = APIConnPool<MsgpCodec, TcpClient<crate::RT>>;

impl MyClient<PoolCaller> {
    pub fn new_client(config: ClientConfig, addr: &str, rt: &crate::RT) -> Self {
        let facts = APIFact::<MsgpCodec>::new(config);
        let pool = facts.new_conn_pool::<TcpClient<crate::RT>, crate::RT>(rt, addr);
        MyClient::new(pool)
    }
}

// =============================================================================
// Server Implementations
// =============================================================================

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
        Ok(a / b)
    }
}

#[derive(Clone, Debug)]
pub struct EchoServer();

#[service]
impl EchoService for EchoServer {
    async fn repeat(&self, msg: String) -> Result<String, RpcError<()>> {
        Ok(msg)
    }

    async fn io_error(&self, _msg: String) -> Result<(), RpcError<Errno>> {
        Err(RpcError::User(Errno::EIO))
    }
}

// =============================================================================
// Server Helpers
// =============================================================================

pub fn create_api_server(config: ServerConfig) -> RpcServer<APIServer> {
    let facts = APIServer::new(config);
    RpcServer::new(facts)
}

pub fn create_service_mux_dispatch(
    cal_server: CalServer, echo_server: EchoServer,
) -> impl razor_stream::server::dispatch::Dispatch {
    let mut service_mux = ServiceMuxDyn::<MsgpCodec>::new();
    service_mux.add(Arc::new(cal_server));
    service_mux.add(Arc::new(echo_server));
    Inline::new(Arc::new(service_mux))
}

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

// =============================================================================
// Tests
// =============================================================================

#[fixture]
pub fn cal_server() -> CalServer {
    CalServer {}
}

#[fixture]
pub fn echo_server() -> EchoServer {
    EchoServer {}
}

#[fixture]
pub fn service_mux_dispatch() -> ServiceMuxDyn<MsgpCodec> {
    ServiceMuxDyn::<MsgpCodec>::new()
}

#[logfn]
#[rstest]
#[case(true, "service_mux_dyn")]
#[case(false, "service_mux_dyn")]
#[case(true, "service_mux_struct")]
#[case(false, "service_mux_struct")]
fn test_api_remote_calls(runner: TestRunner, #[case] is_tcp: bool, #[case] dispatch_type: String) {
    let rt = runner.rt.clone();
    runner.block_on(async move {
        let client_config = ClientConfig::default();
        let server_config = ServerConfig::default();

        let server_bind_addr = if is_tcp {
            "127.0.0.1:0".to_string()
        } else {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            format!("/tmp/razor-rpc-test-api-socket-{}-{}", dispatch_type, timestamp)
        };

        let cal_server = CalServer {};
        let echo_server = EchoServer {};

        let mut server = create_api_server(server_config.clone());

        let (_server, actual_server_addr) = match dispatch_type.as_str() {
            "service_mux_dyn" => {
                let dispatch = create_service_mux_dispatch(cal_server, echo_server);
                let actual_addr = server
                    .listen::<crate::RT, TcpServer<crate::RT>, _>(
                        rt.clone(),
                        &server_bind_addr,
                        dispatch,
                    )
                    .await
                    .expect("server listen");
                (server, actual_addr)
            }
            "service_mux_struct" => {
                let dispatch = create_service_mux_struct_dispatch(cal_server, echo_server);
                let actual_addr = server
                    .listen::<crate::RT, TcpServer<crate::RT>, _>(
                        rt.clone(),
                        &server_bind_addr,
                        dispatch,
                    )
                    .await
                    .expect("server listen");
                (server, actual_addr)
            }
            _ => panic!("Unknown dispatch type: {}", dispatch_type),
        };

        log::debug!("API server addr {:?}", actual_server_addr);

        let client = MyClient::new_client(client_config, &actual_server_addr, &rt);

        // Test CalService methods
        let inc_result = client.inc(41).await.unwrap();
        assert_eq!(inc_result, 42);
        log::info!("inc(41) = {}", inc_result);

        let add_result = client.add((10, 20)).await.unwrap();
        assert_eq!(add_result, 30);
        log::info!("add(10, 20) = {}", add_result);

        let div_result = client.div((10, 2)).await.unwrap();
        assert_eq!(div_result, 5);
        log::info!("div(10, 2) = {}", div_result);

        let div_error = client.div((10, 0)).await.unwrap_err();
        match div_error {
            RpcError::User(msg) => {
                assert_eq!(msg, "divide by zero");
            }
            _ => panic!("Expected User error with 'divide by zero' message"),
        }
        log::info!("div(10, 0) correctly returned error: divide by zero");

        // Test EchoService methods
        let echo_result = client.repeat("Hello, world!".to_string()).await.unwrap();
        assert_eq!(echo_result, "Hello, world!");
        log::info!("repeat('Hello, world!') = '{}'", echo_result);

        let io_error_result = client.io_error("test".to_string()).await.unwrap_err();
        match io_error_result {
            RpcError::User(errno) => {
                assert_eq!(errno, Errno::EIO);
            }
            _ => panic!("Expected User error with EIO errno"),
        }
        log::info!("io_error correctly returned EIO error");
    });
}
