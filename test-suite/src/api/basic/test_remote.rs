use crate::api::client::MyClient;
use crate::api::server::{
    CalServer, EchoServer, create_api_server, create_service_mux_dispatch,
    create_service_mux_struct_dispatch,
};
use crate::*;
use nix::errno::Errno;
use razor_rpc::client::ClientConfig;
use razor_rpc::error::RpcError;
use razor_rpc::server::ServerConfig;
use razor_rpc_codec::MsgpCodec;
use razor_rpc_tcp::TcpServer;

// Import the service traits to make the methods available
use crate::api::service::{CalService, EchoService};

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

        // Create unique socket path for each test to avoid conflicts
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

        // Create server
        let mut server = create_api_server(server_config.clone());

        let (_server, actual_server_addr) = match dispatch_type.as_str() {
            "service_mux_dyn" => {
                // Use the service mux dyn dispatch
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
                // Use service mux struct dispatch
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

        debug!("API server addr {:?}", actual_server_addr);

        let client = MyClient::<MsgpCodec>::new(client_config, &actual_server_addr, &rt);

        // Test CalService methods
        // Test inc method
        let inc_result = client.cal.inc(41).await.unwrap();
        assert_eq!(inc_result, 42);
        log::info!("inc(41) = {}", inc_result);

        // Test add method
        let add_result = client.cal.add((10, 20)).await.unwrap();
        assert_eq!(add_result, 30);
        log::info!("add(10, 20) = {}", add_result);

        // Test div method with normal case
        let div_result = client.cal.div((10, 2)).await.unwrap();
        assert_eq!(div_result, 5);
        log::info!("div(10, 2) = {}", div_result);

        // Test div method with error case (divide by zero)
        let div_error = client.cal.div((10, 0)).await.unwrap_err();
        match div_error {
            RpcError::User(msg) => {
                assert_eq!(msg, "divide by zero");
            }
            _ => panic!("Expected User error with 'divide by zero' message"),
        }
        log::info!("div(10, 0) correctly returned error: divide by zero");

        // Test EchoService methods
        // Test repeat method
        let echo_result = client.echo.repeat("Hello, world!".to_string()).await.unwrap();
        assert_eq!(echo_result, "Hello, world!");
        log::info!("repeat('Hello, world!') = '{}'", echo_result);

        // Test io_error method
        let io_error_result = client.echo.io_error("test".to_string()).await.unwrap_err();
        match io_error_result {
            RpcError::User(errno) => {
                assert_eq!(errno, Errno::EIO);
            }
            _ => panic!("Expected User error with EIO errno"),
        }
        log::info!("io_error correctly returned EIO error");
    });
}
