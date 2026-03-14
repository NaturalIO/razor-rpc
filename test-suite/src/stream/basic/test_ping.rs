use crate::stream::{client::*, server::*};
use crate::*;
use razor_stream::client::ClientConfig;
use razor_stream::server::{ServerConfig, task::ServerTaskDone};

#[logfn]
#[rstest]
#[case(true)]
#[case(false)]
fn test_client_ping(runner: TestRunner, #[case] is_tcp: bool) {
    let client_config = ClientConfig::default();
    let server_config = ServerConfig::default();
    let rt_server = runner.rt.clone();
    let rt = runner.rt.clone();
    // The server dispatch logic doesn't need to do anything special for pings,
    // as they are handled by the framework.
    let dispatch_task = move |task: FileServerTask| {
        async move {
            info!("Server received a task, which shouldn't happen in the ping test.");
            // In a real scenario, handle tasks. For this test, we only expect pings.
            // If a real task arrives, just complete it successfully.
            match task {
                FileServerTask::Open(open_task) => {
                    open_task.set_result(Ok(()));
                }
                FileServerTask::IO(mut io_task) => {
                    io_task.resp = Some(Default::default());
                    io_task.set_result(Ok(()));
                }
            }
            Ok(())
        }
    };

    runner.block_on(async move {
        let server_bind_addr = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-test-socket" };
        let (_server, actual_server_addr) = init_server_closure(
            dispatch_task,
            server_config.clone(),
            &server_bind_addr,
            &rt_server,
        )
        .await
        .expect("server listen");
        debug!("client addr {:?}", actual_server_addr);
        let mut client = init_client(client_config, &actual_server_addr, None, &rt)
            .await
            .expect("connect client");

        // Send several pings
        for i in 0..3 {
            info!("Client sending ping {}", i + 1);
            let result = client.ping().await;
            assert!(result.is_ok());
            info!("Ping {} successful.", i + 1);
            // Give some time between pings
            crate::RT::sleep(std::time::Duration::from_millis(100)).await;
        }

        assert!(!client.is_closed());
        log::info!("Ping test completed successfully.");
    });
}
