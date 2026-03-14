use crate::stream::{client::*, server::*};
use crate::*;
use crossfire::mpsc;
use razor_stream::client::{ClientConfig, task::ClientTaskGetResult};
use razor_stream::server::{ServerConfig, task::ServerTaskDone};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[logfn]
#[rstest]
#[case(true)]
#[case(false)]
fn test_client_drop(runner: TestRunner, #[case] is_tcp: bool) {
    let client_config = ClientConfig::default();
    let server_config = ServerConfig::default();
    let request_count = Arc::new(AtomicUsize::new(0));
    let dispatch_task = {
        let req_count = request_count.clone();
        move |task: FileServerTask| {
            let count = req_count.fetch_add(1, Ordering::SeqCst);
            async move {
                match task {
                    FileServerTask::Open(open_task) => {
                        open_task.set_result(Ok(()));
                    }
                    FileServerTask::IO(mut io_task) => {
                        async_spawn_detach!(async move {
                            // delay 2 secs to response IO
                            RT::sleep(Duration::from_secs(2)).await;
                            info!("Server processing request {} {:?}", count, io_task);

                            io_task.resp = Some(Default::default());
                            io_task.set_result(Ok(()));
                        });
                    }
                }
                Ok(())
            }
        }
    };

    let rt = runner.rt.clone();
    runner.block_on(async move {
        let server_bind_addr = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-test-socket" };
        let (_server, actual_server_addr) = init_server_closure::<_, _, crate::RT>(
            dispatch_task,
            server_config.clone(),
            &server_bind_addr,
            rt.clone(),
        )
        .await
        .expect("server listen");
        debug!("client addr {:?}", actual_server_addr);
        let mut client = init_client(client_config, &actual_server_addr, None, &rt)
            .await
            .expect("connect client");

        let (tx, rx) = mpsc::unbounded_async();

        const TEST_ROUND: usize = 20;
        // First two tasks should succeed
        for i in 0..TEST_ROUND {
            let open_task = FileClientTaskOpen::new(tx.clone(), format!("/tmp/file_{}.txt", i));
            client.send_task(open_task.into(), false).await.expect("send task");
            let read_task = FileClientTaskRead::new(tx.clone(), i as u64, 0, 10);
            client.send_task(read_task.into(), false).await.expect("send task");
        }
        client.flush_req().await.expect("flush");
        drop(client);
        drop(tx);
        // The client.recv_loop should ensure all task received after drop

        let mut recv_count = 0;
        while let Ok(task) = rx.recv().await {
            recv_count += 1;
            info!("recv {:?}", task);
            let r = task.get_result();
            assert!(r.is_ok(), "task err: {:?}", r);
        }
        assert_eq!(recv_count, TEST_ROUND * 2);
        log::info!("Connection drop test completed successfully.");
    });
}
