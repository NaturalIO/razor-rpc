use crate::stream::{client::*, server::*};
use crate::*;
use crossfire::mpsc;
use razor_stream::client::{ClientCaller, ClientConfig, task::ClientTaskGetResult};
use razor_stream::server::{ServerConfig, task::ServerTaskDone};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[logfn]
#[rstest]
#[case(true)] // round_robin
#[case(false)] // not round_robin
fn test_failover_on_server_exit(runner: TestRunner, #[case] round_robin: bool) {
    let rt = runner.rt.clone();
    runner.block_on(async move {
        let client_config = ClientConfig::default();
        let server_config = ServerConfig::default();

        // Track which server handles requests
        let server1_requests = Arc::new(AtomicUsize::new(0));
        let server2_requests = Arc::new(AtomicUsize::new(0));

        // Server 1 dispatch logic
        let dispatch_task1 = {
            let counter = server1_requests.clone();
            move |task: FileServerTask| {
                async move {
                    // Increment counter for server 1
                    counter.fetch_add(1, Ordering::SeqCst);

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
            }
        };

        // Server 2 dispatch logic
        let dispatch_task2 = {
            let counter = server2_requests.clone();
            move |task: FileServerTask| {
                async move {
                    // Increment counter for server 2
                    counter.fetch_add(1, Ordering::SeqCst);

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
            }
        };

        let is_tcp = true;
        // Start server 1
        let server_bind_addr1 = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-failover-1" };
        let (server1, server1_addr) = init_server_closure::<_, _, crate::RT>(
            dispatch_task1,
            server_config.clone(),
            &server_bind_addr1,
            rt.clone(),
        )
        .await
        .expect("server1 listen");

        // Start server 2
        let server_bind_addr2 = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-failover-2" };
        let (_server2, server2_addr) = init_server_closure::<_, _, crate::RT>(
            dispatch_task2,
            server_config.clone(),
            &server_bind_addr2,
            rt.clone(),
        )
        .await
        .expect("server2 listen");
        println!("server1 {} server2 {}", server1_addr, server2_addr);

        // Create failover client (without round-robin for clearer failover behavior)
        let addrs = vec![server1_addr.clone(), server2_addr.clone()];
        let client = init_failover_client(client_config, addrs, round_robin, &rt).await;

        // Send some requests to establish connection to server 1 (primary)
        let (tx, rx) = mpsc::unbounded_async();
        const INITIAL_REQUESTS: usize = 5000;

        for i in 0..INITIAL_REQUESTS {
            let open_task = FileClientTaskOpen::new(tx.clone(), format!("/tmp/file_{}.txt", i));
            client.send_req(open_task.into()).await;
        }
        // Wait until all initial requests are processed by server 1

        let mut server1_count_before;
        loop {
            server1_count_before = server1_requests.load(Ordering::SeqCst);
            println!(
                "server1 {} server2 {}",
                server1_count_before,
                server2_requests.load(Ordering::SeqCst)
            );
            if server1_count_before >= INITIAL_REQUESTS / 2 {
                break;
            }
            crate::RT::sleep(Duration::from_secs(1)).await;
        }
        log::info!("Server 1 processed {} initial requests", server1_count_before);

        // Stop server 1 to trigger failover
        drop(server1);
        println!("drop");

        // Send more requests that should be handled by server 2 due to failover
        const FAILOVER_REQUESTS: usize = 10000;
        for i in INITIAL_REQUESTS..(INITIAL_REQUESTS + FAILOVER_REQUESTS) {
            let open_task = FileClientTaskOpen::new(tx.clone(), format!("/tmp/file_{}.txt", i));
            client.send_req(open_task.into()).await;
        }

        // Wait for all tasks to complete
        drop(tx);

        // Collect responses
        let mut recv_count = 0;
        while let Ok(task) = rx.recv().await {
            recv_count += 1;
            let r = task.get_result();
            assert!(r.is_ok(), "task err: {:?}", r);
        }

        // All requests should succeed
        assert_eq!(recv_count, INITIAL_REQUESTS + FAILOVER_REQUESTS);

        // Verify that server 2 handled the failover requests
        let server1_count = server1_requests.load(Ordering::SeqCst);
        let server2_count = server2_requests.load(Ordering::SeqCst);
        println!("server1 {} server2 {}", server1_count, server2_count);

        log::info!(
            "Failover test completed: Server1={}, Server2={}, Total={}",
            server1_count,
            server2_count,
            recv_count
        );

        // Server 1 should have handled the initial requests
        assert!(server1_count < server2_count);
    });
}
