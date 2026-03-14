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
#[case(true)]
#[case(false)]
fn test_round_robin_distribution(runner: TestRunner, #[case] is_tcp: bool) {
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

        // Start server 1
        let server_bind_addr1 = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-test-1" };
        let (_server1, server1_addr) = init_server_closure::<_, _, crate::RT>(
            dispatch_task1,
            server_config.clone(),
            &server_bind_addr1,
            rt.clone(),
        )
        .await
        .expect("server1 listen");

        // Start server 2
        let server_bind_addr2 = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-test-2" };
        let (_server2, server2_addr) = init_server_closure::<_, _, crate::RT>(
            dispatch_task2,
            server_config.clone(),
            &server_bind_addr2,
            rt.clone(),
        )
        .await
        .expect("server2 listen");

        // Create failover client with round-robin enabled
        let addrs = vec![server1_addr.clone(), server2_addr.clone()];
        let client = init_failover_client(client_config, addrs, true /* round_robin */, &rt).await;

        // Send multiple requests and verify distribution
        let (tx, rx) = mpsc::unbounded_async::<FileClientTask>();
        const REQUEST_COUNT: usize = 100000;
        const BATCH: usize = 5;

        let th = async_spawn!(async move {
            // Collect responses
            let mut recv_count = 0;
            while let Ok(task) = rx.recv().await {
                recv_count += 1;
                let r = task.get_result();
                assert!(r.is_ok(), "task err: {:?}", r);
            }
            recv_count
        });
        for _j in 0..BATCH {
            for i in 0..REQUEST_COUNT {
                let open_task = FileClientTaskOpen::new(tx.clone(), format!("/tmp/file_{}.txt", i));
                client.send_req(open_task.into()).await;
            }
            println!("sleep 3 sec");
            crate::RT::sleep(Duration::from_secs(3)).await;
        }
        // Wait for all tasks to complete
        drop(tx);

        let recv_count = async_join_result!(th);
        assert_eq!(recv_count, REQUEST_COUNT * BATCH);
        drop(client);

        // Verify round-robin distribution
        let server1_count = server1_requests.load(Ordering::SeqCst);
        let server2_count = server2_requests.load(Ordering::SeqCst);

        // With round-robin, each server should handle approximately half the requests
        let expected_per_server = recv_count / 2;
        let tolerance = 2; // Allow small variance due to timing
        println!("server1 {} server2 {}", server1_count, server2_count);

        assert!(
            (server1_count as i32 - expected_per_server as i32).abs() <= tolerance,
            "Server 1 handled {} requests, expected ~{}",
            server1_count,
            expected_per_server
        );
        assert!(
            (server2_count as i32 - expected_per_server as i32).abs() <= tolerance,
            "Server 2 handled {} requests, expected ~{}",
            server2_count,
            expected_per_server
        );

        log::info!(
            "Round-robin test completed: Server1={}, Server2={}, Total={}",
            server1_count,
            server2_count,
            recv_count
        );
        log::info!("sleep to see if log in FailoverPool drop occur");
        crate::RT::sleep(Duration::from_secs(3)).await;
    });
}
