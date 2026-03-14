use crossfire::mpsc;
use io_buffer::{Buffer, rand_buffer};
use nix::errno::Errno;
use razor_stream::client::{
    ClientConfig,
    task::{ClientTaskDone, ClientTaskEncode, ClientTaskGetResult},
};
use razor_stream::proto::{RPC_REQ_HEADER_LEN, RpcAction};
use razor_stream::server::{
    ServerConfig,
    task::{ServerTaskAction, ServerTaskDone},
};
use razor_rpc_test::stream::{client::*, server::*};
use razor_rpc_test::*;
use std::convert::TryFrom;
use std::time::Instant;
use captains_log::*;
use rstest::*;

#[logfn]
#[rstest]
#[case(true)]
#[case(false)]
fn test_throughput(runner: TestRunner, #[case] is_tcp: bool) {
    let client_config = ClientConfig::default();
    let server_config = ServerConfig::default();
    let rt = &runner.rt;

    let dispatch_task = move |task: FileServerTask| async move {
        match task {
            FileServerTask::Open(open_task) => {
                open_task.set_result(Ok(()));
                Ok(())
            }
            FileServerTask::IO(mut io_task) => {
                let action = match io_task.get_action() {
                    RpcAction::Num(action_num) => FileAction::try_from(action_num as u8).unwrap(),
                    _ => unreachable!(),
                };
                match action {
                    FileAction::Write => {
                        if let Some(blob) = io_task.req_blob.take() {
                            let ret_size = blob.len() as u64;
                            io_task.resp = Some(FileIOResp { ret_size });
                            io_task.set_result(Ok(()));
                        } else {
                            io_task.set_result(Err(Errno::EINVAL));
                        }
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            }
        }
    };

    runner.block_on(async move {
        log::set_max_level(LevelFilter::Info);
        let server_bind_addr = if is_tcp { "127.0.0.1:0" } else { "/tmp/razor-rpc-test-socket" };
        let (_server, actual_server_addr) =
            init_server(dispatch_task, server_config.clone(), server_bind_addr)
                .expect("server listen");
        println!("actual_addr: {}", actual_server_addr);
        let mut client = if is_tcp {
            init_client(client_config, &actual_server_addr, None, rt).await.expect("connect client")
        } else {
            init_client(client_config, server_bind_addr, None, rt).await.expect("connect client")
        };

        // Test Open task
        let (tx, rx) = mpsc::unbounded_async();
        let open_task = FileClientTaskOpen::new(tx.clone(), "/tmp/test_file.txt".to_string());
        client.send_task(open_task.into(), true).await.expect("send open task");
        let completed_open_task = rx.recv().await.unwrap();
        assert!(matches!(completed_open_task, FileClientTask::Open(_)));
        assert!(completed_open_task.get_result().is_ok());

        // Test Write task with 64k blob
        let data_len = 64 * 1024;
        let mut write_data = Buffer::alloc(data_len).expect("alloc");
        rand_buffer(&mut write_data);

        let write_task = FileClientTaskWrite::new(tx.clone(), 1, 0, write_data.clone());
        let mut task: FileClientTask = write_task.into();
        let codec = razor_rpc_test::Codec::default();
        let mut req_buf = Vec::new();
        task.encode_req(&codec, &mut req_buf).expect("encode");
        let req_size = req_buf.len() + RPC_REQ_HEADER_LEN + data_len as usize;
        task.set_ok();
        task.done();
        println!("pre recv");
        let _ = rx.recv().await; // consume one channel tx ref
        println!("req_size: {}", req_size);

        let start = Instant::now();
        crate::RT::spawn_detach(async move {
            for _ in 0..100000 {
                let write_task = FileClientTaskWrite::new(tx.clone(), 1, 0, write_data.clone());
                client.send_task(write_task.into(), false).await.expect("send write task");
            }
            client.flush_req().await.expect("flush");
            drop(client);
            drop(tx);
            println!("send done");
        });
        let mut recv_count = 0;
        while let Ok(task) = rx.recv().await {
            assert!(task.get_result().is_ok());
            recv_count += 1;
        }
        let duration = start.elapsed();
        let throughput_bytes_per_sec = (req_size * recv_count) as f64 / duration.as_secs_f64();
        let throughput_mb_per_sec = throughput_bytes_per_sec / (1024.0 * 1024.0);
        println!(
            "{server_bind_addr} Write 64k blob task completed in: {:?}, Throughput: {:.2} MB/s",
            duration, throughput_mb_per_sec
        );
    });
}
