use crate::stream::{client::*, server::*};
use crate::*;
use crossfire::mpsc;
use io_buffer::{Buffer, rand_buffer}; // Added rand_buffer
use nix::errno::Errno;
use razor_stream::client::{ClientConfig, task::ClientTaskGetResult};
use razor_stream::proto::RpcAction;
use razor_stream::server::{
    ServerConfig,
    task::{ServerTaskAction, ServerTaskDone},
};

use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

#[logfn]
#[rstest]
#[case(true)]
#[case(false)]
fn test_client_server(runner: TestRunner, #[case] is_tcp: bool) {
    let client_config = ClientConfig::default();
    let server_config = ServerConfig::default();
    let rt_server = runner.rt.clone();
    let rt = runner.rt.clone();

    let store: Arc<Mutex<Option<Buffer>>> = Arc::new(Mutex::new(None));

    let dispatch_task = {
        let _store = store.clone();
        move |task: FileServerTask| {
            async move {
                match task {
                    FileServerTask::Open(open_task) => {
                        info!("Server received Open task: {:?}", open_task.req);
                        // Simulate opening a file
                        open_task.set_result(Ok(()));
                        Ok(())
                    }
                    FileServerTask::IO(mut io_task) => {
                        match io_task.get_action() {
                            RpcAction::Num(action_num) => {
                                let action =
                                    FileAction::try_from(action_num as u8).map_err(|e| {
                                        log::error!(
                                            "Error converting action num to FileAction: {}",
                                            e
                                        );
                                        ()
                                    })?;

                                match action {
                                    FileAction::Read => {
                                        info!("Server received Read task: {:?}", io_task.req);
                                        // Retrieve stored data
                                        let data_to_read = _store
                                            .lock()
                                            .unwrap()
                                            .take()
                                            .unwrap_or_else(|| Buffer::from(vec![]));
                                        let ret_size = data_to_read.len() as u64;

                                        // Set response blob
                                        io_task.resp_blob = Some(data_to_read);

                                        // Set response message
                                        io_task.resp = Some(FileIOResp { ret_size });

                                        io_task.set_result(Ok(()));
                                        Ok(())
                                    }
                                    FileAction::Write => {
                                        info!("Server received Write task: {:?}", io_task.req);
                                        // Simulate writing data
                                        if let Some(blob) = io_task.req_blob.take() {
                                            info!("Server received blob data len: {}", blob.len());
                                            let ret_size = blob.len() as u64;
                                            _store.lock().unwrap().replace(blob);
                                            io_task.resp = Some(FileIOResp { ret_size });
                                            io_task.set_result(Ok(()));
                                        } else {
                                            log::error!("Write task received without blob data.");
                                            io_task.set_result(Err(Errno::EINVAL));
                                        }
                                        Ok(())
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            _ => {
                                log::error!("Unexpected RpcAction type for IO task.");
                                io_task.set_result(Err(Errno::ENOSYS));
                                Err(())
                            }
                        }
                    }
                }
            }
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

        // Test Open task
        let (tx, rx) = mpsc::unbounded_async();
        let open_task = FileClientTaskOpen::new(tx.clone(), "/tmp/test_file.txt".to_string());
        client.send_task(open_task.into(), true).await.expect("send open task");
        let completed_open_task = rx.recv().await.unwrap();
        assert!(matches!(completed_open_task, FileClientTask::Open(_)));
        assert!(
            completed_open_task.get_result().is_ok(),
            "res {:?}",
            completed_open_task.get_result()
        );
        log::info!("Open task completed successfully.");

        // Test Write task
        let data_len = 1024;
        let mut write_data = Buffer::alloc(data_len).expect("alloc");
        rand_buffer(&mut write_data);
        let write_task = FileClientTaskWrite::new(tx.clone(), 1, 0, write_data.clone());
        client.send_task(write_task.into(), true).await.expect("send write task");
        let completed_write_task = rx.recv().await.unwrap();
        assert!(matches!(completed_write_task, FileClientTask::Write(_)));
        assert!(completed_write_task.get_result().is_ok());
        if let FileClientTask::Write(task) = completed_write_task {
            assert_eq!(task.resp.unwrap().ret_size, write_data.len() as u64);
        }
        log::info!("Write task completed successfully.");

        // Test Read task
        let read_task = FileClientTaskRead::new(tx.clone(), 1, 0, data_len as usize);
        client.send_task(read_task.into(), true).await.expect("send read task");
        let completed_read_task = rx.recv().await.unwrap();
        assert!(matches!(completed_read_task, FileClientTask::Read(_)));
        assert!(completed_read_task.get_result().is_ok());
        if let FileClientTask::Read(task) = completed_read_task {
            assert_eq!(task.resp.unwrap().ret_size, data_len as u64);
            assert_eq!(task.read_data.as_ref().unwrap().as_ref(), write_data.as_ref()); // Compare with original random data
        }
        log::info!("Read task completed successfully.");
    });
}
