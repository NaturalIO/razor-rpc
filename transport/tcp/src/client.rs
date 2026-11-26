use io_buffer::Buffer;
use orb::io::AsyncBufStream;
use orb::net::UnifyStream;
use orb::prelude::*;
use razor_stream::client::task::{ClientTaskDecode, ClientTaskDone};
use razor_stream::client::{ClientConfig, ClientFacts, ClientTransport, registry::ClientTaskItem};
use razor_stream::error::*;
use razor_stream::proto;
use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{MaybeUninit, transmute};
use std::str::FromStr;
use std::time::Duration;

pub const CLIENT_DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct TcpClient<RT: AsyncRuntime> {
    stream: UnsafeCell<AsyncBufStream<UnifyStream<RT>>>,
    resp_buf: UnsafeCell<Vec<u8>>,
    conn_id: String,
    write_timeout: Duration,
}

unsafe impl<RT: AsyncRuntime> Send for TcpClient<RT> {}
unsafe impl<RT: AsyncRuntime> Sync for TcpClient<RT> {}

impl<RT: AsyncRuntime> fmt::Debug for TcpClient<RT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "client {}", self.conn_id)
    }
}

impl<RT: AsyncRuntime> TcpClient<RT> {
    // Because async runtimes does not support splitting read and write to static handler,
    // we use unsafe to achieve such goal,
    #[inline(always)]
    fn get_stream_mut(&self) -> &mut AsyncBufStream<UnifyStream<RT>> {
        unsafe { transmute(self.stream.get()) }
    }

    #[inline(always)]
    fn get_resp_buf(&self, len: usize) -> &mut Vec<u8> {
        let buf: &mut Vec<u8> = unsafe { transmute(self.resp_buf.get()) };
        buf.resize(len, 0);
        buf
    }

    async fn _recv_and_dump(&self, l: usize) -> Result<(), RpcIntErr> {
        let reader = self.get_stream_mut();
        // TODO is there dump on the orb abstraction
        match Buffer::alloc(l as i32) {
            Ok(mut buf) => match reader.read_exact(&mut buf).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    warn!("{:?} recv task failed: {}", self, e);
                    return Err(RpcIntErr::IO);
                }
            },
            Err(_) => {
                warn!("{:?} alloc buf failed", self);
                return Err(RpcIntErr::NoMem);
            }
        }
    }

    #[inline]
    async fn _recv_error<F: ClientFacts>(
        &self, facts: &F, logger: &F::Logger, codec: &F::Codec, resp_head: &proto::RespHead,
        task: ClientTaskItem<F::Task>,
    ) -> Result<(), RpcIntErr> {
        log_debug_assert!(resp_head.flag > 0);
        let reader = self.get_stream_mut();
        match resp_head.flag {
            proto::RESP_FLAG_HAS_ERRNO => {
                let mut task = task.task;
                // In the transport layer, we don't have routing info for last_index/conf_ver
                task.set_custom_error(codec, EncodedErr::Num(resp_head.msg_len.get()), 0, 0);
                facts.error_handle(task);
                return Ok(());
            }
            proto::RESP_FLAG_HAS_ERR_STRING => {
                let buf = self.get_resp_buf(resp_head.blob_len.get() as usize);
                let res = reader.read_exact(buf).await;
                let mut task = task.task;
                match res {
                    Ok(_) => {
                        // Only prefix by rpc_
                        if buf.starts_with(RPC_ERR_PREFIX.as_bytes()) {
                            if let Ok(s) = str::from_utf8(buf) {
                                if let Ok(e) = RpcIntErr::from_str(s) {
                                    task.set_rpc_error(e);
                                    facts.error_handle(task);
                                    return Ok(());
                                }
                            }
                        }
                        // In the transport layer, we don't have routing info for last_index/conf_ver
                        task.set_custom_error(
                            codec,
                            EncodedErr::Buf(Cow::Borrowed(buf.as_ref())),
                            0,
                            0,
                        );
                        facts.error_handle(task);
                        return Ok(());
                    }
                    Err(e) => {
                        logger_warn!(logger, "{:?} recv buffer error: {}", self, e);
                        task.set_rpc_error(RpcIntErr::IO);
                        facts.error_handle(task);
                        return Err(RpcIntErr::IO);
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}

impl<RT: AsyncRuntime> ClientTransport for TcpClient<RT> {
    type RT = RT;

    #[inline]
    async fn connect(addr: &str, conn_id: &str, config: &ClientConfig) -> Result<Self, RpcIntErr> {
        let stream: UnifyStream<RT> = match UnifyStream::<RT>::connect_timeout(
            addr,
            Duration::from_secs(config.connect_timeout as u64),
        )
        .await
        {
            Ok(_stream) => _stream,
            Err(e) => {
                warn!("Cannot connect addr {}: {}", addr, e);
                return Err(RpcIntErr::Unreachable);
            }
        };
        let mut buf_size = config.stream_buf_size;
        if buf_size == 0 {
            buf_size = CLIENT_DEFAULT_BUF_SIZE;
        }
        Ok(Self {
            stream: UnsafeCell::new(AsyncBufStream::new(stream, buf_size)),
            resp_buf: UnsafeCell::new(Vec::with_capacity(512)),
            conn_id: conn_id.to_string(),
            write_timeout: Duration::from_secs(config.write_timeout as u64),
        })
    }

    #[inline(always)]
    async fn close_conn<F: ClientFacts>(&self, logger: &F::Logger) {
        if self.flush_req::<F>(logger).await.is_ok() {
            // stream close is just shutdown on sending, receiver might not be notified on peer dead
            let stream = self.get_stream_mut();
            let _ = stream.get_inner().shutdown_write().await;
        }
    }

    #[inline(always)]
    async fn flush_req<F: ClientFacts>(&self, logger: &F::Logger) -> Result<(), RpcIntErr> {
        let writer = self.get_stream_mut();
        match crate::io_with_timeout!(RT, self.write_timeout, writer.flush()) {
            Ok(()) => {
                logger_trace!(logger, "{:?}: flush_req ok", self);
                Ok(())
            }
            Err(e) => {
                logger_warn!(logger, "{:?} flush_req flush err: {}", self, e);
                return Err(RpcIntErr::IO);
            }
        }
    }

    #[inline(always)]
    async fn write_req<'a, F: ClientFacts>(
        &'a self, logger: &F::Logger, buf: &'a [u8], blob: Option<&'a [u8]>, need_flush: bool,
    ) -> Result<(), RpcIntErr> {
        let writer = self.get_stream_mut();
        crate::io_with_timeout!(RT, self.write_timeout, writer.write_all(buf))?;
        if let Some(blob_buf) = blob {
            match crate::io_with_timeout!(RT, self.write_timeout, writer.write_all(blob_buf)) {
                Ok(()) => {}
                Err(e) => {
                    logger_warn!(logger, "{:?} write_req err: {}", self, e);
                    return Err(RpcIntErr::IO);
                }
            }
        }
        logger_trace!(logger, "{:?}: write req ok flush={need_flush}", self);
        if !need_flush {
        } else {
            self.flush_req::<F>(logger).await?;
        }
        Ok(())
    }

    #[inline]
    async fn read_resp_head<F: ClientFacts>(
        &self, logger: &F::Logger, head: &mut MaybeUninit<proto::RespHead>,
    ) -> Result<(), RpcIntErr> {
        // NOTE: the timeout resp reading process is implemented in ClientStream
        {
            let buf = unsafe { head.assume_init_mut().as_buf_mut() };
            let reader = self.get_stream_mut();
            match reader.read_exact(buf).await {
                Ok(()) => {}
                Err(e) => {
                    logger_debug!(logger, "{:?} rpc client read resp head err: {}", self, e);
                    return Err(e.into());
                }
            }
        }
        unsafe { head.assume_init_ref() }.validate()
    }

    #[inline]
    async fn read_resp_body<F: ClientFacts>(
        &self, facts: &F, logger: &F::Logger, codec: &F::Codec, resp_head: &proto::RespHead,
        o_task: Option<ClientTaskItem<F::Task>>,
    ) -> Result<(), RpcIntErr> {
        // NOTE: the timeout resp reading process is implemented in ClientStream
        let reader = self.get_stream_mut();
        let blob_len = resp_head.blob_len.get();
        let read_buf = self.get_resp_buf(resp_head.msg_len.get() as usize);
        if let Some(mut task_item) = o_task {
            let task = &mut task_item.task;
            if resp_head.flag == 0 {
                if resp_head.msg_len > 0 {
                    match reader.read_exact(read_buf).await {
                        Ok(()) => {}
                        Err(_) => {
                            task.set_rpc_error(RpcIntErr::IO);
                            facts.error_handle(task_item.task);
                            return Err(RpcIntErr::IO);
                        }
                    }
                } // When msg_len == 0, read_buf has 0 size

                if blob_len > 0 {
                    match task.reserve_resp_blob(blob_len) {
                        Some(buf) => {
                            // ensure buf can fit blob_len
                            match reader.read_exact(buf).await {
                                Ok(()) => {}
                                Err(e) => {
                                    logger_warn!(
                                        logger,
                                        "{:?} rpc client reader read ext_buf err: {}",
                                        self,
                                        e
                                    );
                                    task.set_rpc_error(RpcIntErr::IO);
                                    facts.error_handle(task_item.task);
                                    return Err(RpcIntErr::IO);
                                }
                            }
                        }
                        None => {
                            logger_error!(
                                logger,
                                "{:?} rpc client task {:?} has no ext_buf",
                                self,
                                task,
                            );
                            task.set_rpc_error(RpcIntErr::Decode);
                            facts.error_handle(task_item.task);
                            return self._recv_and_dump(blob_len as usize).await;
                        }
                    }
                }
                logger_trace!(logger, "{:?} recv task {:?} ok", self, task);
                if resp_head.msg_len > 0 {
                    // set result of task, and notify task completed
                    if task.decode_resp(codec, read_buf).is_ok() {
                        task.set_ok();
                    } else {
                        logger_warn!(logger, "{:?} rpc client reader decode resp err", self,);
                        task.set_rpc_error(RpcIntErr::Decode);
                        facts.error_handle(task_item.task);
                        return Ok(());
                    }
                } else {
                    task.set_ok();
                }
                task_item.task.done();
                return Ok(());
            } else {
                return self._recv_error(facts, logger, codec, resp_head, task_item).await;
            }
        } else {
            let seq = resp_head.seq;
            logger_trace!(logger, "{:?} timer take_task(seq={}) return None", self, seq);
            let mut data_len = 0;
            if resp_head.flag == 0 {
                data_len += resp_head.msg_len.get() + resp_head.blob_len.get();
            } else if resp_head.flag == proto::RESP_FLAG_HAS_ERR_STRING {
                data_len += resp_head.blob_len.get();
            }
            if data_len > 0 {
                return self._recv_and_dump(data_len as usize).await;
            } else {
                return Ok(());
            }
        }
    }
}
