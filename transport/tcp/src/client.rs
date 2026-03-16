use captains_log::filter::LogFilter;
use crossfire::{AsyncRx, mpsc};
use io_buffer::Buffer;
use orb::net::UnifyStream;
use orb::prelude::*;
use orb::{io::AsyncBufStream, utils::Cancellable};
use razor_stream::client::task::{ClientTaskDecode, ClientTaskDone};
use razor_stream::client::timer::ClientTaskTimer;
use razor_stream::client::{ClientConfig, ClientFacts, ClientTransport};
use razor_stream::error::*;
use razor_stream::proto;
use std::cell::UnsafeCell;
use std::mem::transmute;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt, io};

pub const CLIENT_DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct TcpClient<RT: AsyncRuntime> {
    stream: UnsafeCell<AsyncBufStream<UnifyStream<RT>>>,
    resp_buf: UnsafeCell<Vec<u8>>,
    conn_id: String,
    read_timeout: Duration,
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
        unsafe { std::mem::transmute(self.stream.get()) }
    }

    #[inline(always)]
    fn get_resp_buf(&self, len: usize) -> &mut Vec<u8> {
        let buf: &mut Vec<u8> = unsafe { transmute(self.resp_buf.get()) };
        buf.resize(len as usize, 0);
        buf
    }

    async fn _recv_and_dump<F: ClientFacts>(&self, logger: &LogFilter, l: usize) -> io::Result<()> {
        let reader = self.get_stream_mut();
        // TODO is there dump ?
        match Buffer::alloc(l as i32) {
            Err(_) => {
                logger_warn!(logger, "{:?} alloc buf failed", self);
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            Ok(mut buf) => {
                if let Err(e) =
                    crate::io_with_timeout!(RT, self.read_timeout, reader.read_exact(&mut buf))
                {
                    logger_warn!(logger, "{:?} recv task failed: {}", self, e);
                    return Err(e);
                }
                return Ok(());
            }
        }
    }

    #[inline]
    async fn _recv_error<F: ClientFacts>(
        &self, facts: &F, logger: &LogFilter, codec: &F::Codec, resp_head: &proto::RespHead,
        mut task: F::Task,
    ) -> io::Result<()> {
        log_debug_assert!(resp_head.flag > 0);
        let reader = self.get_stream_mut();
        match resp_head.flag {
            1 => {
                // In the transport layer, we don't have routing info for last_index/conf_ver
                task.set_custom_error(codec, EncodedErr::Num(resp_head.msg_len.get()), 0, 0);
                facts.error_handle(task);
                return Ok(());
            }
            2 => {
                let buf = self.get_resp_buf(resp_head.blob_len.get() as usize);
                match crate::io_with_timeout!(RT, self.read_timeout, reader.read_exact(buf)) {
                    Err(e) => {
                        logger_warn!(logger, "{:?} recv buffer error: {}", self, e);
                        task.set_rpc_error(RpcIntErr::IO);
                        facts.error_handle(task);
                        return Err(e);
                    }
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
                        task.set_custom_error(codec, EncodedErr::Buf(buf.clone()), 0, 0);
                        facts.error_handle(task);
                        return Ok(());
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    async fn _recv_resp_body<F: ClientFacts>(
        &self, facts: &F, logger: &LogFilter, codec: &F::Codec, task_reg: &mut ClientTaskTimer<F>,
        resp_head: &proto::RespHead,
    ) -> io::Result<()> {
        let reader = self.get_stream_mut();
        let read_timeout = self.read_timeout;
        let blob_len = resp_head.blob_len.get();
        let read_buf = self.get_resp_buf(resp_head.msg_len.get() as usize);
        if let Some(mut task_item) = task_reg.take_task(resp_head.seq.get()).await {
            let mut task = task_item.task.take().unwrap();
            if resp_head.flag > 0 {
                return self._recv_error(facts, logger, codec, resp_head, task).await;
            }
            if resp_head.msg_len > 0 {
                if let Err(e) =
                    crate::io_with_timeout!(RT, read_timeout, reader.read_exact(read_buf))
                {
                    task.set_rpc_error(RpcIntErr::IO);
                    facts.error_handle(task);
                    return Err(e);
                }
            } // When msg_len == 0, read_buf has 0 size

            if blob_len > 0 {
                match task.reserve_resp_blob(blob_len) {
                    None => {
                        logger_error!(
                            logger,
                            "{:?} rpc client task {:?} has no ext_buf",
                            self,
                            task,
                        );
                        task.set_rpc_error(RpcIntErr::Decode);
                        facts.error_handle(task);
                        return self._recv_and_dump::<F>(logger, blob_len as usize).await;
                    }
                    Some(buf) => {
                        // ensure buf can fit blob_len
                        if let Err(e) =
                            crate::io_with_timeout!(RT, read_timeout, reader.read_exact(buf))
                        {
                            logger_warn!(
                                logger,
                                "{:?} rpc client reader read ext_buf err: {}",
                                self,
                                e
                            );
                            task.set_rpc_error(RpcIntErr::IO);
                            facts.error_handle(task);
                            return Err(e);
                        }
                    }
                }
            }
            logger_trace!(logger, "{:?} recv task {:?} ok", self, task);
            if resp_head.msg_len > 0 {
                // set result of task, and notify task completed
                if let Err(_) = task.decode_resp(codec, read_buf) {
                    logger_warn!(logger, "{:?} rpc client reader decode resp err", self,);
                    task.set_rpc_error(RpcIntErr::Decode);
                    facts.error_handle(task);
                    return Ok(());
                } else {
                    task.set_ok();
                }
            } else {
                task.set_ok();
            }
            task.done();
            return Ok(());
        } else {
            let seq = resp_head.seq;
            logger_trace!(logger, "{:?} timer take_task(seq={}) return None", self, seq);
            let mut data_len = 0;
            if resp_head.flag == 0 {
                data_len += resp_head.msg_len.get() + resp_head.blob_len.get() as u32;
            } else if resp_head.flag == proto::RESP_FLAG_HAS_ERR_STRING {
                data_len += resp_head.blob_len.get() as u32;
            }
            if data_len > 0 {
                return self._recv_and_dump::<F>(logger, data_len as usize).await;
            } else {
                return Ok(());
            }
        }
    }
}

impl<RT: AsyncRuntime + Clone> ClientTransport for TcpClient<RT> {
    type RT = RT;

    async fn connect(addr: &str, conn_id: &str, config: &ClientConfig) -> Result<Self, RpcIntErr> {
        let stream: UnifyStream<RT> =
            match UnifyStream::<RT>::connect_timeout(addr, config.connect_timeout).await {
                Ok(_stream) => _stream,
                Err(e) => {
                    warn!("Cannot connect addr {}: {}", addr, e);
                    return Err(RpcIntErr::Unreachable.into());
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
            write_timeout: config.write_timeout,
            read_timeout: config.read_timeout,
        })
    }

    #[inline(always)]
    async fn close_conn<F: ClientFacts>(&self, logger: &LogFilter) {
        if self.flush_req::<F>(logger).await.is_ok() {
            // stream close is just shutdown on sending, receiver might not be notified on peer dead
            let stream = self.get_stream_mut();
            let _ = stream.get_inner().shutdown_write().await;
        }
    }

    #[inline(always)]
    async fn flush_req<F: ClientFacts>(&self, logger: &LogFilter) -> io::Result<()> {
        let writer = self.get_stream_mut();
        if let Err(e) = crate::io_with_timeout!(RT, self.write_timeout, writer.flush()) {
            logger_warn!(logger, "{:?} flush_req flush err: {}", self, e);
            return Err(e);
        }
        logger_trace!(logger, "{:?}: flush_req ok", self);
        Ok(())
    }

    #[inline(always)]
    async fn write_req<'a, F: ClientFacts>(
        &'a self, logger: &LogFilter, buf: &'a [u8], blob: Option<&'a [u8]>, need_flush: bool,
    ) -> io::Result<()> {
        let writer = self.get_stream_mut();
        let write_timeout = self.write_timeout;

        macro_rules! err_log {
            ($r: expr) => {{
                if let Err(e) = $r {
                    logger_warn!(logger, "{:?} write_req err: {}", self, e);
                    return Err(e);
                }
            }};
        }
        crate::io_with_timeout!(RT, write_timeout, writer.write_all(buf))?;
        if let Some(blob_buf) = blob {
            err_log!(crate::io_with_timeout!(RT, write_timeout, writer.write_all(blob_buf)));
        }
        if need_flush {
            self.flush_req::<F>(logger).await?;
        }
        return Ok(());
    }

    /// return false to indicate aborted by close_f
    #[inline]
    async fn read_resp<F: ClientFacts>(
        &self, facts: &F, logger: &LogFilter, codec: &F::Codec,
        close_ch: Option<&mut AsyncRx<mpsc::Null>>, task_reg: &mut ClientTaskTimer<F>,
    ) -> Result<bool, RpcIntErr> {
        let mut resp_head_buf = [0u8; proto::RPC_RESP_HEADER_LEN];
        let reader = self.get_stream_mut();
        if let Some(close_ch) = close_ch {
            let read_header_f = reader.read_exact(&mut resp_head_buf);
            let close_f = close_ch.recv();
            let res = Cancellable::new(read_header_f, close_f).await;
            match res {
                Ok(r) => {
                    if let Err(e) = r {
                        logger_debug!(logger, "{:?} rpc client read resp head err: {:?}", self, e);
                        return Err(e.into());
                    }
                }
                Err(_) => {
                    return Ok(false);
                }
            }
        } else {
            if let Err(e) = crate::io_with_timeout!(
                RT,
                self.read_timeout,
                reader.read_exact(&mut resp_head_buf)
            ) {
                logger_debug!(logger, "{:?} rpc client read resp head err: {}", self, e);
                return Err(e.into());
            }
        }
        match proto::RespHead::decode_head(&resp_head_buf) {
            Err(e) => {
                logger_debug!(logger, "{:?} rpc client decode_response_header err: {}", self, e);
                return Err(e);
            }
            Ok(head) => {
                logger_trace!(logger, "{:?} rpc client read head response {}", self, &head);
                if let Err(e) = self._recv_resp_body(facts, logger, codec, task_reg, &head).await {
                    return Err(e.into());
                }
                return Ok(true);
            }
        }
    }
}
