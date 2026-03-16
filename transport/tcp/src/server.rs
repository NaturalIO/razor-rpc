use captains_log::filter::LogFilter;
use crossfire::{MAsyncRx, mpmc};
use io_buffer::Buffer;
use orb::io::AsyncBufStream;
use orb::net::{UnifyListener, UnifyStream};
use orb::prelude::*;
use orb::utils::Cancellable;
use razor_stream::server::{RpcSvrReq, ServerConfig, ServerTransport, task::ServerTaskEncode};
use razor_stream::{Codec, error::*};
use razor_stream::{proto, proto::RpcAction};
use std::cell::UnsafeCell;
use std::mem::transmute;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

pub const SERVER_DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub struct TcpServer<RT: AsyncRuntime> {
    stream: UnsafeCell<AsyncBufStream<UnifyStream<RT>>>,
    _conn_count: Arc<()>,
    config: ServerConfig,
    /// for read
    action_buf: UnsafeCell<Vec<u8>>,
    /// for read
    msg_buf: UnsafeCell<Vec<u8>>,
    /// for write
    encode_buf: UnsafeCell<Vec<u8>>,
}

unsafe impl<RT: AsyncRuntime> Send for TcpServer<RT> {}

unsafe impl<RT: AsyncRuntime> Sync for TcpServer<RT> {}

impl<RT: AsyncRuntime> TcpServer<RT> {
    // Because async runtimes does not support splitting read and write to static handler,
    // we use unsafe to achieve such goal,
    #[inline(always)]
    fn get_stream_mut(&self) -> &mut AsyncBufStream<UnifyStream<RT>> {
        unsafe { transmute(self.stream.get()) }
    }

    #[inline(always)]
    fn get_msg_buf(&self) -> &mut Vec<u8> {
        unsafe { transmute(self.msg_buf.get()) }
    }

    #[inline(always)]
    fn get_action_buf(&self) -> &mut Vec<u8> {
        unsafe { transmute(self.action_buf.get()) }
    }

    #[inline(always)]
    fn get_encode_buf(&self) -> &mut Vec<u8> {
        unsafe { transmute(self.encode_buf.get()) }
    }
}

impl<RT: AsyncRuntime> fmt::Debug for TcpServer<RT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.get_stream_mut().fmt(f)
    }
}

impl<RT: AsyncRuntime + Clone> ServerTransport for TcpServer<RT> {
    type RT = RT;
    type Listener = UnifyListener<RT>;

    async fn bind(addr: &str) -> io::Result<Self::Listener> {
        Self::Listener::bind(addr).await
    }

    fn new_conn(stream: UnifyStream<RT>, config: &ServerConfig, conn_count: Arc<()>) -> Self {
        let mut buf_size = config.stream_buf_size;
        if buf_size == 0 {
            buf_size = SERVER_DEFAULT_BUF_SIZE;
        }
        Self {
            stream: UnsafeCell::new(AsyncBufStream::new(stream, buf_size)),
            config: config.clone(),
            action_buf: UnsafeCell::new(Vec::with_capacity(128)),
            msg_buf: UnsafeCell::new(Vec::with_capacity(512)),
            // TODO add const assert with RPC_RESP_HEADER_LEN
            encode_buf: UnsafeCell::new(Vec::with_capacity(512)),
            _conn_count: conn_count,
        }
    }

    /// recv_req and return a temporary structure.
    ///
    /// NOTE: you should consume the buffer ref before recv another request.
    async fn read_req<'a>(
        &'a self, logger: &LogFilter, close_ch: &MAsyncRx<mpmc::Null>,
    ) -> Result<RpcSvrReq<'a>, RpcIntErr> {
        let reader = self.get_stream_mut();
        let read_timeout = self.config.read_timeout;
        let idle_timeout = self.config.idle_timeout;
        let mut req_header_buf = [0u8; proto::RPC_REQ_HEADER_LEN];

        let cancel_f = close_ch.recv_with_timer(RT::sleep(idle_timeout));
        match Cancellable::new(reader.read_exact(&mut req_header_buf), cancel_f).await {
            Ok(Err(e)) => {
                logger_debug!(logger, "{:?}: recv_req: err {}", self, e);
                return Err(RpcIntErr::IO);
            }
            Err(()) => {
                logger_trace!(logger, "{:?}: read timeout", self);
                return Err(RpcIntErr::Timeout);
            }
            _ => {}
        }
        let rpc_head: &proto::ReqHead;
        match proto::ReqHead::decode_head(&req_header_buf) {
            Err(e) => {
                logger_warn!(logger, "{:?}: decode_head error, {}", self, e);
                return Err(RpcIntErr::Decode);
            }
            Ok(head) => {
                rpc_head = head;
            }
        }
        logger_trace!(logger, "{:?}: recv req: {}", self, rpc_head);
        // XXX: we do return ping
        let action = match rpc_head.get_action() {
            Ok(num) => RpcAction::Num(num),
            Err(action_len) => {
                let action_buf = self.get_action_buf();
                action_buf.resize(action_len as usize, 0);
                match crate::io_with_timeout!(RT, read_timeout, reader.read_exact(action_buf)) {
                    Err(e) => {
                        logger_trace!(logger, "{:?}: read_exact error {}", self, e);
                        return Err(RpcIntErr::IO);
                    }
                    Ok(_) => {
                        match std::str::from_utf8(action_buf) {
                            Ok(s) => RpcAction::Str(s),
                            Err(_) => {
                                error!("{:?}: read action string decode error", self);
                                return Err(RpcIntErr::Decode);
                                // XXX stop reading or consume junk data?
                            }
                        }
                    }
                }
            }
        };

        let msg_buf = self.get_msg_buf();
        msg_buf.resize(rpc_head.msg_len.get() as usize, 0);
        if rpc_head.msg_len > 0 {
            if let Err(e) = crate::io_with_timeout!(RT, read_timeout, reader.read_exact(msg_buf)) {
                logger_trace!(logger, "{:?}: read req msg error: {:?}", self, e);
                return Err(RpcIntErr::IO);
            }
        }
        let mut blob: Option<Buffer> = None;
        let blob_len = rpc_head.blob_len.get() as i32;
        if blob_len > 0 {
            match Buffer::alloc(blob_len) {
                Err(_) => return Err(RpcIntErr::Decode),
                Ok(mut ext_buf) => {
                    match crate::io_with_timeout!(RT, read_timeout, reader.read_exact(&mut ext_buf))
                    {
                        Err(e) => {
                            logger_trace!(logger, "{:?}: read_exact_buffer error: {}", self, e);
                            return Err(RpcIntErr::IO);
                        }
                        Ok(_) => {
                            blob = Some(ext_buf);
                        }
                    }
                }
            }
        }
        return Ok(RpcSvrReq::<'a> { seq: rpc_head.seq.get(), action, msg: msg_buf, blob });
    }

    #[inline]
    async fn write_resp<T: ServerTaskEncode>(
        &self, logger: &LogFilter, codec: &impl Codec, mut task: T,
    ) -> io::Result<()> {
        let writer = self.get_stream_mut();
        let write_timeout = self.config.write_timeout;
        let buf = self.get_encode_buf();
        let (seq, blob_buf) = proto::RespHead::encode(&logger, codec, buf, &mut task);
        if let Err(e) = crate::io_with_timeout!(RT, write_timeout, writer.write_all(buf)) {
            logger_warn!(logger, "{:?}: send_resp write resp seq={} msg err: {}", self, seq, e);
            return Err(e);
        }
        if let Some(blob) = blob_buf {
            if let Err(e) =
                crate::io_with_timeout!(RT, write_timeout, writer.write_all(blob.as_ref()))
            {
                logger_debug!(
                    logger,
                    "{:?}: send_resp write resp seq={} blob err: {}",
                    self,
                    seq,
                    e
                );
                return Err(e);
            }
        }
        logger_trace!(logger, "{:?}: send resp seq={}", self, seq);
        return Ok(());
    }

    #[inline(always)]
    async fn write_resp_internal(
        &self, logger: &LogFilter, seq: u64, err: Option<RpcIntErr>,
    ) -> io::Result<()> {
        let writer = self.get_stream_mut();
        let write_timeout = self.config.write_timeout;
        let buf = self.get_encode_buf();
        let seq = proto::RespHead::encode_internal(&logger, buf, seq, err);
        if let Err(e) = crate::io_with_timeout!(RT, write_timeout, writer.write_all(buf)) {
            logger_warn!(logger, "{:?}: send_resp write resp seq={} msg err: {}", self, seq, e);
            return Err(e);
        }
        logger_trace!(logger, "{:?}: send resp seq={}", self, seq);
        return Ok(());
    }

    #[inline(always)]
    async fn flush_resp(&self, logger: &LogFilter) -> io::Result<()> {
        let writer = self.get_stream_mut();
        if let Err(e) = crate::io_with_timeout!(RT, self.config.write_timeout, writer.flush()) {
            logger_warn!(logger, "{:?}: flush err: {}", self, e);
            return Err(e);
        }
        logger_trace!(logger, "{:?}: flush_resp ok", self);
        return Ok(());
    }

    #[inline]
    async fn close_conn(&self, logger: &LogFilter) {
        if self.flush_resp(logger).await.is_ok() {
            let writer = self.get_stream_mut();
            let _ = writer.get_inner().shutdown_write().await;
        }
    }
}
