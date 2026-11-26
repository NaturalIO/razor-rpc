//! [ClientStream] represents a client-side connection.
//!
//! On Drop, will close the connection on write-side, the response reader coroutine not exit
//! until all the ClientTask have a response or after `task_timeout` is reached.
//!
//! The user sends packets in sequence, with a throttler controlling the IO depth of in-flight packets.
//! An internal timer then registers the request through a channel, and when the response
//! is received, it can optionally notify the user through a user-defined channel or another mechanism.

use super::{
    registry::{TaskRegistry, TaskRegistryTx},
    task::ClientTaskDone,
    throttler::Throttler,
    *,
};
use crate::proto;
use futures_util::pin_mut;
use orb::prelude::*;
use std::time::Duration;
use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    sync::{
        Arc,
        atomic::{
            AtomicBool, AtomicU32, AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
        },
    },
    task::{Context, Poll},
};

pub const MAX_THRESHOLD: u32 = 1 << 16;

/// ClientStream represents a client-side connection.
///
/// On Drop, the connection will be closed on the write-side. The response reader coroutine will not exit
/// until all the ClientTasks have a response or after `task_timeout` is reached.
///
/// The user sends packets in sequence, with a throttler controlling the IO depth of in-flight packets.
/// An internal timer then registers the request through a channel, and when the response
/// is received, it can optionally notify the user through a user-defined channel or another mechanism.
#[derive(DebugAttr)]
#[fmt("{inner:?}")]
pub struct ClientStream<F: ClientFacts, P: ClientTransport> {
    tx: TaskRegistryTx<F>,
    inner: Arc<ClientStreamInner<F, P>>,
    encode_buf: Vec<u8>,
    seq_gen: u32,
}

impl<F: ClientFacts, P: ClientTransport> ClientStream<F, P> {
    /// Make a streaming connection to the server, returns [ClientStream] on success
    #[inline]
    pub async fn connect(
        facts: F, rt: Option<&<P::RT as AsyncRuntime>::Exec>, addr: &str, conn_id: &str,
        last_resp_ts: Option<Arc<AtomicU64>>,
    ) -> Result<Self, RpcIntErr> {
        let client_id = facts.get_client_id();
        let config = facts.get_config();
        let conn = P::connect(addr, conn_id, config).await?;
        let (tx, reg) = TaskRegistry::new(config.task_timeout);
        let inner = Arc::new(ClientStreamInner::new(facts, conn, client_id, last_resp_ts));
        logger_debug!(inner.logger, "{:?} connected", inner);
        let this = Self { inner, encode_buf: Vec::with_capacity(1024), tx, seq_gen: 0 };
        let worker = ClientRecvWorker::new(this.inner.clone(), reg);
        let f = worker.run();
        if let Some(_rt) = rt {
            _rt.spawn_detach(f);
        } else {
            P::RT::spawn_detach(f);
        }
        Ok(this)
    }

    #[inline]
    pub fn get_codec(&self) -> &F::Codec {
        &self.inner.codec
    }

    /// Since the transport layer might have buffer, user should always call flush explicitly.
    /// you can set `need_flush` = true for some urgent message, or call flush_req() explicitly.
    #[inline(always)]
    pub async fn flush_req(&mut self) -> Result<(), RpcIntErr> {
        if let Err(e) = self.inner.conn.flush_req::<F>(&self.inner.logger).await {
            logger_warn!(self.inner.logger, "{:?} flush_req flush err: {}", self, e);
            self.inner.has_err.store(true, Release);
            return Err(RpcIntErr::IO);
        }
        Ok(())
    }

    #[inline]
    async fn generate_seq(&mut self) -> u32 {
        let seq = self.seq_gen.wrapping_add(1);
        self.seq_gen = seq;
        // XXX If task_timeout = 60, we need to send packets at the speed of ((1<<32) / 60) per second,
        // to reach the limit, which is almost impossible since we have throttler.
        loop {
            let min_seq = self.inner.min_seq.load(Acquire);
            if seq != min_seq {
                return seq;
            } else {
                log::warn!("razor-rpc: seq wrapping detected, wait for timeout");
                <P::RT as AsyncTime>::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    /// send_task() should only be called without parallelism.
    ///
    /// NOTE: After send, will wait for response if too many inflight task in throttler.
    ///
    /// Since the transport layer might have buffer, user should always call flush explicitly.
    /// You can set `need_flush` = true for some urgent messages, or call flush_req() explicitly.
    #[inline(always)]
    pub async fn send_task(
        &mut self, mut task: F::Task, mut need_flush: bool,
    ) -> Result<(), RpcIntErr> {
        let e: RpcIntErr = if !self.inner.has_err.load(Acquire) {
            // TODO merge two calls to throttler
            self.inner.throttler.throttle().await;
            if self.inner.throttler.nearly_full() {
                need_flush = true;
            }
            let seq = self.generate_seq().await;
            task.as_mut().set_seq(seq);
            let logger = &self.inner.logger;
            let buf = &mut self.encode_buf;
            match proto::ReqHead::encode(&self.inner.codec, buf, self.inner.client_id, &mut task) {
                Ok(blob_buf) => {
                    match self.inner.conn.write_req::<F>(&logger, buf, blob_buf, need_flush).await {
                        Ok(()) => {
                            let wg = self.inner.throttler.add_task();
                            logger_trace!(logger, "{self:?} send task {task:?} ok");
                            self.tx.reg_task(task, wg).await;
                            return Ok(());
                        }
                        Err(e) => {
                            logger_warn!(
                                logger,
                                "{:?} send_req write req {:?} err: {:?}",
                                self,
                                task,
                                e
                            );
                            self.inner.has_err.store(true, Release);
                            logger_warn!(logger, "{self:?} sending task {task:?} err: {e}");
                            e
                        }
                    }
                }
                Err(()) => {
                    logger_warn!(logger, "{self:?} send_req encode req {task:?} err");
                    // XXX has_err is duplicated ?
                    self.inner.has_err.store(true, Release);
                    RpcIntErr::Encode
                }
            }
        } else {
            logger_warn!(self.inner.logger, "{self:?} send_req {task:?} already closed");
            RpcIntErr::IO
        };
        task.set_rpc_error(e);
        self.inner.facts.error_handle(task);
        Err(e)
    }

    /// NOTE: will skip if throttler is full
    #[inline(always)]
    pub async fn ping(&mut self) -> Result<(), RpcIntErr> {
        if !self.inner.has_err.load(Acquire) {
            let seq = self.generate_seq().await;
            // PING does not counted in throttler
            let buf = &mut self.encode_buf;
            proto::ReqHead::encode_ping(buf, self.inner.client_id, seq);
            // Ping does not have a ClientTask, in order
            // alive. Connection Prober can monitor the liveness of ClientConn
            match self.inner.conn.write_req::<F>(&self.inner.logger, buf, None, true).await {
                Ok(()) => {
                    self.tx.send_ping(seq).await;
                    return Ok(());
                }
                Err(e) => {
                    logger_warn!(self.inner.logger, "{:?} send ping err: {:?}", self, e);
                    return Err(RpcIntErr::IO);
                }
            }
        } else {
            logger_warn!(self.inner.logger, "{:?} send_ping_req skip as conn closed", self);
            return Err(RpcIntErr::IO);
        }
    }

    #[inline(always)]
    pub fn get_last_resp_ts(&self) -> u64 {
        if let Some(ts) = self.inner.last_resp_ts.as_ref() { ts.load(Relaxed) } else { 0 }
    }

    /// Force the receiver to exit.
    ///
    /// You can call it when connectivity probes detect that a server is unreachable.
    /// And then just let the Client drop
    pub async fn set_error_and_exit(&mut self) {
        // TODO review usage when doing ConnProbe
        self.inner.has_err.store(true, Release);
        self.inner.conn.close_conn::<F>(&self.inner.logger).await;
    }

    /// Check the throttler and see if future send_task() might be blocked
    #[inline]
    pub fn will_block(&self) -> bool {
        self.inner.throttler.nearly_full()
    }

    /// Get the task sent but not yet received response
    #[inline]
    pub fn get_inflight_count(&self) -> usize {
        // TODO confirm ping task counted ?
        self.inner.throttler.get_inflight_count()
    }
}

#[derive(DebugAttr)]
#[fmt("{conn:?}")]
pub(super) struct ClientStreamInner<F: ClientFacts, P: ClientTransport> {
    client_id: u32,
    conn: P,
    has_err: AtomicBool,
    throttler: Throttler,
    last_resp_ts: Option<Arc<AtomicU64>>,
    codec: F::Codec,
    logger: F::Logger,
    pub(crate) facts: F,
    pub(crate) min_seq: AtomicU32,
}

// because we use Arc
unsafe impl<F: ClientFacts, P: ClientTransport> Send for ClientStreamInner<F, P> {}
unsafe impl<F: ClientFacts, P: ClientTransport> Sync for ClientStreamInner<F, P> {}

impl<F: ClientFacts, P: ClientTransport> ClientStreamInner<F, P> {
    pub fn new(facts: F, conn: P, client_id: u32, last_resp_ts: Option<Arc<AtomicU64>>) -> Self {
        let config = facts.get_config();
        let mut thresholds = config.thresholds as u32;
        if thresholds == 0 {
            thresholds = 128;
        } else if thresholds > MAX_THRESHOLD {
            thresholds = MAX_THRESHOLD;
        }
        let client_inner = Self {
            client_id,
            conn,
            throttler: Throttler::new(thresholds as usize),
            last_resp_ts,
            has_err: AtomicBool::new(false),
            codec: F::Codec::default(),
            logger: facts.new_logger(),
            min_seq: AtomicU32::new(0),
            facts,
        };
        logger_trace!(client_inner.logger, "{:?} throttler is set to {}", client_inner, thresholds,);
        client_inner
    }
}

#[derive(DebugAttr)]
#[fmt("{inner:?}")]
struct ClientRecvWorker<F: ClientFacts, P: ClientTransport> {
    inner: Arc<ClientStreamInner<F, P>>,
    reg: TaskRegistry<F>,
    tick: <P::RT as AsyncTime>::Interval,
    read_timeout: u16,
}

impl<F: ClientFacts, P: ClientTransport> ClientRecvWorker<F, P> {
    #[inline]
    fn new(inner: Arc<ClientStreamInner<F, P>>, reg: TaskRegistry<F>) -> Self {
        let config = inner.facts.get_config();
        let read_timeout = config.read_timeout;
        Self {
            inner,
            reg,
            tick: <P::RT as AsyncTime>::interval(Duration::from_secs(1)),
            read_timeout,
        }
    }

    #[inline]
    async fn recv_one_resp(&mut self) -> bool {
        let mut _head: MaybeUninit<proto::RespHead> = MaybeUninit::uninit();
        let (reg, inner) = (&mut self.reg, &self.inner);
        {
            let f = inner.conn.read_resp_head::<F>(&inner.logger, &mut _head);
            pin_mut!(f);
            let r = ReadRespHeader {
                recv_fut: Pin::new(&mut f),
                tick: Pin::new(&mut self.tick),
                reg,
                inner,
            }
            .await;
            match r {
                Ok(true) => {}
                Ok(false) => return false,
                Err(_e) => return false,
            }
        }
        let head = unsafe { _head.assume_init_ref() };
        let o_task = reg.take_task(head.seq.get()).await;
        if head.msg_len > 0 || head.blob_len > 0 {
            let f =
                inner.conn.read_resp_body(&inner.facts, &inner.logger, &inner.codec, head, o_task);
            pin_mut!(f);
            // TODO:
            // we are currently aiming for LAN, there won't be much issue to set timeout on the
            // whole task. Should do more adaptive timeout for the WAN in the future.
            let r = ReadRespTimeout {
                recv_fut: Pin::new(&mut f),
                tick: Pin::new(&mut self.tick),
                reg,
                inner,
                // add one second because ticker is not very acurate
                timeout: self.read_timeout as usize + 1,
            }
            .await;
            match r {
                Ok(()) => return true,
                Err(_e) => return false,
            }
        } else if let Some(mut task) = o_task {
            // XXX we cannot tell whether its ping msg, but it's possible other msg have empty
            // respond
            task.task.set_ok();
            task.task.done();
        }
        true
    }

    async fn run(mut self) {
        while self.recv_one_resp().await {}
        // if the connection is close without any task, this is a Noop;
        // otherwise the rest task should set to error on connection break;
        self.reg.clean_pending_tasks(&self.inner.facts).await;
    }
}

struct ReadRespHeader<'a, FR, F, P, I>
where
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
{
    recv_fut: Pin<&'a mut FR>,
    reg: &'a mut TaskRegistry<F>,
    inner: &'a ClientStreamInner<F, P>,
    tick: Pin<&'a mut I>,
}

/// # Return value:
/// - Ok(true): a header has been received
/// - Ok(false): no task in the registry, and sender has been drop
impl<'a, FR, F, P, I> Future for ReadRespHeader<'a, FR, F, P, I>
where
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
{
    type Output = Result<bool, RpcIntErr>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        // Even if receive future is done, we should poll ticker in order to detect timeout event
        if this.tick.as_mut().poll_tick(ctx).is_pending() {
        } else {
            logger_trace!(
                this.inner.logger,
                "{:?} has {} pending_tasks",
                this.inner,
                this.inner.throttler.get_inflight_count()
            );
            this.reg.adjust_task_queue(&this.inner);
        }
        if let Poll::Ready(res) = this.recv_fut.as_mut().poll(ctx) {
            return Poll::Ready(res.map(|_| true));
        }
        loop {
            match this.reg.poll_sent_task(ctx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(())) => {
                    if this.reg.stash_count == 0 {
                        // there's nothing sent and the sender has drop, we should break;
                        return Poll::Ready(Ok(false));
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

struct ReadRespTimeout<'a, FR, F, P, I>
where
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
{
    recv_fut: Pin<&'a mut FR>,
    reg: &'a mut TaskRegistry<F>,
    tick: Pin<&'a mut I>,
    timeout: usize,
    inner: &'a ClientStreamInner<F, P>,
}

impl<'a, FR, F, P, I> Future for ReadRespTimeout<'a, FR, F, P, I>
where
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
{
    type Output = Result<(), RpcIntErr>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        // Even if receive future is done, we should poll ticker in order to detect timeout event
        if this.tick.as_mut().poll_tick(ctx).is_pending() {
        } else {
            logger_trace!(
                this.inner.logger,
                "{:?} has {} pending_tasks",
                this.inner,
                this.inner.throttler.get_inflight_count()
            );
            this.reg.adjust_task_queue(&this.inner);
            if this.timeout > 0 {
                this.timeout -= 1;
            } else {
                return Poll::Ready(Err(RpcIntErr::Timeout));
            }
        }
        if let Poll::Ready(r) = this.recv_fut.as_mut().poll(ctx) {
            return Poll::Ready(r);
        }
        while let Poll::Ready(Ok(())) = this.reg.poll_sent_task(ctx) {}
        return Poll::Pending;
    }
}
