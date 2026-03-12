//! [ClientStream] represents a client-side connection.
//!
//! On Drop, will close the connection on write-side, the response reader coroutine not exit
//! until all the ClientTask have a response or after `task_timeout` is reached.
//!
//! The user sends packets in sequence, with a throttler controlling the IO depth of in-flight packets.
//! An internal timer then registers the request through a channel, and when the response
//! is received, it can optionally notify the user through a user-defined channel or another mechanism.

use super::throttler::Throttler;
use crate::client::task::ClientTaskDone;
use crate::client::timer::ClientTaskTimer;
use crate::{client::*, proto};
use captains_log::filter::LogFilter;
use crossfire::null::CloseHandle;
use futures_util::pin_mut;
use orb::prelude::*;
use std::time::Duration;
use std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    mem::transmute,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

/// ClientStream represents a client-side connection.
///
/// On Drop, the connection will be closed on the write-side. The response reader coroutine will not exit
/// until all the ClientTasks have a response or after `task_timeout` is reached.
///
/// The user sends packets in sequence, with a throttler controlling the IO depth of in-flight packets.
/// An internal timer then registers the request through a channel, and when the response
/// is received, it can optionally notify the user through a user-defined channel or another mechanism.
pub struct ClientStream<F: ClientFacts, P: ClientTransport> {
    close_tx: Option<CloseHandle<mpsc::Null>>,
    inner: Arc<ClientStreamInner<F, P>>,
}

impl<F: ClientFacts, P: ClientTransport> ClientStream<F, P> {
    /// Make a streaming connection to the server, returns [ClientStream] on success
    #[inline]
    pub async fn connect(
        facts: Arc<F>, addr: &str, conn_id: &str, last_resp_ts: Option<Arc<AtomicU64>>,
    ) -> Result<Self, RpcIntErr> {
        let client_id = facts.get_client_id();
        let conn = P::connect(addr, conn_id, facts.get_config()).await?;
        Ok(Self::new(facts, conn, client_id, conn_id.to_string(), last_resp_ts))
    }

    #[inline]
    fn new(
        facts: Arc<F>, conn: P, client_id: u64, conn_id: String,
        last_resp_ts: Option<Arc<AtomicU64>>,
    ) -> Self {
        let (_close_tx, _close_rx) = mpsc::new::<mpsc::Null, _, _>();
        let inner = Arc::new(ClientStreamInner::new(
            facts,
            conn,
            client_id,
            conn_id,
            _close_rx,
            last_resp_ts,
        ));
        logger_debug!(inner.logger, "{:?} connected", inner);
        let _inner = inner.clone();
        inner.facts.spawn_detach(async move {
            _inner.receive_loop().await;
        });
        Self { close_tx: Some(_close_tx), inner }
    }

    #[inline]
    pub fn get_codec(&self) -> &F::Codec {
        &self.inner.codec
    }

    /// Should be call in sender threads
    ///
    /// NOTE: will skip if throttler is full
    #[inline(always)]
    pub async fn ping(&mut self) -> Result<(), RpcIntErr> {
        self.inner.send_ping_req().await
    }

    #[inline(always)]
    pub fn get_last_resp_ts(&self) -> u64 {
        if let Some(ts) = self.inner.last_resp_ts.as_ref() { ts.load(Ordering::Relaxed) } else { 0 }
    }

    /// Since sender and receiver are two threads, might be close on either side
    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Force the receiver to exit.
    ///
    /// You can call it when connectivity probes detect that a server is unreachable.
    /// And then just let the Client drop
    pub async fn set_error_and_exit(&mut self) {
        // TODO review usage when doing ConnProbe
        self.inner.has_err.store(true, Ordering::SeqCst);
        self.inner.conn.close_conn::<F>(&self.inner.logger).await;
    }

    /// send_task() should only be called without parallelism.
    ///
    /// NOTE: After send, will wait for response if too many inflight task in throttler.
    ///
    /// Since the transport layer might have buffer, user should always call flush explicitly.
    /// You can set `need_flush` = true for some urgent messages, or call flush_req() explicitly.
    ///
    #[inline(always)]
    pub async fn send_task(&mut self, task: F::Task, need_flush: bool) -> Result<(), RpcIntErr> {
        self.inner.send_task(task, need_flush).await
    }

    /// Since the transport layer might have buffer, user should always call flush explicitly.
    /// you can set `need_flush` = true for some urgent message, or call flush_req() explicitly.
    #[inline(always)]
    pub async fn flush_req(&mut self) -> Result<(), RpcIntErr> {
        self.inner.flush_req().await
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

impl<F: ClientFacts, P: ClientTransport> Drop for ClientStream<F, P> {
    fn drop(&mut self) {
        self.close_tx.take();
        let timer = self.inner.get_timer_mut();
        timer.stop_reg_task();
        self.inner.closed.store(true, Ordering::SeqCst);
    }
}

impl<F: ClientFacts, P: ClientTransport> fmt::Debug for ClientStream<F, P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

struct ClientStreamInner<F: ClientFacts, P: ClientTransport> {
    client_id: u64,
    conn: P,
    seq: AtomicU64,
    // NOTE: because close_rx is AsyncRx (lower cost to register waker), but does not have Sync, to solve the & does
    // not have Send problem, we convert this to mut ref to make borrow checker shutup
    close_rx: UnsafeCell<AsyncRx<mpsc::Null>>,
    closed: AtomicBool, // flag set by either sender or receive on there exit
    timer: UnsafeCell<ClientTaskTimer<F>>,
    // TODO can closed and has_err merge ?
    has_err: AtomicBool,
    throttler: Throttler,
    last_resp_ts: Option<Arc<AtomicU64>>,
    encode_buf: UnsafeCell<Vec<u8>>,
    codec: F::Codec,
    logger: Arc<LogFilter>,
    facts: Arc<F>,
}

unsafe impl<F: ClientFacts, P: ClientTransport> Send for ClientStreamInner<F, P> {}

unsafe impl<F: ClientFacts, P: ClientTransport> Sync for ClientStreamInner<F, P> {}

impl<F: ClientFacts, P: ClientTransport> fmt::Debug for ClientStreamInner<F, P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.conn.fmt(f)
    }
}

impl<F: ClientFacts, P: ClientTransport> ClientStreamInner<F, P> {
    pub fn new(
        facts: Arc<F>, conn: P, client_id: u64, conn_id: String, close_rx: AsyncRx<mpsc::Null>,
        last_resp_ts: Option<Arc<AtomicU64>>,
    ) -> Self {
        let config = facts.get_config();
        let mut thresholds = config.thresholds;
        if thresholds == 0 {
            thresholds = 128;
        }
        let client_inner = Self {
            client_id,
            conn,
            close_rx: UnsafeCell::new(close_rx),
            closed: AtomicBool::new(false),
            seq: AtomicU64::new(1),
            encode_buf: UnsafeCell::new(Vec::with_capacity(1024)),
            throttler: Throttler::new(thresholds),
            last_resp_ts,
            has_err: AtomicBool::new(false),
            codec: F::Codec::default(),
            logger: facts.new_logger(),
            timer: UnsafeCell::new(ClientTaskTimer::new(conn_id, config.task_timeout, thresholds)),
            facts,
        };
        logger_trace!(client_inner.logger, "{:?} throttler is set to {}", client_inner, thresholds,);
        client_inner
    }

    #[inline(always)]
    fn get_timer_mut(&self) -> &mut ClientTaskTimer<F> {
        unsafe { transmute(self.timer.get()) }
    }

    #[inline(always)]
    fn get_close_rx(&self) -> &mut AsyncRx<mpsc::Null> {
        unsafe { transmute(self.close_rx.get()) }
    }

    #[inline(always)]
    fn get_encoded_buf(&self) -> &mut Vec<u8> {
        unsafe { transmute(self.encode_buf.get()) }
    }

    /// Directly work on the socket steam, when failed
    async fn send_task(&self, mut task: F::Task, mut need_flush: bool) -> Result<(), RpcIntErr> {
        if self.throttler.nearly_full() {
            need_flush = true;
        }
        let timer = self.get_timer_mut();
        timer.pending_task_count_ref().fetch_add(1, Ordering::SeqCst);
        // It's possible receiver set close after pending_task_count increase, keep this until
        // review
        if self.closed.load(Ordering::Acquire) {
            logger_warn!(
                self.logger,
                "{:?} sending task {:?} failed: {}",
                self,
                task,
                RpcIntErr::IO,
            );
            task.set_rpc_error(RpcIntErr::IO);
            self.facts.error_handle(task);
            timer.pending_task_count_ref().fetch_sub(1, Ordering::SeqCst); // rollback
            return Err(RpcIntErr::IO);
        }
        match self.send_request(task, need_flush).await {
            Err(_) => {
                self.closed.store(true, Ordering::SeqCst);
                self.has_err.store(true, Ordering::SeqCst);
                return Err(RpcIntErr::IO);
            }
            Ok(_) => {
                // register task to norifier
                self.throttler.throttle().await;
                return Ok(());
            }
        }
    }

    #[inline(always)]
    async fn flush_req(&self) -> Result<(), RpcIntErr> {
        if let Err(e) = self.conn.flush_req::<F>(&self.logger).await {
            logger_warn!(self.logger, "{:?} flush_req flush err: {}", self, e);
            self.closed.store(true, Ordering::SeqCst);
            self.has_err.store(true, Ordering::SeqCst);
            let timer = self.get_timer_mut();
            timer.stop_reg_task();
            return Err(RpcIntErr::IO);
        }
        Ok(())
    }

    #[inline(always)]
    async fn send_request(&self, mut task: F::Task, need_flush: bool) -> Result<(), RpcIntErr> {
        let seq = self.seq_update();
        task.set_seq(seq);
        let buf = self.get_encoded_buf();
        match proto::ReqHead::encode(&self.codec, buf, self.client_id, &task) {
            Err(_) => {
                logger_warn!(&self.logger, "{:?} send_req encode req {:?} err", self, task);
                return Err(RpcIntErr::Encode);
            }
            Ok(blob_buf) => {
                if let Err(e) =
                    self.conn.write_req::<F>(&self.logger, buf, blob_buf, need_flush).await
                {
                    logger_warn!(
                        self.logger,
                        "{:?} send_req write req {:?} err: {:?}",
                        self,
                        task,
                        e
                    );
                    self.closed.store(true, Ordering::SeqCst);
                    self.has_err.store(true, Ordering::SeqCst);
                    let timer = self.get_timer_mut();
                    // TODO check stop_reg_task
                    // rollback counter
                    timer.pending_task_count_ref().fetch_sub(1, Ordering::SeqCst);
                    timer.stop_reg_task();
                    logger_warn!(self.logger, "{:?} sending task {:?} err: {}", self, task, e);
                    task.set_rpc_error(RpcIntErr::IO);
                    self.facts.error_handle(task);
                    return Err(RpcIntErr::IO);
                } else {
                    let wg = self.throttler.add_task();
                    let timer = self.get_timer_mut();
                    logger_trace!(self.logger, "{:?} send task {:?} ok", self, task);
                    timer.reg_task(task, wg).await;
                }
                return Ok(());
            }
        }
    }

    #[inline(always)]
    async fn send_ping_req(&self) -> Result<(), RpcIntErr> {
        if self.closed.load(Ordering::Acquire) {
            logger_warn!(self.logger, "{:?} send_ping_req skip as conn closed", self);
            return Err(RpcIntErr::IO);
        }
        // PING does not counted in throttler
        let buf = self.get_encoded_buf();
        proto::ReqHead::encode_ping(buf, self.client_id, self.seq_update());
        // Ping does not need to reg_task, and have no error_handle, just to keep the connection
        // alive. Connection Prober can monitor the liveness of ClientConn
        if let Err(e) = self.conn.write_req::<F>(&self.logger, buf, None, true).await {
            logger_warn!(self.logger, "{:?} send ping err: {:?}", self, e);
            self.closed.store(true, Ordering::SeqCst);
            return Err(RpcIntErr::IO);
        }
        Ok(())
    }

    // return Ok(false) when close_rx has close and nothing more pending resp to receive
    async fn recv_some(&self) -> Result<(), RpcIntErr> {
        for _ in 0i32..20 {
            // Underlayer rpc socket is buffered, might not yeal to runtime
            // return if recv_one_resp runs too long, allow timer to be fire at each second
            match self.recv_one_resp().await {
                Err(e) => {
                    return Err(e);
                }
                Ok(_) => {
                    if let Some(last_resp_ts) = self.last_resp_ts.as_ref() {
                        last_resp_ts.store(self.facts.get_timestamp(), Ordering::Release);
                    }
                }
            }
        }
        Ok(())
    }

    async fn recv_one_resp(&self) -> Result<(), RpcIntErr> {
        let timer = self.get_timer_mut();
        loop {
            if self.closed.load(Ordering::Acquire) {
                logger_trace!(self.logger, "{:?} read_resp from already close", self.conn);
                // ensure task receive on normal exit
                if timer.check_pending_tasks_empty() || self.has_err.load(Ordering::Relaxed) {
                    return Err(RpcIntErr::IO);
                }
                // When ClientStream(sender) dropped, receiver will be timer
                if let Err(_e) = self
                    .conn
                    .read_resp(self.facts.as_ref(), &self.logger, &self.codec, None, timer)
                    .await
                {
                    self.closed.store(true, Ordering::SeqCst);
                    return Err(RpcIntErr::IO);
                }
            } else {
                // Block here for new header without timeout
                // NOTE: because close_rx is AsyncRx, which does not have Sync, to solve the & does
                // not have Send problem, we convert this to mut ref to make borrow checker shutup
                match self
                    .conn
                    .read_resp(
                        self.facts.as_ref(),
                        &self.logger,
                        &self.codec,
                        Some(self.get_close_rx()),
                        timer,
                    )
                    .await
                {
                    Err(_e) => {
                        return Err(RpcIntErr::IO);
                    }
                    Ok(r) => {
                        // TODO FIXME
                        if !r {
                            self.closed.store(true, Ordering::SeqCst);
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn receive_loop(&self) {
        let mut tick = <F as AsyncTime>::interval(Duration::from_secs(1));
        loop {
            let f = self.recv_some();
            pin_mut!(f);
            let selector = ReceiverTimerFuture::new(self, &mut tick, &mut f);
            match selector.await {
                Ok(_) => {}
                Err(e) => {
                    logger_debug!(self.logger, "{:?} receive_loop error: {}", self, e);
                    self.closed.store(true, Ordering::SeqCst);
                    let timer = self.get_timer_mut();
                    timer.clean_pending_tasks(self.facts.as_ref());
                    // If pending_task_count > 0 means some tasks may still remain in the pending chan
                    while timer.pending_task_count_ref().load(Ordering::SeqCst) > 0 {
                        // After the 'closed' flag has taken effect,
                        // pending_task_count will not keep growing,
                        // so there is no need to sleep here.
                        timer.clean_pending_tasks(self.facts.as_ref());
                        <F as AsyncTime>::sleep(Duration::from_secs(1)).await;
                    }
                    return;
                }
            }
        }
    }

    // Adjust the waiting queue
    fn time_reach(&self) {
        logger_trace!(
            self.logger,
            "{:?} has {} pending_tasks",
            self,
            self.throttler.get_inflight_count()
        );
        let timer = self.get_timer_mut();
        timer.adjust_task_queue(self.facts.as_ref());
        return;
    }

    #[inline(always)]
    fn seq_update(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }
}

impl<F: ClientFacts, P: ClientTransport> Drop for ClientStreamInner<F, P> {
    fn drop(&mut self) {
        let timer = self.get_timer_mut();
        timer.clean_pending_tasks(self.facts.as_ref());
    }
}

struct ReceiverTimerFuture<'a, F, P, I, FR>
where
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
{
    client: &'a ClientStreamInner<F, P>,
    inv: Pin<&'a mut I>,
    recv_future: Pin<&'a mut FR>,
}

impl<'a, F, P, I, FR> ReceiverTimerFuture<'a, F, P, I, FR>
where
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
{
    fn new(client: &'a ClientStreamInner<F, P>, inv: &'a mut I, recv_future: &'a mut FR) -> Self {
        Self { inv: Pin::new(inv), client, recv_future: Pin::new(recv_future) }
    }
}

// Return Ok(true) to indicate Ok
// Return Ok(false) when client sender has close normally
// Err(e) when connection error
impl<'a, F, P, I, FR> Future for ReceiverTimerFuture<'a, F, P, I, FR>
where
    F: ClientFacts,
    P: ClientTransport,
    I: TimeInterval,
    FR: Future<Output = Result<(), RpcIntErr>> + Unpin,
{
    type Output = Result<(), RpcIntErr>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        // In case ticker not fire, and ensure ticker schedule after ready
        while _self.inv.as_mut().poll_tick(ctx).is_ready() {
            _self.client.time_reach();
        }
        if _self.client.has_err.load(Ordering::Relaxed) {
            // When sentinel detect peer unreachable, recv_some mighe blocked, at least inv will
            // wait us, just exit
            return Poll::Ready(Err(RpcIntErr::IO));
        }
        _self.client.get_timer_mut().poll_sent_task(ctx);
        // Even if receive future has block, we should poll_sent_task in order to detect timeout event
        if let Poll::Ready(r) = _self.recv_future.as_mut().poll(ctx) {
            return Poll::Ready(r);
        }
        return Poll::Pending;
    }
}
