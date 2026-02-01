use crate::proto::RpcAction;
use crate::server::*;
use captains_log::filter::LogFilter;
use crossfire::{AsyncRx, MAsyncRx, mpmc, mpsc, null::CloseHandle};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// An RpcServer that listen, accept, and server connections, according to ServerFacts interface.
pub struct RpcServer<F>
where
    F: ServerFacts,
{
    // join handles
    listeners_abort: Vec<(<F as AsyncExec>::AsyncHandle<()>, String)>,
    logger: Arc<LogFilter>,
    facts: Arc<F>,
    conn_ref_count: Arc<()>,
    // because we have multiple listeners
    server_close_tx: Mutex<Option<CloseHandle<mpmc::Null>>>,
    // because we have multiple listeners
    server_close_rx: MAsyncRx<mpmc::Null>,
}

impl<F> RpcServer<F>
where
    F: ServerFacts,
{
    pub fn new(facts: Arc<F>) -> Self {
        let (tx, rx) = crossfire::mpmc::new::<mpmc::Null, _, _>();
        Self {
            listeners_abort: Vec::new(),
            logger: facts.new_logger(),
            facts,
            conn_ref_count: Arc::new(()),
            server_close_tx: Mutex::new(Some(tx)),
            server_close_rx: rx,
        }
    }

    pub async fn listen<T: ServerTransport, D: Dispatch>(
        &mut self, addr: &str, dispatch: D,
    ) -> io::Result<String> {
        match T::bind(addr).await {
            Err(e) => {
                error!("bind addr {:?} err: {}", addr, e);
                return Err(e);
            }
            Ok(mut listener) => {
                let local_addr = match listener.local_addr() {
                    Ok(addr) => addr,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::AddrNotAvailable {
                            // For Unix sockets, return a dummy address
                            "0.0.0.0:0".parse().unwrap()
                        } else {
                            return Err(e);
                        }
                    }
                };
                let facts = self.facts.clone();
                let conn_ref_count = self.conn_ref_count.clone();
                let listener_info = format!("listener {:?}", addr);
                let server_close_rx = self.server_close_rx.clone();
                debug!("listening on {:?}", listener);
                let handle = self.facts.spawn(async move {
                    loop {
                        match listener.accept().await {
                            Err(e) => {
                                warn!("{:?} accept error: {}", listener, e);
                                return;
                            }
                            Ok(stream) => {
                                let conn =
                                    T::new_conn(stream, facts.get_config(), conn_ref_count.clone());
                                Self::server_conn::<T, D>(
                                    conn,
                                    &facts,
                                    dispatch.clone(),
                                    server_close_rx.clone(),
                                )
                            }
                        }
                    }
                });
                self.listeners_abort.push((handle, listener_info));
                return Ok(local_addr);
            }
        }
    }

    fn server_conn<T: ServerTransport, D: Dispatch>(
        conn: T, facts: &F, dispatch: D, server_close_rx: MAsyncRx<mpmc::Null>,
    ) {
        let conn = Arc::new(conn);

        let (done_tx, done_rx) = mpsc::unbounded_async();
        let codec = Arc::new(D::Codec::default());

        let noti = RespNoti(done_tx);
        struct Reader<T: ServerTransport, D: Dispatch> {
            noti: RespNoti<D::RespTask>,
            conn: Arc<T>,
            server_close_rx: MAsyncRx<mpmc::Null>,
            codec: Arc<D::Codec>,
            dispatch: D,
            logger: Arc<LogFilter>,
        }
        let reader = Reader::<T, D> {
            noti,
            codec: codec.clone(),
            dispatch,
            conn: conn.clone(),
            server_close_rx,
            logger: facts.new_logger(),
        };
        facts.spawn_detach(async move { reader.run().await });

        impl<T: ServerTransport, D: Dispatch> Reader<T, D> {
            async fn run(self) -> Result<(), ()> {
                loop {
                    match self.conn.read_req(&self.logger, &self.server_close_rx).await {
                        Ok(req) => {
                            if req.action == RpcAction::Num(0) && req.msg.len() == 0 {
                                // ping request
                                self.send_quick_resp(req.seq, None)?;
                            } else {
                                let seq = req.seq;
                                if self
                                    .dispatch
                                    .dispatch_req(&self.codec, req, self.noti.clone())
                                    .await
                                    .is_err()
                                {
                                    self.send_quick_resp(seq, Some(RpcIntErr::Decode.into()))?;
                                }
                            }
                        }
                        Err(_e) => {
                            // XXX read_req return error not used
                            return Err(());
                        }
                    }
                }
            }

            #[inline]
            fn send_quick_resp(&self, seq: u64, err: Option<RpcIntErr>) -> Result<(), ()> {
                if self.noti.send_err(seq, err).is_err() {
                    logger_warn!(self.logger, "{:?} reader abort due to writer has err", self.conn);
                    return Err(());
                }
                Ok(())
            }
        }

        struct Writer<T: ServerTransport, D: Dispatch> {
            codec: Arc<D::Codec>,
            done_rx: AsyncRx<mpsc::List<Result<D::RespTask, (u64, Option<RpcIntErr>)>>>,
            conn: Arc<T>,
            logger: Arc<LogFilter>,
        }
        let writer = Writer::<T, D> { done_rx, codec, conn, logger: facts.new_logger() };
        facts.spawn_detach(async move { writer.run().await });

        impl<T: ServerTransport, D: Dispatch> Writer<T, D> {
            async fn run(self) -> Result<(), io::Error> {
                macro_rules! process {
                    ($task: expr) => {{
                        match $task {
                            Ok(_task) => {
                                logger_trace!(self.logger, "write_resp {:?}", _task);
                                self.conn
                                    .write_resp::<D::RespTask>(
                                        &self.logger,
                                        self.codec.as_ref(),
                                        _task,
                                    )
                                    .await?;
                            }
                            Err((seq, err)) => {
                                self.conn.write_resp_internal(&self.logger, seq, err).await?;
                            }
                        }
                    }};
                }
                while let Ok(task) = self.done_rx.recv().await {
                    process!(task);
                    while let Ok(task) = self.done_rx.try_recv() {
                        process!(task);
                    }
                    self.conn.flush_resp(&self.logger).await?;
                }
                logger_trace!(self.logger, "{:?} writer exits", self.conn);
                self.conn.close_conn(&self.logger).await;
                Ok(())
            }
        }
    }

    #[inline]
    fn get_alive_conn(&self) -> usize {
        Arc::strong_count(&self.conn_ref_count) - 1
    }

    /// Gracefully close the server
    ///
    /// Steps:
    /// - listeners coroutine is abort
    /// - drop the close channel to notify connection read coroutines.
    /// - the writer coroutines will exit after all the reference of RespNoti channel drop to 0
    /// - wait for connection coroutines to exit with a timeout defined by
    /// ServerConfig.server_close_wait
    pub async fn close(&mut self) {
        // close listeners
        for h in self.listeners_abort.drain(0..) {
            h.0.abort();
            logger_info!(self.logger, "{} has closed", h.1);
        }
        // Notify all reader connection exit, then the reader will notify writer
        let _ = self.server_close_tx.lock().unwrap().take();

        let mut exists_count = self.get_alive_conn();
        // wait client close all connections
        let start_ts = Instant::now();
        let config = self.facts.get_config();
        while exists_count > 0 {
            F::sleep(Duration::from_secs(1)).await;
            exists_count = self.get_alive_conn();
            if Instant::now().duration_since(start_ts) > config.server_close_wait {
                logger_warn!(
                    self.logger,
                    "closed as wait too long for all conn closed voluntarily({} conn left)",
                    exists_count,
                );
                break;
            }
        }
        logger_info!(self.logger, "server closed with alive conn {}", exists_count);
    }
}
