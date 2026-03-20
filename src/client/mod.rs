pub mod task;
use captains_log::filter::LogFilter;
use crossfire::oneshot::oneshot;
pub use razor_rpc_macros::{endpoint_async, endpoint_client};
pub use razor_stream::client::ClientCaller;
pub use task::*;

use crate::Codec;
use crate::error::{RpcErrCodec, RpcError, RpcIntErr};
pub use razor_stream::client::{
    ClientCallerBlocking, ClientConfig, ClientFacts, ClientTransport, ConnPool, FailoverPool,
};
use std::fmt;
use std::future::Future;
use std::sync::Arc;

pub struct APIFact<C: Codec> {
    pub logger: Arc<LogFilter>,
    config: ClientConfig,
    _phan: std::marker::PhantomData<fn(&C)>,
}

pub type APIConnPool<C, P> = ConnPool<APIFact<C>, P>;
pub type APIFailoverPool<C, P> = FailoverPool<APIFact<C>, P>;

impl<C: Codec> APIFact<C> {
    pub fn new(config: ClientConfig) -> Arc<Self> {
        Arc::new(Self { logger: Arc::new(LogFilter::new()), config, _phan: Default::default() })
    }

    #[inline]
    pub fn set_log_level(&self, level: log::Level) {
        self.logger.set_level(level);
    }

    pub fn new_conn_pool<P: ClientTransport>(
        self: Arc<Self>, rt: Option<&<P::RT as orb::AsyncRuntime>::Exec>, addr: &str,
    ) -> APIConnPool<C, P> {
        ConnPool::<APIFact<C>, P>::new(self.clone(), rt, addr, 0)
    }

    pub fn new_failover<P: ClientTransport>(
        self: Arc<Self>, rt: Option<&<P::RT as orb::AsyncRuntime>::Exec>, addrs: Vec<String>,
        stateless: bool, retry_limit: usize,
    ) -> APIFailoverPool<C, P> {
        FailoverPool::<APIFact<C>, P>::new(self.clone(), rt, addrs, stateless, retry_limit, 0)
    }
}

impl<C: Codec> ClientFacts for APIFact<C> {
    type Codec = C;
    type Task = APIClientReq;

    #[inline]
    fn new_logger(&self) -> Arc<LogFilter> {
        self.logger.clone()
    }

    #[inline]
    fn get_config(&self) -> &ClientConfig {
        &self.config
    }
}

pub trait APIClientCaller: ClientCaller<Facts: ClientFacts<Task = APIClientReq>> {
    fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> impl Future<Output = Result<Resp, RpcError<E>>> + Send
    where
        Req: serde::Serialize + fmt::Debug + Send + Sync,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec;
}

impl<F, P> APIClientCaller for ConnPool<F, P>
where
    F: ClientFacts<Task = APIClientReq>,
    P: ClientTransport,
{
    async fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> Result<Resp, RpcError<E>>
    where
        Req: serde::Serialize + fmt::Debug + Send + Sync,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let (tx, rx) = oneshot::<APIClientReq>();
        let codec = <Self as ClientCaller>::get_codec(self);
        let mut task = APIClientReq::new(&codec, service_method, req);
        task.set_noti(tx);
        self.send_req(task).await;
        if let Ok(mut task) = rx.recv_async().await {
            return task.process_res(&codec);
        } else {
            return Err(RpcIntErr::Internal.into());
        }
    }
}

impl<F, P> APIClientCaller for FailoverPool<F, P>
where
    F: ClientFacts<Task = APIClientReq>,
    P: ClientTransport,
{
    async fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> Result<Resp, RpcError<E>>
    where
        Req: serde::Serialize + fmt::Debug + Send + Sync,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let codec = <Self as ClientCaller>::get_codec(self);
        let mut retry_count = 0;
        let max_retries = self.get_retry_limit();
        let mut task = APIClientReq::new(&codec, service_method, req);
        let (tx, mut rx) = oneshot::<APIClientReq>();
        task.set_noti(tx);
        self.send_req(task).await;
        loop {
            if let Ok(mut task) = rx.recv_async().await {
                let result = task.process_res::<_, Resp, E>(&codec);
                match result {
                    Ok(resp) => return Ok(resp),
                    Err(RpcError::Rpc(e)) => {
                        // RpcIntErr less than Method is retriable by FailoverPool internally
                        return Err(RpcError::Rpc(e));
                    }
                    Err(RpcError::User(e)) => {
                        retry_count += 1;
                        match e.should_failover() {
                            Ok(Some(redirect_addr)) => {
                                if retry_count < max_retries {
                                    // Retry to specific address
                                    let (tx, _rx) = oneshot::<APIClientReq>();
                                    task.set_noti(tx);
                                    rx = _rx;
                                    self.resubmit(
                                        task,
                                        Ok(redirect_addr.to_string()),
                                        retry_count,
                                        None,
                                    )
                                    .await;
                                    continue;
                                }
                            }
                            Ok(None) => {
                                if retry_count < max_retries {
                                    // Retry to next node
                                    let (tx, _rx) = oneshot::<APIClientReq>();
                                    task.set_noti(tx);
                                    rx = _rx;
                                    let last_index = task.last_index;
                                    self.resubmit(task, Err(last_index), retry_count, None).await;
                                    continue;
                                }
                            }
                            Err(()) => return Err(RpcError::User(e)),
                        }
                        return Err(RpcError::User(e));
                    }
                }
            } else {
                return Err(RpcIntErr::Internal.into());
            }
        }
    }
}
