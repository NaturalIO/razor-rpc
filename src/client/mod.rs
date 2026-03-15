pub mod task;
use captains_log::filter::LogFilter;
use crossfire::oneshot::oneshot;
use crossfire::*;
use orb::AsyncRuntime;
pub use razor_rpc_macros::{endpoint_async, endpoint_client};
pub use razor_stream::client::ClientCaller;
pub use task::*;

use crate::Codec;
use crate::error::{EncodedErr, RpcErrCodec, RpcError, RpcIntErr};
pub use razor_stream::client::{
    ClientCallerBlocking, ClientConfig, ClientFacts, ClientTransport, ConnPool, FailoverPool,
};
use std::fmt;
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

    pub fn new_conn_pool<P: ClientTransport, RT: AsyncRuntime + Clone>(
        self: Arc<Self>, rt: &RT, addr: &str,
    ) -> APIConnPool<C, P> {
        ConnPool::<APIFact<C>, P>::new::<RT>(self.clone(), rt, addr, 0)
    }

    pub fn new_failover<P: ClientTransport, RT: AsyncRuntime + Clone>(
        self: Arc<Self>, rt: &RT, addrs: Vec<String>, round_robin: bool, retry_limit: usize,
    ) -> APIFailoverPool<C, P> {
        FailoverPool::<APIFact<C>, P>::new::<RT>(
            self.clone(),
            rt,
            addrs,
            round_robin,
            retry_limit,
            0,
        )
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
    ) -> impl std::future::Future<Output = Result<Resp, RpcError<E>>> + Send
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
    fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> impl std::future::Future<Output = Result<Resp, RpcError<E>>> + Send
    where
        Req: serde::Serialize + fmt::Debug + Send + Sync,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        async move {
            let (tx, rx) = oneshot::<APIClientReq>();
            let codec = <Self as ClientCaller>::get_codec(self);
            self.send_req(make_req(&codec, service_method, req, tx)).await;
            process_res(&codec, rx.recv_async().await)
        }
    }
}

impl<F, P> APIClientCaller for FailoverPool<F, P>
where
    F: ClientFacts<Task = APIClientReq>,
    P: ClientTransport,
{
    fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> impl std::future::Future<Output = Result<Resp, RpcError<E>>> + Send
    where
        Req: serde::Serialize + fmt::Debug + Send + Sync,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        async move {
            let (tx, rx) = oneshot::<APIClientReq>();
            let codec = <Self as ClientCaller>::get_codec(self);
            self.send_req(make_req(&codec, service_method, req, tx)).await;
            process_res(&codec, rx.recv_async().await)
        }
    }
}

/*
 *
BlockingEndpoint trait is provided for user-defined blocking clients
 Users implement this trait on their client structs to get the call() method
pub trait BlockingEndpoint: ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> Result<Resp, RpcError<E>>
    where
        Req: serde::Serialize + fmt::Debug,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let (tx, rx) = oneshot::<APIClientReq>();
        let codec = self.codec();
        self.caller().send_req_blocking(make_req(codec, service_method, req, tx));
        process_res(codec, rx.recv())
    }
}
*/

#[inline]
fn make_req<C, Req>(
    codec: &C, service_method: &'static str, req: &Req, done_tx: oneshot::TxOneshot<APIClientReq>,
) -> APIClientReq
where
    C: Codec,
    Req: serde::Serialize + fmt::Debug,
{
    let req_buf = codec.encode(req).expect("encode");
    APIClientReq {
        common: Default::default(),
        req_msg: Some(req_buf),
        action: service_method.to_string(),
        resp: None,
        res: None,
        noti: Some(done_tx),
    }
}

#[inline]
fn process_res<C, Resp, E>(
    codec: &C, task_res: Result<APIClientReq, crossfire::RecvError>,
) -> Result<Resp, RpcError<E>>
where
    C: Codec,
    Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
    E: RpcErrCodec,
{
    match task_res {
        Ok(mut task) => {
            let res = task.res.take().unwrap();
            match res {
                Ok(()) => {
                    if let Some(resp) = task.resp {
                        match codec.decode(&resp) {
                            Ok(resp_msg) => Ok(resp_msg),
                            Err(()) => Err(RpcIntErr::Decode.into()),
                        }
                    } else {
                        Ok(Resp::default())
                    }
                }
                Err(EncodedErr::Rpc(e)) => Err(RpcError::Rpc(e)),
                Err(EncodedErr::Num(n)) => match E::decode(codec, Ok(n)) {
                    Ok(e) => Err(RpcError::User(e)),
                    Err(()) => Err(RpcIntErr::Decode.into()),
                },
                Err(EncodedErr::Buf(buf)) => match E::decode(codec, Err(&buf)) {
                    Ok(e) => Err(RpcError::User(e)),
                    Err(()) => Err(RpcIntErr::Decode.into()),
                },
                _ => unreachable!(),
            }
        }
        Err(_) => Err(RpcIntErr::Internal.into()),
    }
}

/// A macro to implement AsyncEndpoint trait and Clone for a client struct.
///
/// The client struct must have `caller: C` and `codec: <C::Facts as ClientFacts>::Codec` fields.
///
/// # Example
///
/// ```ignore
/// pub struct MyClient<C> {
///     caller: C,
/// }
///
/// impl_client!(MyClient);
/// ```
#[macro_export]
macro_rules! impl_client {
    ($client:ident) => {
        impl<C> Clone for $client<C>
        where
            C: $crate::client::ClientCaller + Clone + Sync,
            C::Facts: $crate::client::ClientFacts<Task = $crate::client::task::APIClientReq>,
            <C::Facts as $crate::client::ClientFacts>::Codec: Clone,
        {
            fn clone(&self) -> Self {
                Self { caller: self.caller.clone() }
            }
        }

        impl<C> std::convert::AsRef<C> for $client<C>
        where
            C: $crate::client::ClientCaller + Sync,
            C::Facts: $crate::client::ClientFacts<Task = $crate::client::task::APIClientReq>,
        {
            fn as_ref(&self) -> &C {
                &self.caller
            }
        }
    };
}
