pub mod task;
use crossfire::oneshot::oneshot;
use crossfire::*;
pub use razor_rpc_macros::endpoint_async;
pub use razor_stream::client::ClientCaller;
pub use task::*;

use crate::Codec;
use crate::error::{EncodedErr, RpcErrCodec, RpcError, RpcIntErr};
pub use razor_stream::client::{
    ClientCallerBlocking, ClientConfig, ClientFacts, ClientPool, ClientTransport, FailoverPool,
};
use std::fmt;
use std::sync::Arc;

pub type APIClientDefault<IO, C> = razor_stream::client::ClientDefault<APIClientReq, IO, C>;

pub trait APIClientFacts: ClientFacts<Task = APIClientReq> {
    fn create_pool_async<T: ClientTransport>(self: Arc<Self>, addr: &str) -> ClientPool<Self, T> {
        ClientPool::new(self.clone(), addr, 0)
    }

    fn create_failover_async<T: ClientTransport>(
        self: Arc<Self>, addrs: Vec<String>, round_robin: bool, retry_limit: usize,
    ) -> Arc<FailoverPool<Self, T>> {
        Arc::new(FailoverPool::new(self.clone(), addrs, round_robin, retry_limit, 0))
    }
}

impl<F: ClientFacts<Task = APIClientReq>> APIClientFacts for F {}

pub struct AsyncEndpoint<C>
where
    C: ClientCaller<Facts: ClientFacts<Task = APIClientReq>>,
{
    caller: C,
    codec: <C::Facts as ClientFacts>::Codec,
}

impl<C> AsyncEndpoint<C>
where
    C: ClientCaller<Facts: ClientFacts<Task = APIClientReq>>,
{
    pub fn new(caller: C) -> Self {
        Self { caller, codec: Default::default() }
    }

    pub async fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> Result<Resp, RpcError<E>>
    where
        Req: serde::Serialize + fmt::Debug,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let (tx, rx) = oneshot::<APIClientReq>();
        // TODO should optimize one shot channel
        <C as ClientCaller>::send_req(&self.caller, make_req(&self.codec, service_method, req, tx))
            .await;
        process_res(&self.codec, rx.recv_async().await)
    }
}

impl<C> Clone for AsyncEndpoint<C>
where
    C: Clone + ClientCaller<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn clone(&self) -> Self {
        Self::new(self.caller.clone())
    }
}

impl<C> std::ops::Deref for AsyncEndpoint<C>
where
    C: ClientCaller<Facts: ClientFacts<Task = APIClientReq>>,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.caller
    }
}

pub struct BlockingEndpoint<C>
where
    C: ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    caller: C,
    codec: <C::Facts as ClientFacts>::Codec,
}

impl<C> BlockingEndpoint<C>
where
    C: ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn new(caller: C) -> Self {
        Self { caller, codec: Default::default() }
    }

    pub fn call<Req, Resp, E>(
        &self, service_method: &'static str, req: &Req,
    ) -> Result<Resp, RpcError<E>>
    where
        Req: serde::Serialize + fmt::Debug,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let (tx, rx) = oneshot::<APIClientReq>();
        // TODO should optimize one shot channel
        self.caller.send_req_blocking(make_req(&self.codec, service_method, req, tx));
        process_res(&self.codec, rx.recv())
    }
}

impl<C> Clone for BlockingEndpoint<C>
where
    C: Clone + ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn clone(&self) -> Self {
        Self::new(self.caller.clone())
    }
}

impl<C> std::ops::Deref for BlockingEndpoint<C>
where
    C: ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.caller
    }
}

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
