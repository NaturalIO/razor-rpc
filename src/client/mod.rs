pub mod task;
use crossfire::oneshot::oneshot;
use crossfire::*;
pub use razor_rpc_macros::{endpoint_async, endpoint_client};
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

/// Optional helper trait to Provide two convenient function to crete ClientPool / FailoverPool
///
/// We automatically impl this trait for traits met the requirement `ClientFacts<Task = APIClientReq>`.
/// To use it, you just have to:
///
/// ```
/// use razor_rpc::client::APIClientFacts;
/// ```
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

pub trait AsyncEndpoint<C>: AsRef<C> + Sync
where
    C: ClientCaller<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn codec(&self) -> &<C::Facts as ClientFacts>::Codec;

    fn caller(&self) -> &C {
        self.as_ref()
    }

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
            let codec = self.codec();
            <C as ClientCaller>::send_req(self.caller(), make_req(codec, service_method, req, tx))
                .await;
            process_res(codec, rx.recv_async().await)
        }
    }
}

// AsyncEndpoint trait is provided for user-defined clients
// Users implement this trait on their client structs to get the call() method

pub trait BlockingEndpoint<C>: AsRef<C>
where
    C: ClientCallerBlocking<Facts: ClientFacts<Task = APIClientReq>>,
{
    fn codec(&self) -> &<C::Facts as ClientFacts>::Codec;

    fn caller(&self) -> &C {
        self.as_ref()
    }

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

// BlockingEndpoint trait is provided for user-defined blocking clients
// Users implement this trait on their client structs to get the call() method

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
///     codec: <C::Facts as ClientFacts>::Codec,
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
                Self { caller: self.caller.clone(), codec: self.codec.clone() }
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

        impl<C> $crate::client::AsyncEndpoint<C> for $client<C>
        where
            C: $crate::client::ClientCaller + Sync,
            C::Facts: $crate::client::ClientFacts<Task = $crate::client::task::APIClientReq>,
        {
            fn codec(&self) -> &<C::Facts as $crate::client::ClientFacts>::Codec {
                &self.codec
            }
        }
    };
}
