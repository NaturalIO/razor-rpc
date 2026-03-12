use super::RpcSvrReq;
use super::task::*;
use crate::Codec;
use std::marker::PhantomData;
use std::sync::Arc;

/// Dispatch should be a user-defined struct initialized for every connection, by ServerFacts::new_dispatcher.
///
/// Dispatch must have Sync, because the connection reader and writer access concurrently.
///
/// A `Codec` should be created and holds inside, shared by the read/write coroutine.
/// If you have encryption in the Codec, it could have shared states.
pub trait Dispatch: Send + Sync + Sized + Clone + 'static {
    type RespTask: ServerTaskResp;

    type Codec: Codec;

    /// Decode and handle the request, called from the connection reader coroutine.
    ///
    /// You can dispatch them to a worker pool.
    /// If you are processing them directly in the connection coroutine, should make sure not
    /// blocking the thread for long.
    /// This is an async fn, but you should avoid waiting as much as possible.
    /// Should return Err(()) when codec decode_req failed.
    fn dispatch_req<'a>(
        &'a self, codec: &Arc<Self::Codec>, req: RpcSvrReq<'a>, noti: RespNoti<Self::RespTask>,
    ) -> impl Future<Output = Result<(), ()>> + Send;
}

/// A Dispatch trait impl with a closure, only useful for writing tests.
///
/// NOTE: The closure requires Clone.
///
/// # Example
///
/// ```no_compile,ignore
/// use razor_stream::server::{ServerFacts, Dispatch};
/// impl ServerFacts for YourServer {
///
///     ...
///
///     #[inline]
///     fn new_dispatcher(&self) -> impl Dispatch<Self::RespTask> {
///         let dispatch_f = move |task: FileServerTask| {
///             async move {
///                 todo!();
///             }
///         }
///         return DispatchClosure::<MsgpCodec, YourServerTask, Self::RespTask, _, _>::new(
///             dispatch_f,
///         );
///     }
/// }
/// ```
pub struct DispatchClosure<C, T, R, H, F>
where
    C: Codec,
    T: ServerTaskDecode<R>,
    R: ServerTaskResp,
    H: FnOnce(T) -> F + Send + Sync + 'static + Clone,
    F: Future<Output = Result<(), ()>> + Send + 'static,
{
    task_handle: H,
    _phan: PhantomData<fn(&R, &T, &C)>,
}

impl<C, T, R, H, F> DispatchClosure<C, T, R, H, F>
where
    C: Codec,
    T: ServerTaskDecode<R>,
    R: ServerTaskResp,
    H: FnOnce(T) -> F + Send + Sync + 'static + Clone,
    F: Future<Output = Result<(), ()>> + Send + 'static,
{
    #[inline]
    pub fn new(task_handle: H) -> Self {
        Self { task_handle, _phan: Default::default() }
    }
}

impl<C, T, R, H, F> Clone for DispatchClosure<C, T, R, H, F>
where
    C: Codec,
    T: ServerTaskDecode<R>,
    R: ServerTaskResp,
    H: FnOnce(T) -> F + Send + Sync + 'static + Clone,
    F: Future<Output = Result<(), ()>> + Send + 'static,
{
    #[inline]
    fn clone(&self) -> Self {
        Self::new(self.task_handle.clone())
    }
}
impl<C, T, R, H, F> Dispatch for DispatchClosure<C, T, R, H, F>
where
    C: Codec,
    T: ServerTaskDecode<R>,
    R: ServerTaskResp,
    H: FnOnce(T) -> F + Send + Sync + 'static + Clone,
    F: Future<Output = Result<(), ()>> + Send + 'static,
{
    type Codec = C;

    type RespTask = R;

    #[inline]
    async fn dispatch_req<'a>(
        &'a self, codec: &Arc<Self::Codec>, req: RpcSvrReq<'a>, noti: RespNoti<R>,
    ) -> Result<(), ()> {
        match <T as ServerTaskDecode<R>>::decode_req(
            codec.as_ref(),
            req.action,
            req.seq,
            req.msg,
            req.blob,
            noti,
        ) {
            Err(_) => {
                error!("action {:?} seq={} decode err", req.action, req.seq);
                return Err(());
            }
            Ok(task) => {
                let handle = self.task_handle.clone();
                if (handle)(task).await.is_err() {
                    error!("action {:?} seq={} dispatch err", req.action, req.seq);
                    return Err(());
                }
                Ok(())
            }
        }
    }
}
