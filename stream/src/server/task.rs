//! Trait for server-side task, and predefined task structures
//!
//! There are pre-defined structs that impl [ServerTaskDecode] and [ServerTaskResp]:
//! - [ServerTaskVariant] is for the situation to map a request struct to a response struct
//! - [ServerTaskVariantFull] is for the situation of holding the request and response msg in the same struct
//!
//! Provided macros:
//!
//! - [`#[server_task_enum]`](macro@server_task_enum): For defining a server-side RPC task enum.

pub use razor_stream_macros::server_task_enum;

use crate::proto::{RpcAction, RpcActionOwned};
use crate::{Codec, error::*};
use crossfire::*;
use io_buffer::Buffer;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Sum up trait for server response task
pub trait ServerTaskResp: ServerTaskEncode + Send + Sized + 'static + fmt::Debug {}

/// How to decode a server request
pub trait ServerTaskDecode<R: Send + 'static>: Send + Sized + 'static {
    fn decode_req<'a, C: Codec>(
        codec: &'a C, action: RpcAction<'a>, seq: u64, req: &'a [u8], blob: Option<Buffer>,
        noti: RespNoti<R>,
    ) -> Result<Self, ()>;
}

/// How to encode a server response
///
/// For a server task with any type of buffer, user can always return u8 slice, so the framework
/// don't need to known the type, but this requires reference and lifetime to the task.
/// for the returning EncodedErr, it's possible generated during the encode,
/// Otherwise when existing EncodedErr held in `res` field, the user need to take the res field out of the task.
pub trait ServerTaskEncode: Send + 'static {
    fn encode_resp<'a, 'b, C: Codec>(
        &'a mut self, codec: &'b C, buf: &'b mut Vec<u8>,
    ) -> (u64, Result<(usize, Option<&'a [u8]>), EncodedErr>);
}

/// How to notify Rpc framework when a task is done
///
/// This is not mandatory for the framework, this a guideline,
/// You can skip this as long as you send the result back to RespNoti.
pub trait ServerTaskDone<T, E>: Sized + 'static
where
    T: Send + 'static,
    E: RpcErrCodec,
{
    /// Should implement for enum delegation, not intended for user call
    fn _set_result(&mut self, res: Result<(), E>) -> RespNoti<T>;

    /// For users, set the result in the task and send it back
    #[inline]
    fn set_result(mut self, res: Result<(), E>)
    where
        T: std::convert::From<Self>,
    {
        // NOTE: To allow a trait to consume self, must require Sized
        let noti = self._set_result(res);
        let parent: T = self.into();
        noti.done(parent);
    }
}

/// Get RpcAction from a enum task, or a sub-type that fits multiple RpcActions
pub trait ServerTaskAction {
    fn get_action<'a>(&'a self) -> RpcAction<'a>;
}

/// A container that impl ServerTaskResp to show an example,
/// presuming you have a different types to represent Request and Response.
/// You can write your customize version.
#[allow(dead_code)]
pub struct ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: Send + 'static,
    E: RpcErrCodec,
{
    pub seq: u64,
    pub action: RpcActionOwned,
    pub msg: M,
    pub blob: Option<Buffer>,
    pub res: Option<Result<(), E>>,
    pub noti: Option<RespNoti<T>>,
}

impl<T, M, E> fmt::Debug for ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: fmt::Debug + Send + 'static,
    E: RpcErrCodec + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "task seq={} action={:?} {:?}", self.seq, self.action, self.msg)?;
        match self.res.as_ref() {
            Some(Ok(())) => {
                write!(f, " ok")
            }
            Some(Err(e)) => {
                write!(f, " err: {}", e)
            }
            _ => Ok(()),
        }
    }
}

impl<T, M, E> ServerTaskDone<T, E> for ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: Send + 'static,
    E: RpcErrCodec,
{
    fn _set_result(&mut self, res: Result<(), E>) -> RespNoti<T> {
        self.res.replace(res);
        return self.noti.take().unwrap();
    }
}

impl<T, M, E> ServerTaskDecode<T> for ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: for<'b> Deserialize<'b> + Send + 'static,
    E: RpcErrCodec,
{
    fn decode_req<'a, C: Codec>(
        codec: &'a C, action: RpcAction<'a>, seq: u64, msg: &'a [u8], blob: Option<Buffer>,
        noti: RespNoti<T>,
    ) -> Result<Self, ()> {
        let req = codec.decode(msg)?;
        Ok(Self { seq, action: action.into(), msg: req, blob, res: None, noti: Some(noti) })
    }
}

impl<T, M, E> ServerTaskAction for ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: Send + 'static,
    E: RpcErrCodec,
{
    fn get_action<'a>(&'a self) -> RpcAction<'a> {
        self.action.to_action()
    }
}

impl<T, M, E> ServerTaskEncode for ServerTaskVariant<T, M, E>
where
    T: Send + 'static,
    M: Serialize + Send + 'static,
    E: RpcErrCodec,
{
    #[inline]
    fn encode_resp<'a, 'b, C: Codec>(
        &'a mut self, codec: &'b C, buf: &'b mut Vec<u8>,
    ) -> (u64, Result<(usize, Option<&'a [u8]>), EncodedErr>) {
        if let Some(res) = self.res.as_ref() {
            match res {
                Ok(_) => match codec.encode_into(&self.msg, buf) {
                    Err(_) => {
                        return (self.seq, Err(RpcIntErr::Encode.into()));
                    }
                    Ok(msg_len) => {
                        return (self.seq, Ok((msg_len, self.blob.as_deref())));
                    }
                },
                Err(e) => {
                    return (self.seq, Err(e.encode::<C>(codec)));
                }
            }
        } else {
            panic!("no result when encode_resp");
        }
    }
}

/// A container that impl ServerTaskResp to show an example,
/// presuming you have a type to carry both Request and Response.
/// You can write your customize version.
#[allow(dead_code)]
pub struct ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: Send + 'static,
    P: Send + 'static,
    E: RpcErrCodec,
{
    pub seq: u64,
    pub action: RpcActionOwned,
    pub req: R,
    pub req_blob: Option<Buffer>,
    pub resp: Option<P>,
    pub resp_blob: Option<Buffer>,
    pub res: Option<Result<(), E>>,
    noti: Option<RespNoti<T>>,
}

impl<T, R, P, E> fmt::Debug for ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: Send + 'static + fmt::Debug,
    P: Send + 'static,
    E: RpcErrCodec + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "task seq={} action={:?} {:?}", self.seq, self.action, self.req)?;
        match self.res.as_ref() {
            Some(Ok(())) => write!(f, " ok"),
            Some(Err(e)) => write!(f, " err: {}", e),
            _ => Ok(()),
        }
    }
}

impl<T, R, P, E> ServerTaskDone<T, E> for ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: Send + 'static,
    P: Send + 'static,
    E: RpcErrCodec,
{
    fn _set_result(&mut self, res: Result<(), E>) -> RespNoti<T> {
        self.res.replace(res);
        return self.noti.take().unwrap();
    }
}

impl<T, R, P, E> ServerTaskDecode<T> for ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: for<'b> Deserialize<'b> + Send + 'static,
    P: Send + 'static,
    E: RpcErrCodec,
{
    fn decode_req<'a, C: Codec>(
        codec: &'a C, action: RpcAction<'a>, seq: u64, msg: &'a [u8], blob: Option<Buffer>,
        noti: RespNoti<T>,
    ) -> Result<Self, ()> {
        let req = codec.decode(msg)?;
        Ok(Self {
            seq,
            action: action.into(),
            req,
            req_blob: blob,
            res: None,
            resp: None,
            resp_blob: None,
            noti: Some(noti),
        })
    }
}

impl<T, R, P, E> ServerTaskAction for ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: Send + 'static,
    P: Send + 'static,
    E: RpcErrCodec,
{
    fn get_action<'a>(&'a self) -> RpcAction<'a> {
        self.action.to_action()
    }
}

impl<T, R, P, E> ServerTaskEncode for ServerTaskVariantFull<T, R, P, E>
where
    T: Send + 'static,
    R: Send + 'static,
    P: Send + 'static + Serialize,
    E: RpcErrCodec,
{
    #[inline]
    fn encode_resp<'a, 'b, C: Codec>(
        &'a mut self, codec: &'b C, buf: &'b mut Vec<u8>,
    ) -> (u64, Result<(usize, Option<&'a [u8]>), EncodedErr>) {
        if let Some(res) = self.res.as_ref() {
            match res {
                Ok(_) => {
                    if let Some(resp) = self.resp.as_ref() {
                        match codec.encode_into(resp, buf) {
                            Err(_) => {
                                return (self.seq, Err(RpcIntErr::Encode.into()));
                            }
                            Ok(msg_len) => {
                                return (self.seq, Ok((msg_len, self.resp_blob.as_deref())));
                            }
                        }
                    } else {
                        // empty response
                        return (self.seq, Ok((0, self.resp_blob.as_deref())));
                    }
                }
                Err(e) => {
                    return (self.seq, Err(e.encode::<C>(codec)));
                }
            }
        } else {
            panic!("no result when encode_resp");
        }
    }
}

/// A writer channel to send response to the server framework.
///
/// It can be cloned anywhere.
/// The user doesn't need to call it directly.
pub struct RespNoti<T: Send + 'static>(
    pub(crate) MTx<mpsc::List<Result<T, (u64, Option<RpcIntErr>)>>>,
);

impl<T: Send + 'static> Clone for RespNoti<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + 'static> RespNoti<T> {
    pub fn new(tx: MTx<mpsc::List<Result<T, (u64, Option<RpcIntErr>)>>>) -> Self {
        Self(tx)
    }

    #[inline]
    pub fn done(self, task: T) {
        let _ = self.0.send(Ok(task));
    }

    #[inline]
    pub(crate) fn send_err(&self, seq: u64, err: Option<RpcIntErr>) -> Result<(), ()> {
        if self.0.send(Err((seq, err))).is_err() { return Err(()) } else { Ok(()) }
    }
}
