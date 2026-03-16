//! Trait and utils for client-side task
//!
//! Provided Macros
//!
//! - [`#[client_task]`](macro@client_task): For defining a client-side RPC task on a struct. It will not generate ClientTask trait (it's optional to you to define ClientTaskAction with it)
//! - [`#[client_task_enum]`](macro@client_task_enum): For creating an enum that delegates to client task variants. It will generate ClientTask trait for the enum

use crate::proto::RpcAction;
use crate::{
    Codec,
    error::{EncodedErr, RpcErrCodec, RpcError, RpcIntErr},
};
use std::fmt;
use std::ops::DerefMut;

pub use razor_stream_macros::{client_task, client_task_enum};

/// Sum up trait for client task, including request and response
pub trait ClientTask:
    ClientTaskAction
    + ClientTaskEncode
    + ClientTaskDecode
    + ClientTaskDone
    + DerefMut<Target = ClientTaskCommon>
    + Send
    + Sized
    + 'static
    + fmt::Debug
    + Unpin
{
}

/// Encode the request to buffer that can be send to server
pub trait ClientTaskEncode {
    /// sererialized the msg into buf (with std::io::Writer), and return the size written
    fn encode_req<C: Codec>(&self, codec: &C, buf: &mut Vec<u8>) -> Result<usize, ()>;

    /// Contain optional extra data to send to server side.
    ///
    /// By Default, return None when client task does not have a req_blob field
    #[inline(always)]
    fn get_req_blob(&self) -> Option<&[u8]> {
        None
    }
}

/// Decode the response from server and assign to the task struct
pub trait ClientTaskDecode {
    fn decode_resp<C: Codec>(&mut self, codec: &C, buf: &[u8]) -> Result<(), ()>;

    /// You can call crate::io::AllocateBuf::reserve(_size) on the following types:
    /// `Option<Vec<u8>>`, `Vec<u8>`, `Option<io_buffer::Buffer>`, `io_buffer::Buffer`
    ///
    /// By Default, return None when client task does not have a resp_blob field
    #[inline(always)]
    fn reserve_resp_blob(&mut self, _size: i32) -> Option<&mut [u8]> {
        None
    }
}

/// client_task_enum should impl this for user, not used by framework
pub trait ClientTaskGetResult<E: RpcErrCodec> {
    /// Check the result of the task
    fn get_result(&self) -> Result<(), &RpcError<E>>;
}

/// How to get notify from Rpc framework to user when a task is done
///
/// The rpc framework first call set_ok or set_xxx_error, then call done
pub trait ClientTaskDone: Sized + 'static {
    /// Set the custom error Called by RPC transport layer.
    /// the last_index and conf_ver is for FailoverPool callback to original ClientTask
    fn set_custom_error<C: Codec>(
        &mut self, codec: &C, e: EncodedErr, last_index: usize, conf_ver: u64,
    );

    fn set_rpc_error(&mut self, e: RpcIntErr);

    fn set_ok(&mut self);

    fn done(self);
}

/// Get RpcAction from a enum task, or a sub-type that fits multiple RpcActions
pub trait ClientTaskAction {
    fn get_action<'a>(&'a self) -> RpcAction<'a>;
}

/// A common struct for every ClientTask
///
/// The fields might be extended in the future
#[derive(Debug, Default)]
pub struct ClientTaskCommon {
    /// Every task should be assigned an ID which is unique inside a socket connection
    pub seq: u64,
}

impl ClientTaskCommon {
    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
}
