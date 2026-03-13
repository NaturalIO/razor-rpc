//! # The protocol
//!
//! The packet starts with a fixed-length header and is followed by a variable-length body.
//! An [RpcAction] represents the type of packet.
//! The action type is either numeric or a string.
//!
//! The request body contains a mandatory structured message and optional blob data.
//!
//! The response for each request either returns successfully with an optional structured message and
//! optional blob data (the response can be empty), or it returns with an RpcError. The error type can
//! be numeric (like a Unix errno), text (for user-customized errors), or a statically predefined error
//! string (for errors that occur during socket communication or encoding/decoding).
//!
//! ## Request
//!
//! Fixed length of `ReqHead` = 32B
//!
//! | Field     | Size | Description                               |
//! |-----------|------|-------------------------------------------|
//! | `magic`   | 2B   | Magic number                              |
//! | `ver`     | 1B   | Protocol version                          |
//! | `format`  | 1B   | Encoder-decoder format                    |
//! | `action`  | 4B   | Action type (numeric or length if string) |
//! | `seq`     | 8B   | Increased ID of request message           |
//! | `client_id`| 8B   | Client identifier                         |
//! | `msg_len` | 4B   | Structured message length                 |
//! | `blob_len`| 4B   | Unstructured message (blob) length        |
//!
//! Variable length message components:
//! - `action_len` (if `action` is a string)
//! - `msg_len`
//! - `blob_len`
//!
//! ## Response:
//!
//! Fixed length of `RespHead` = 20B
//!
//! | Field     | Size | Description                               |
//! |-----------|------|-------------------------------------------|
//! | `magic`   | 2B   | Magic number                              |
//! | `ver`     | 1B   | Protocol version                          |
//! | `has_err` | 1B   | Error flag                                |
//! | `seq`     | 8B   | Increased ID of request message           |
//! | `msg_len` | 4B   | Structured message length or errno        |
//! | `blob_len`| 4B   | Unstructured message (blob) length        |
//!
//! Variable length message components:
//! - `msg_len`
//! - `blob_len`

use crate::client::task::ClientTask;
use crate::server::task::ServerTaskEncode;
use crate::{Codec, error::*};
use std::fmt;
use std::io::Write;
use std::mem::size_of;
use std::ptr::addr_of;
use zerocopy::byteorder::little_endian;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub const PING_ACTION: u32 = 0;

pub const RPC_MAGIC: little_endian::U16 = little_endian::U16::new(19749);
pub const U32_HIGH_MASK: u32 = 1 << 31;

pub const RESP_FLAG_HAS_ERRNO: u8 = 1;
pub const RESP_FLAG_HAS_ERR_STRING: u8 = 2;
pub const RPC_VERSION_1: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RpcAction<'a> {
    Str(&'a str),
    Num(i32),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RpcActionOwned {
    Str(String),
    Num(i32),
}

impl<'a> From<RpcAction<'a>> for RpcActionOwned {
    fn from(action: RpcAction<'a>) -> Self {
        match action {
            RpcAction::Str(s) => RpcActionOwned::Str(s.to_string()),
            RpcAction::Num(n) => RpcActionOwned::Num(n),
        }
    }
}

impl RpcActionOwned {
    pub fn to_action<'a>(&'a self) -> RpcAction<'a> {
        match self {
            RpcActionOwned::Str(s) => RpcAction::Str(s.as_str()),
            RpcActionOwned::Num(n) => RpcAction::Num(*n),
        }
    }
}

/// Fixed-length header for request
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, KnownLayout, PartialEq, Clone)]
#[repr(C, packed)]
pub struct ReqHead {
    pub magic: little_endian::U16,
    pub ver: u8,
    pub format: u8,
    /// encoder-decoder format

    /// If highest bit is 0, the rest will be i32 action_num.
    ///
    /// If highest is 1, the lower bit will be i32 action_len.
    pub action: little_endian::U32,

    /// Increased ID of request msg in the socket connection.
    pub seq: little_endian::U64,

    pub client_id: little_endian::U64,
    /// structured msg len
    pub msg_len: little_endian::U32,
    /// unstructured msg
    pub blob_len: little_endian::U32,
}

pub const RPC_REQ_HEADER_LEN: usize = size_of::<ReqHead>();

impl ReqHead {
    #[inline(always)]
    pub fn encode_ping(buf: &mut Vec<u8>, client_id: u64, seq: u64) {
        debug_assert!(buf.capacity() > RPC_REQ_HEADER_LEN);
        unsafe { buf.set_len(RPC_REQ_HEADER_LEN) };
        Self::_write_head(buf, client_id, PING_ACTION, seq, 0, 0);
    }

    #[inline(always)]
    fn _write_head(
        buf: &mut [u8], client_id: u64, action: u32, seq: u64, msg_len: u32, blob_len: i32,
    ) {
        // NOTE: We are directly init ReqHead on the buffer with unsafe, check carefully don't miss
        // a field
        let header: &mut Self =
            Self::mut_from_bytes(&mut buf[0..RPC_REQ_HEADER_LEN]).expect("fill header buf");
        header.magic = RPC_MAGIC;
        header.ver = RPC_VERSION_1;
        header.format = 0;
        header.action.set(action);
        header.seq.set(seq);
        header.client_id.set(client_id);
        header.msg_len.set(msg_len);
        header.blob_len.set(blob_len as u32);
    }

    /// write header, action, msg into `buf`, return reference to blob if there's any
    #[inline(always)]
    pub fn encode<'a, T, C>(
        codec: &C, buf: &mut Vec<u8>, client_id: u64, task: &'a T,
    ) -> Result<Option<&'a [u8]>, ()>
    where
        T: ClientTask,
        C: Codec,
    {
        debug_assert!(buf.capacity() >= RPC_REQ_HEADER_LEN);
        // Leave a room at the beginning of buffer for ReqHead
        unsafe { buf.set_len(RPC_REQ_HEADER_LEN) };
        // But we have to write action str and encode the msg first to get message data len
        let action_flag: u32;
        match task.get_action() {
            RpcAction::Num(num) => action_flag = num as u32,
            RpcAction::Str(s) => {
                action_flag = s.len() as u32 | U32_HIGH_MASK;
                buf.write_all(s.as_bytes()).expect("fill action buffer");
            }
        }
        let msg_len = task.encode_req(codec, buf)?;
        if msg_len > u32::MAX as usize {
            error!("ReqHead: req len {} cannot larger than u32", msg_len);
            return Err(());
        }
        let blob = task.get_req_blob();
        let blob_len = if let Some(blob) = blob { blob.len() } else { 0 };
        if blob_len > i32::MAX as usize {
            error!("ReqHead: blob_len {} cannot larger than i32", blob_len);
            return Err(());
        }
        Self::_write_head(buf, client_id, action_flag, task.seq(), msg_len as u32, blob_len as i32);
        Ok(blob)
    }

    #[inline(always)]
    pub fn decode_head(head_buf: &[u8]) -> Result<&Self, RpcIntErr> {
        let head: &Self = Self::ref_from_bytes(head_buf).expect("from header buf");
        if head.magic != RPC_MAGIC {
            warn!("rpc server: wrong magic receive {:?}", head.magic);
            return Err(RpcIntErr::IO);
        }
        if head.ver != RPC_VERSION_1 {
            warn!("rpc server: version {} not supported", head.ver);
            return Err(RpcIntErr::Version);
        }
        return Ok(head);
    }

    #[inline]
    pub fn get_action(&self) -> Result<i32, i32> {
        if self.action & U32_HIGH_MASK == 0 {
            Ok(self.action.get() as i32)
        } else {
            let action_len = self.action ^ U32_HIGH_MASK;
            Err(action_len.get() as i32)
        }
    }
}

impl fmt::Display for ReqHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = unsafe {
            write!(
                f,
                "[client_id:{}, seq:{}, msg:{}, blob:{}",
                addr_of!(self.client_id).read_unaligned(),
                addr_of!(self.seq).read_unaligned(),
                addr_of!(self.msg_len).read_unaligned(),
                addr_of!(self.blob_len).read_unaligned(),
            )
        };
        match self.get_action() {
            Ok(action_num) => {
                write!(f, ", action:{:?}]", action_num)
            }
            Err(action_len) => {
                write!(f, "action_len:{}]", action_len)
            }
        }
    }
}

impl fmt::Debug for ReqHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Fixed-length header for response
#[derive(FromBytes, IntoBytes, Unaligned, Immutable, KnownLayout, PartialEq, Clone)]
#[repr(C, packed)]
pub struct RespHead {
    pub magic: little_endian::U16,
    pub ver: u8,

    /// when flag == RESP_FLAG_HAS_ERRNO: msg_len is posix errno; blob_len = 0
    /// when flag == RESP_FLAG_HAS_ERR_STRING: msg_len=0, blob_len > 0 and follow an error string
    pub flag: u8,

    /// structured msg_len or errno
    pub msg_len: little_endian::U32,

    /// Increased ID of request msg in the socket connection (response.seq==request.seq)
    pub seq: little_endian::U64,
    /// unstructured msg, only support half of 16Byte, must larger than zero
    pub blob_len: little_endian::I32,
}

pub const RPC_RESP_HEADER_LEN: usize = size_of::<RespHead>();

impl RespHead {
    #[inline]
    pub fn encode<'a, 'b, L, C, T>(
        logger: &'b L, codec: &'b C, buf: &'b mut Vec<u8>, task: &'a mut T,
    ) -> (u64, Option<&'a [u8]>)
    where
        L: captains_log::filter::Filter,
        C: Codec,
        T: ServerTaskEncode,
    {
        debug_assert!(buf.capacity() >= RPC_RESP_HEADER_LEN);
        // Leave a room at the beginning of buffer for RespHead
        unsafe { buf.set_len(RPC_RESP_HEADER_LEN) };
        let (seq, r) = task.encode_resp(codec, buf);
        match r {
            Ok((msg_len, None)) => {
                if msg_len > u32::MAX as usize {
                    error!("write_resp: encoded msg len {} exceed u32 limit", msg_len);
                    Self::_encode_error::<L>(logger, buf, seq, EncodedErr::Rpc(RpcIntErr::Encode));
                } else {
                    Self::_write_head(logger, buf, 0, seq, msg_len as u32, 0);
                }
                return (seq, None);
            }
            Ok((msg_len, Some(blob))) => {
                if msg_len > u32::MAX as usize {
                    error!("write_resp: encoded msg len {} exceed u32 limit", msg_len);
                    Self::_encode_error::<L>(logger, buf, seq, EncodedErr::Rpc(RpcIntErr::Encode));
                    return (seq, None);
                } else if blob.len() > i32::MAX as usize {
                    error!("write_resp: blob len {} exceed i32 limit", blob.len());
                    Self::_encode_error::<L>(logger, buf, seq, EncodedErr::Rpc(RpcIntErr::Encode));
                    return (seq, None);
                }
                Self::_write_head::<L>(logger, buf, 0, seq, msg_len as u32, blob.len() as i32);
                return (seq, Some(blob));
            }
            Err(e) => {
                Self::_encode_error::<L>(logger, buf, seq, e);
                return (seq, None);
            }
        }
    }

    #[inline]
    pub fn encode_internal<'a, L>(
        logger: &'a L, buf: &'a mut Vec<u8>, seq: u64, err: Option<RpcIntErr>,
    ) -> u64
    where
        L: captains_log::filter::Filter,
    {
        debug_assert!(buf.capacity() >= RPC_RESP_HEADER_LEN);
        // Leave a room at the beginning of buffer for RespHead
        unsafe { buf.set_len(RPC_RESP_HEADER_LEN) };
        if let Some(e) = err {
            Self::_encode_error::<L>(logger, buf, seq, e.into());
            return seq;
        } else {
            // ping
            Self::_write_head::<L>(logger, buf, 0, seq, 0, 0);
            return seq;
        }
    }

    #[inline(always)]
    fn _encode_error<'b, L>(logger: &'b L, buf: &'b mut Vec<u8>, seq: u64, e: EncodedErr)
    where
        L: captains_log::filter::Filter,
    {
        macro_rules! write_err {
            ($s: expr) => {
                Self::_write_head::<L>(
                    logger,
                    buf,
                    RESP_FLAG_HAS_ERR_STRING,
                    seq,
                    0,
                    $s.len() as i32,
                );
                buf.write_all($s).expect("fill error str");
            };
        }
        match e {
            EncodedErr::Num(n) => {
                Self::_write_head::<L>(logger, buf, RESP_FLAG_HAS_ERRNO, seq, n, 0);
            }
            EncodedErr::Rpc(s) => {
                write_err!(s.as_bytes());
            }
            EncodedErr::Buf(s) => {
                write_err!(&s);
            }
            EncodedErr::Static(s) => {
                write_err!(s.as_bytes());
            }
        }
    }

    #[inline]
    fn _write_head<L>(logger: &L, buf: &mut [u8], flag: u8, seq: u64, msg_len: u32, blob_len: i32)
    where
        L: captains_log::filter::Filter,
    {
        let header = Self::mut_from_bytes(&mut buf[0..RPC_RESP_HEADER_LEN]).expect("fill header");
        header.magic = RPC_MAGIC;
        header.ver = RPC_VERSION_1;
        header.flag = flag;
        header.msg_len.set(msg_len);
        header.seq.set(seq);
        header.blob_len.set(blob_len);
        logger_trace!(logger, "resp {:?}", header);
    }

    #[inline(always)]
    pub fn decode_head(head_buf: &[u8]) -> Result<&Self, RpcIntErr> {
        let head: &Self = Self::ref_from_bytes(head_buf).expect("decode header");
        if head.magic != RPC_MAGIC {
            warn!("rpc server: wrong magic receive {:?}", head.magic);
            return Err(RpcIntErr::IO);
        }
        if head.ver != RPC_VERSION_1 {
            warn!("rpc server: version {} not supported", head.ver);
            return Err(RpcIntErr::Version);
        }
        return Ok(head);
    }
}

impl fmt::Display for RespHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unsafe {
            write!(
                f,
                "[seq:{}, flag:{}, msg:{}, blob:{}]",
                addr_of!(self.seq).read_unaligned(), // format_args deals with unaligned field
                addr_of!(self.flag).read_unaligned(),
                addr_of!(self.msg_len).read_unaligned(),
                addr_of!(self.blob_len).read_unaligned(),
            )
        }
    }
}

impl fmt::Debug for RespHead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_header_len() {
        assert_eq!(RPC_REQ_HEADER_LEN, 32);
        assert_eq!(RPC_RESP_HEADER_LEN, 20);
    }
}
