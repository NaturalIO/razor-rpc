//! task for server internal use, only need to known when you are writing
//! [Dispatch](crate::server::dispatch)

use crate::{Codec, error::*};
use razor_stream::server::task::{RespNoti, ServerTaskEncode, ServerTaskResp};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::Write;
use std::sync::Arc;

pub struct APIServerReq<C: Codec> {
    pub seq: u64,
    pub service: String,
    pub method: String,
    pub req: Option<Vec<u8>>,
    pub codec: Arc<C>,
    pub noti: RespNoti<APIServerResp>,
}

impl<C: Codec> APIServerReq<C> {
    #[inline]
    pub fn decode<'a, R: Deserialize<'a>>(&'a mut self, buf: &'a [u8]) -> Result<R, ()> {
        self.codec.decode::<R>(buf)
    }

    #[inline(always)]
    pub fn set_result<R: Serialize>(self, resp: R) {
        match self.codec.encode::<R>(&resp) {
            Err(()) => {
                self.noti.done(APIServerResp {
                    seq: self.seq,
                    msg: None,
                    res: Some(Err(RpcIntErr::Encode.into())),
                });
            }
            Ok(msg) => {
                self.noti.done(APIServerResp { seq: self.seq, msg: Some(msg), res: Some(Ok(())) });
            }
        }
    }

    #[inline(always)]
    pub fn set_rpc_error(self, e: RpcIntErr) {
        self.noti.done(APIServerResp { seq: self.seq, msg: None, res: Some(Err(e.into())) });
    }

    #[inline(always)]
    pub fn set_error<E: RpcErrCodec>(self, e: RpcError<E>) {
        let encoded_err = match e {
            RpcError::Rpc(rpc_int_err) => rpc_int_err.into(),
            RpcError::User(user_err) => user_err.encode(self.codec.as_ref()),
        };
        self.noti.done(APIServerResp { seq: self.seq, msg: None, res: Some(Err(encoded_err)) });
    }
}

pub struct APIServerResp {
    pub seq: u64,
    pub msg: Option<Vec<u8>>,
    pub res: Option<Result<(), EncodedErr>>,
}

impl fmt::Debug for APIServerResp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "resp {} res {:?}", self.seq, self.res)
    }
}

impl ServerTaskEncode for APIServerResp {
    #[inline]
    fn encode_resp<'a, 'b, C: Codec>(
        &'a mut self, _codec: &'b C, buf: &'b mut Vec<u8>,
    ) -> (u64, Result<(usize, Option<&'a [u8]>), EncodedErr>) {
        match self.res.take().unwrap() {
            Ok(()) => {
                if let Some(msg) = self.msg.as_ref() {
                    buf.write_all(msg).expect("fill msg buf");
                    (self.seq, Ok((msg.len(), None)))
                } else {
                    (self.seq, Ok((0, None)))
                }
            }
            Err(e) => (self.seq, Err(e)),
        }
    }
}

impl ServerTaskResp for APIServerResp {}
