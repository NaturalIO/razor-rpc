use crate::{
    Codec,
    error::{EncodedErr, RpcIntErr},
};
use crossfire::oneshot::TxOneshot;
use razor_stream::client::task::{
    ClientTask, ClientTaskAction, ClientTaskCommon, ClientTaskDecode, ClientTaskDone,
    ClientTaskEncode,
};
use razor_stream::proto::RpcAction;
use std::fmt;
use std::io::Write;

pub struct APIClientReq {
    pub common: ClientTaskCommon,
    pub req_msg: Option<Vec<u8>>,
    /// action is in "Service.method" format
    pub action: String,
    pub resp: Option<Vec<u8>>,
    pub res: Option<Result<(), EncodedErr>>,
    pub noti: Option<TxOneshot<Self>>,
}

impl ClientTaskEncode for APIClientReq {
    #[inline]
    fn encode_req<C: Codec>(&self, _codec: &C, buf: &mut Vec<u8>) -> Result<usize, ()> {
        if let Some(msg) = self.req_msg.as_ref() {
            // The msg is pre encoded
            buf.write_all(msg).expect("append msg");
            Ok(msg.len())
        } else {
            Ok(0)
        }
    }
}

impl ClientTaskDecode for APIClientReq {
    #[inline]
    fn decode_resp<C: Codec>(&mut self, _codec: &C, buf: &[u8]) -> Result<(), ()> {
        // Ignore the Codec, as we don't known the resp type yet
        if !buf.is_empty() {
            self.resp.replace(buf.to_vec());
        }
        Ok(())
    }
}

impl ClientTaskDone for APIClientReq {
    #[inline]
    fn set_custom_error<C: Codec>(&mut self, _codec: &C, e: EncodedErr) {
        // Ignore the Codec, as we don't known the error type yet
        self.res.replace(Err(e));
    }

    #[inline]
    fn set_rpc_error(&mut self, e: RpcIntErr) {
        self.res.replace(Err(e.into()));
    }

    #[inline]
    fn set_ok(&mut self) {
        self.res.replace(Ok(()));
    }

    #[inline]
    fn done(mut self) {
        self.noti.take().unwrap().send(self);
    }
}

impl std::ops::Deref for APIClientReq {
    type Target = ClientTaskCommon;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.common
    }
}

impl std::ops::DerefMut for APIClientReq {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.common
    }
}

impl ClientTaskAction for APIClientReq {
    #[inline]
    fn get_action<'a>(&'a self) -> RpcAction<'a> {
        RpcAction::Str(self.action.as_str())
    }
}

impl fmt::Debug for APIClientReq {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "APIClientReq(seq={}, action={})", self.seq, self.action)
    }
}

impl ClientTask for APIClientReq {}
