use crate::{
    Codec,
    error::{EncodedErr, RpcErrCodec, RpcError, RpcIntErr},
};
use crossfire::oneshot::TxOneshot;
use razor_stream::client::task::{
    ClientTask, ClientTaskAction, ClientTaskCommon, ClientTaskDecode, ClientTaskDone,
    ClientTaskEncode,
};
use razor_stream::proto::RpcAction;
use std::fmt;
use std::io::Write;

/// Routing info for failover/retry operations
pub struct RoutingInfo {}

pub struct APIClientReq {
    pub common: ClientTaskCommon,
    pub req_msg: Option<Vec<u8>>,
    /// action is in "Service.method" format
    pub action: String,
    pub resp: Option<Vec<u8>>,
    pub res: Option<Result<(), EncodedErr>>,
    pub noti: Option<TxOneshot<Self>>,
    /// route info for FailoverPool
    pub config_ver: u64,
    /// Routing info for FailoverPool
    pub last_index: usize,
}

impl APIClientReq {
    #[inline]
    pub fn new<C, Req>(codec: &C, service_method: &'static str, req: &Req) -> Self
    where
        C: Codec,
        Req: serde::Serialize + fmt::Debug,
    {
        let req_buf = codec.encode(req).expect("encode");
        Self {
            common: Default::default(),
            req_msg: Some(req_buf), // TODO why some?
            action: service_method.to_string(),
            resp: None,
            res: None,
            noti: None,
            config_ver: 0,
            last_index: 0,
        }
    }

    #[inline]
    pub fn set_noti(&mut self, done_tx: TxOneshot<Self>) {
        self.noti.replace(done_tx);
    }

    #[inline]
    pub fn process_res<C, Resp, E>(&mut self, codec: &C) -> Result<Resp, RpcError<E>>
    where
        C: Codec,
        Resp: for<'a> serde::Deserialize<'a> + Send + fmt::Debug + 'static + Default,
        E: RpcErrCodec,
    {
        let res = self.res.take().unwrap();
        match res {
            Ok(()) => {
                if let Some(resp) = &self.resp {
                    match codec.decode(resp) {
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
    fn set_custom_error<C: Codec>(
        &mut self, _codec: &C, e: EncodedErr, last_idx: usize, config_ver: u64,
    ) {
        // Ignore the Codec, as we don't known the error type yet
        self.res.replace(Err(e));
        self.config_ver = config_ver;
        self.last_index = last_idx;
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
