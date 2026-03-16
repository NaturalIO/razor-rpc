use crossfire::*;
use io_buffer::Buffer;
use nix::errno::Errno;
use razor_rpc_codec::MsgpCodec;
use razor_rpc_tcp::TcpClient;
use razor_stream::client::stream::ClientStream;
use razor_stream::client::task::*;
use razor_stream::client::*;
use razor_stream::error::{RpcError, RpcIntErr};
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, atomic::AtomicU64};

pub type MyClient = ClientDefault<FileClientTask, MsgpCodec>;

pub type FileClient = ClientStream<MyClient, TcpClient<crate::RT>>;

pub async fn init_client(
    config: ClientConfig, addr: &str, last_resp_ts: Option<Arc<AtomicU64>>, rt: &crate::RT,
) -> Result<FileClient, RpcIntErr> {
    let facts = MyClient::new(config);
    FileClient::connect(facts, rt, addr, &format!("to {}", addr), last_resp_ts).await
}

#[derive(PartialEq, Debug)]
#[repr(u8)]
pub enum FileAction {
    Open = 1,
    Read = 2,
    Write = 3,
}

impl TryFrom<u8> for FileAction {
    type Error = RpcError<Errno>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(FileAction::Open),
            2 => Ok(FileAction::Read),
            3 => Ok(FileAction::Write),
            _ => Err(RpcIntErr::Method.into()),
        }
    }
}

#[derive(Debug)]
#[client_task_enum(error = Errno)]
pub enum FileClientTask {
    #[action(FileAction::Open)]
    Open(FileClientTaskOpen),
    #[action(FileAction::Read)]
    Read(FileClientTaskRead),
    #[action(FileAction::Write)]
    Write(FileClientTaskWrite),
}

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct FileOpenReq {
    pub path: String,
}

#[client_task(debug)]
pub struct FileClientTaskOpen {
    #[field(common)]
    pub common: ClientTaskCommon,
    #[field(req)]
    pub req: FileOpenReq,
    #[field(resp)]
    pub resp: Option<()>,
    #[field(res)]
    pub res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    pub sender: Option<MTx<mpsc::List<FileClientTask>>>,
}

impl FileClientTaskOpen {
    pub fn new(sender: MTx<mpsc::List<FileClientTask>>, path: String) -> Self {
        Self {
            common: Default::default(),
            sender: Some(sender),
            req: FileOpenReq { path },
            res: None,
            resp: None,
        }
    }
}

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct FileIOReq {
    pub inode: u64,
    pub offset: i64,
    pub len: usize,
}

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct FileIOResp {
    pub ret_size: u64,
}

#[client_task(debug)]
pub struct FileClientTaskRead {
    #[field(common)]
    pub common: ClientTaskCommon,
    #[field(req)]
    pub req: FileIOReq,
    #[field(resp)]
    pub resp: Option<FileIOResp>,
    #[field(resp_blob)]
    pub read_data: Option<Buffer>,
    #[field(res)]
    pub res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    pub sender: Option<MTx<mpsc::List<FileClientTask>>>,
}

impl FileClientTaskRead {
    pub fn new(
        sender: MTx<mpsc::List<FileClientTask>>, inode: u64, offset: i64, len: usize,
    ) -> Self {
        Self {
            common: Default::default(),
            sender: Some(sender),
            res: None,
            req: FileIOReq { inode, offset, len },
            resp: None,
            read_data: None,
        }
    }
}

#[client_task(debug)]
pub struct FileClientTaskWrite {
    #[field(common)]
    pub common: ClientTaskCommon,
    #[field(req)]
    pub req: FileIOReq,
    #[field(req_blob)]
    pub data: Buffer,
    #[field(resp)]
    pub resp: Option<FileIOResp>,
    #[field(res)]
    pub res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    pub sender: Option<MTx<mpsc::List<FileClientTask>>>,
}

impl FileClientTaskWrite {
    pub fn new(
        sender: MTx<mpsc::List<FileClientTask>>, inode: u64, offset: i64, data: Buffer,
    ) -> Self {
        Self {
            common: Default::default(),
            sender: Some(sender),
            res: None,
            req: FileIOReq { inode, offset, len: data.len() },
            data,
            resp: None,
        }
    }
}

pub async fn init_failover_client(
    config: ClientConfig, addrs: Vec<String>, round_robin: bool, rt: &crate::RT,
) -> FailoverPool<MyClient, TcpClient<crate::RT>> {
    // NOTE: Do not new rt to the client, pass a handle from TestRunner.
    // since client may be drop by test logic, it's not allow
    // to drop a tokio runtime inside async code.
    let facts = MyClient::new(config);
    FailoverPool::new(
        facts,
        rt,
        addrs,
        round_robin,
        3,   // retry_limit
        100, // pool_channel_size
    )
}
