use super::client::{FileAction, FileIOReq, FileIOResp, FileOpenReq};
use nix::errno::Errno;
use razor_rpc_codec::MsgpCodec;
use razor_rpc_tcp::TcpServer;
use razor_stream::server::{dispatch::*, task::*, *};

pub type MyServer = razor_stream::server::ServerDefault;

pub fn init_server(config: ServerConfig) -> RpcServer<MyServer> {
    let facts = MyServer::new(config);
    RpcServer::new(facts)
}

pub async fn init_server_closure<H, FH>(
    server_handle: H, config: ServerConfig, addr: &str,
) -> Result<(RpcServer<MyServer>, String), std::io::Error>
where
    H: FnOnce(FileServerTask) -> FH + Send + Sync + 'static + Clone,
    FH: Future<Output = Result<(), ()>> + Send + 'static,
{
    // NOTE: Do not new rt to the client, pass a handle from TestRunner.
    // since client may be drop by test logic, it's not allow
    // to drop a tokio runtime inside async code.
    let mut server = init_server(config);
    let dispatch = new_closure_dispatcher(server_handle);
    let local_addr = server.listen::<TcpServer<crate::RT>, _>(addr, dispatch).await?;
    Ok((server, local_addr))
}

pub fn new_closure_dispatcher<H, FH>(handle: H) -> impl Dispatch
where
    H: FnOnce(FileServerTask) -> FH + Send + Sync + 'static + Clone,
    FH: Future<Output = Result<(), ()>> + Send + 'static,
{
    DispatchClosure::<MsgpCodec, FileServerTask, FileServerTask, _, _>::new(handle.clone())
}

#[server_task_enum(req, resp, error = Errno)]
#[derive(Debug)]
pub enum FileServerTask {
    #[action(FileAction::Open)]
    Open(ServerTaskOpen),
    #[action(FileAction::Read, FileAction::Write)]
    IO(ServerTaskIO),
}

pub type ServerTaskOpen = ServerTaskVariantFull<FileServerTask, FileOpenReq, (), Errno>;
pub type ServerTaskIO = ServerTaskVariantFull<FileServerTask, FileIOReq, FileIOResp, Errno>;
