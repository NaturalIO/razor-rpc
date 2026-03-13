use nix::errno::Errno;
use razor_rpc::client::{endpoint_async, endpoint_client};
use razor_rpc::error::RpcError;

endpoint_client!(CalClient);
endpoint_client!(EchoClient);

#[endpoint_async(CalClient)]
#[async_trait::async_trait]
pub trait CalService {
    async fn inc(&self, y: isize) -> Result<isize, RpcError<()>>;

    async fn add(&self, args: (isize, isize)) -> Result<isize, RpcError<()>>;

    async fn div(&self, args: (isize, isize)) -> Result<isize, RpcError<String>>;
}

#[endpoint_async(EchoClient)]
pub trait EchoService {
    fn repeat(&self, msg: String) -> impl Future<Output = Result<String, RpcError<()>>> + Send;

    fn io_error(&self, _msg: String) -> impl Future<Output = Result<(), RpcError<Errno>>> + Send;
}
