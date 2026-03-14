pub use super::service::{CalClient, EchoClient};
use razor_rpc::Codec;
use razor_rpc::client::*;
use razor_rpc_tcp::TcpClient;

pub type APIClient<C> = razor_stream::client::ClientDefault<APIClientReq, C>;

pub type PoolCaller<C> = ClientPool<APIClient<C>, TcpClient<crate::RT>>;

pub struct MyClient<C: Codec> {
    pub cal: CalClient<PoolCaller<C>>,
    pub echo: EchoClient<PoolCaller<C>>,
}

impl<C: Codec> MyClient<C> {
    pub fn new(config: ClientConfig, addr: &str, rt: &crate::RT) -> Self {
        // NOTE: Do not new rt to the client, pass a handle from TestRunner.
        // since client may be drop by test logic, it's not allow
        // to drop a tokio runtime inside async code.

        let facts = APIClient::<C>::new(config);
        let pool = facts.clone().create_pool_async::<TcpClient<crate::RT>, crate::RT>(rt, addr);
        let cal = CalClient::new(pool.clone());
        let echo = EchoClient::new(pool.clone());
        MyClient { cal, echo }
    }
}
