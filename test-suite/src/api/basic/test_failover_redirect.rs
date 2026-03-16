use orb::prelude::AsyncExec;
use parking_lot::Mutex;
use razor_rpc::client::{APIFact, ClientConfig, endpoint_async, endpoint_client};
use razor_rpc::error::{EncodedErr, RpcErrCodec, RpcError};
use razor_rpc::server::dispatch::Inline;
use razor_rpc::server::{ServerConfig, ServerDefault, service};
use razor_rpc_codec::{Codec, MsgpCodec};
use razor_rpc_tcp::{TcpClient, TcpServer};
use razor_stream::server::RpcServer;
use rstest::*;
use serde_derive::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

use crate::{TestRunner, logfn, runner};

// =============================================================================
// Custom Error Type for Cluster Operations
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ClusterErr {
    /// Redirect to a specific address (for leader-follower model)
    Redirect(String),
    /// Retry to next node (e.g., node shutting down, not leader, etc.)
    RetryNext,
    /// Internal error, don't retry
    Internal,
}

impl RpcErrCodec for ClusterErr {
    #[inline(always)]
    fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
        match self {
            Self::Redirect(addr) => {
                let s = format!("redirect_{}", addr);
                EncodedErr::Buf(s.into_bytes())
            }
            Self::RetryNext => EncodedErr::Static("retry_next"),
            Self::Internal => EncodedErr::Static("internal"),
        }
    }

    #[inline(always)]
    fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
        if let Err(bytes) = buf {
            let s = unsafe { std::str::from_utf8_unchecked(bytes) };
            if s.starts_with("redirect_") {
                Ok(Self::Redirect(s[9..].to_string()))
            } else if s == "retry_next" {
                Ok(Self::RetryNext)
            } else if s == "internal" {
                Ok(Self::Internal)
            } else {
                Err(())
            }
        } else {
            Err(())
        }
    }

    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }

    #[inline(always)]
    fn should_failover(&self) -> Result<Option<&str>, ()> {
        match self {
            // Redirect to specific address
            Self::Redirect(addr) => Ok(Some(addr)),
            // Retry to next node without specific address
            Self::RetryNext => Ok(None),
            // Don't retry for internal errors
            Self::Internal => Err(()),
        }
    }
}

#[test]
fn test_cluster_err() {
    let codec = MsgpCodec::default();

    // Test Redirect error
    let err = ClusterErr::Redirect("192.168.1.100:8080".to_string());
    let encoded = err.encode(&codec);
    if let EncodedErr::Buf(buf) = encoded {
        let decoded = ClusterErr::decode(&codec, Err(&buf)).expect("decode failed");
        assert_eq!(decoded, err);
        assert_eq!(decoded.should_failover(), Ok(Some("192.168.1.100:8080")));
    } else {
        panic!("Expected EncodedErr::Buf for Redirect");
    }

    // Test RetryNext error
    let err = ClusterErr::RetryNext;
    let encoded = err.encode(&codec);
    if let EncodedErr::Static(s) = encoded {
        let decoded = ClusterErr::decode(&codec, Err(s.as_bytes())).expect("decode failed");
        assert_eq!(decoded, err);
        assert_eq!(decoded.should_failover(), Ok(None));
    } else {
        panic!("Expected EncodedErr::Static for RetryNext");
    }

    // Test Internal error
    let err = ClusterErr::Internal;
    let encoded = err.encode(&codec);
    if let EncodedErr::Static(s) = encoded {
        let decoded = ClusterErr::decode(&codec, Err(s.as_bytes())).expect("decode failed");
        assert_eq!(decoded, err);
        assert_eq!(decoded.should_failover(), Err(()));
    } else {
        panic!("Expected EncodedErr::Static for Internal");
    }
}

// =============================================================================
// Cluster State
// =============================================================================

pub struct ClusterState {
    pub leader_index: usize,
    pub addrs: Vec<String>,
}

impl ClusterState {
    pub fn new(addrs: Vec<String>, leader_index: usize) -> Self {
        Self { leader_index, addrs }
    }

    pub fn get_leader_addr(&self) -> String {
        self.addrs[self.leader_index].clone()
    }

    pub fn is_leader(&self, node_index: usize) -> bool {
        self.leader_index == node_index
    }

    pub fn set_leader(&mut self, leader_index: usize) {
        self.leader_index = leader_index;
    }

    pub fn add_addr(&mut self, addr: String) -> usize {
        let index = self.addrs.len();
        self.addrs.push(addr);
        index
    }
}

// =============================================================================
// Test: Redirect to a new address not in initial pool
// =============================================================================

#[derive(Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct QueryResp {
    pub server_id: usize,
}

pub type FailoverCaller = razor_rpc::client::APIFailoverPool<MsgpCodec, TcpClient<crate::RT>>;

impl QueryClient<FailoverCaller> {
    pub fn new_failover_client(config: ClientConfig, addrs: Vec<String>, rt: &crate::RT) -> Self {
        let fact = APIFact::<MsgpCodec>::new(config);
        let pool = fact.new_failover::<TcpClient<crate::RT>>(rt, addrs, false, 3);
        QueryClient::new(pool)
    }
}

// Client for query service
endpoint_client!(QueryClient);

// Service trait with query method
#[endpoint_async(QueryClient)]
#[async_trait::async_trait]
pub trait QueryService {
    async fn query(&self, _req: ()) -> Result<QueryResp, RpcError<ClusterErr>>;

    async fn always_error_query(&self, _req: ()) -> Result<(), RpcError<ClusterErr>>;
}

// Service impl that checks leader and redirects if needed
#[derive(Clone)]
pub struct QueryServiceImpl {
    pub node_index: usize,
    pub state: Arc<Mutex<ClusterState>>,
}

#[service]
#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    async fn query(&self, _req: ()) -> Result<QueryResp, RpcError<ClusterErr>> {
        let state = self.state.lock();
        if !state.is_leader(self.node_index) {
            let leader_addr = state.get_leader_addr();
            return Err(RpcError::User(ClusterErr::Redirect(leader_addr)));
        }
        Ok(QueryResp { server_id: self.node_index })
    }

    async fn always_error_query(&self, _req: ()) -> Result<(), RpcError<ClusterErr>> {
        // Always return RetryNext to trigger retry
        Err(RpcError::User(ClusterErr::RetryNext))
    }
}

/// Start a query server at given bind address
async fn start_query_server<RT: orb::AsyncRuntime + Clone>(
    rt: RT, bind_addr: &str, node_index: usize, state: Arc<Mutex<ClusterState>>,
    server_config: ServerConfig,
) -> (RpcServer<ServerDefault>, String) {
    let mut server = RpcServer::new(ServerDefault::new(server_config));
    let service_impl = QueryServiceImpl { node_index, state };
    let dispatch = Inline::<MsgpCodec, _>::new(service_impl);
    let actual_addr =
        server.listen::<TcpServer<RT>, _>(rt, bind_addr, dispatch).await.expect("server listen");
    (server, actual_addr)
}

/// Start a 3-node query cluster
async fn start_three_node_query_cluster<RT: orb::AsyncRuntime + Clone>(
    rt: RT,
) -> (Arc<Mutex<ClusterState>>, Vec<RpcServer<ServerDefault>>) {
    let server_config = ServerConfig::default();
    let cluster_state = Arc::new(Mutex::new(ClusterState::new(vec![], 0)));
    let mut servers = Vec::new();
    for i in 0..3 {
        let state_clone = cluster_state.clone();
        let rt_clone = rt.clone();
        let addr = "127.0.0.1:0";
        let (server, actual_addr) =
            start_query_server(rt_clone, &addr, i, state_clone, server_config.clone()).await;
        assert_eq!(cluster_state.lock().add_addr(actual_addr), i);
        servers.push(server);
    }
    // Give servers time to fully start
    //RT::sleep(std::time::Duration::from_millis(500)).await;
    (cluster_state, servers)
}

/// Test leader redirect within known addresses (no new address added)
#[logfn]
#[rstest]
fn test_failover_leader_redirect(runner: TestRunner) {
    let rt = runner.rt.clone();
    runner.rt.block_on(async move {
        let client_config = ClientConfig::default();
        let (cluster_state, _servers) = start_three_node_query_cluster(rt.clone()).await;
        let actual_addrs = { cluster_state.lock().addrs.clone() };
        log::debug!("Server addresses: {:?}", actual_addrs);

        // Test 1: Query should reach leader (node 1)
        let client =
            QueryClient::new_failover_client(client_config.clone(), actual_addrs.clone(), &rt);
        let resp = client.query(()).await.expect("query should succeed");
        assert_eq!(resp.server_id, 0, "Query should reach initial leader (node 0)");
        log::info!("Test 1 passed: query reached node 1 (initial leader)");
        // Test 2: Change leader to node 2 and verify redirect works
        cluster_state.lock().set_leader(2);
        log::info!("Leader changed to node 2");

        let resp = client.query(()).await.expect("query should succeed after leader change");
        assert_eq!(resp.server_id, 2, "Query should reach new leader (node 2) after redirect");
        log::info!("Test 2 passed: query reached node 2 (new leader via redirect)");
    });
}

#[logfn]
#[rstest]
fn test_failover_retry_limit(runner: TestRunner) {
    let rt = runner.rt.clone();
    runner.block_on(async move {
        let client_config = ClientConfig::default();
        let (cluster_state, _servers) = start_three_node_query_cluster(rt.clone()).await;
        let actual_addrs = { cluster_state.lock().addrs.clone() };
        log::debug!("Server addresses: {:?}", actual_addrs);

        let client =
            QueryClient::new_failover_client(client_config.clone(), actual_addrs.clone(), &rt);
        let resp = client.always_error_query(()).await;
        // Should fail because all retries exhausted
        assert_eq!(resp.unwrap_err(), RpcError::User(ClusterErr::RetryNext));
        log::info!("Retry limit test passed: query failed as expected after retries");
    });
}

#[logfn]
#[rstest]
fn test_redirect_to_new_address(runner: TestRunner) {
    let rt = runner.rt.clone();
    runner.rt.block_on(async move {
        let client_config = ClientConfig::default();
        let (cluster_state, mut _servers) = start_three_node_query_cluster(rt.clone()).await;
        let actual_addrs = { cluster_state.lock().addrs.clone() };
        log::debug!("Server addresses: {:?}", actual_addrs);

        let client =
            QueryClient::new_failover_client(client_config.clone(), actual_addrs.clone(), &rt);

        // Test 1: Query should reach leader (node 1)
        let resp = client.query(()).await.expect("query should succeed");
        assert_eq!(resp.server_id, 0, "query should reach initial leader (node 0)");

        // Test 2: start server3 and change leader to it
        let (server3, actual_addr3) = start_query_server(
            rt.clone(),
            "127.0.0.1:0",
            3,
            cluster_state.clone(),
            ServerConfig::default(),
        )
        .await;

        assert_eq!(cluster_state.lock().add_addr(actual_addr3), 3);
        _servers.push(server3);
        cluster_state.lock().set_leader(3);

        let resp = client.query(()).await.expect("query should succeed");
        assert_eq!(resp.server_id, 3, "query should reach initial leader (node 3)");
        // again
        let resp = client.query(()).await.expect("query should succeed");
        assert_eq!(resp.server_id, 3, "query should reach initial leader (node 3)");
    });
}
