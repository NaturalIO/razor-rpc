use crate::client::task::*;
use crate::client::{
    ClientCaller, ClientCallerBlocking, ClientConfig, ClientFacts, ClientTransport, ConnPool,
};
use crate::proto::RpcAction;
use crate::{
    Codec,
    error::{EncodedErr, RpcIntErr},
};
use ahash::AHashMap;
use arc_swap::ArcSwap;
use captains_log::filter::LogFilter;
use crossfire::{AsyncRx, MTx, SendError, mpsc};
use orb::AsyncRuntime;
use std::fmt;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// A pool supports failover to multiple addresses with stateless (round-robin) or stateful (leader-based) strategy
///
/// Supports async and blocking context.
///
/// Only retry RpcIntErr that less than RpcIntErr::Method,
/// currently ignore custom error due to complexity of generic.
/// (If you need to custom failover logic, copy the code and impl your own pool.)
pub struct FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    inner: Arc<FailoverPoolInner<F, P>>,
}

struct FailoverFacts<F>
where
    F: ClientFacts,
{
    retry_tx: MTx<mpsc::List<FailoverTask<F::Task>>>,
    facts: Arc<F>,
    logger: Arc<LogFilter>,
    retry_limit: usize,
}

struct FailoverPoolInner<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    pools: ArcSwap<ClusterConfig<F, P>>,
    stateless: bool,
    ver: AtomicU64,
    /// Next node index for routing:
    /// - In stateless mode: used for round-robin selection
    /// - In stateful mode: used as the current leader index
    next_node: AtomicUsize,
    pool_channel_size: usize,
    facts: Arc<FailoverFacts<F>>,
}

struct ClusterConfig<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    pools: Vec<ConnPool<FailoverFacts<F>, P>>,
    ver: u64,
}

impl<F, P> FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    /// Initiate the pool with multiple addresses.
    /// When stateless == true, all addresses in the pool will be selected with equal chance (round-robin);
    /// When stateless == false, the leader address will always be picked unless error happens.
    pub fn new<RT: AsyncRuntime + Clone>(
        facts: Arc<F>, rt: &RT, addrs: Vec<String>, stateless: bool, retry_limit: usize,
        pool_channel_size: usize,
    ) -> Self {
        let (retry_tx, retry_rx) = mpsc::unbounded_async();
        let retry_logger = facts.new_logger();
        let wrapped_facts =
            Arc::new(FailoverFacts { retry_limit, retry_tx, logger: facts.new_logger(), facts });
        let mut pools = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            let pool = ConnPool::new::<RT>(wrapped_facts.clone(), rt, addr, pool_channel_size);
            pools.push(pool);
        }
        // NOTE: the ConnPool has cycle reference with FailoverPoolInner
        let inner = Arc::new(FailoverPoolInner::<F, P> {
            pools: ArcSwap::new(Arc::new(ClusterConfig { ver: 0, pools })),
            stateless,
            facts: wrapped_facts,
            ver: AtomicU64::new(1),
            next_node: AtomicUsize::new(0),
            pool_channel_size,
        });
        let weak_self = Arc::downgrade(&inner);
        rt.spawn_detach(async move {
            FailoverPoolInner::retry_worker(weak_self, retry_logger, retry_rx).await;
        });
        Self { inner }
    }

    /// Get the retry limit for redirect operations
    #[inline]
    pub fn get_retry_limit(&self) -> usize {
        self.inner.facts.retry_limit
    }

    /// Resubmit a request for retry with optional specific address.
    /// Called by APIClientCaller when should_failover returns Ok(_).
    ///
    /// NOTE: max_retries is currently not used yet (TODO api interface)
    pub async fn resubmit(
        &self, task: F::Task, addr_or_retry: Result<String, usize>, retry_count: usize,
        max_retries: Option<usize>,
    ) where
        F::Task: ClientTask,
    {
        // If specific address provided, try to find matching pool
        match &addr_or_retry {
            Ok(addr) => {
                let (pool, index, conf_ver) = self.get_or_add_addr(addr);
                // TODO should add addr to the pool on-the-fly if not exists
                // Update next_node (leader in stateful mode)
                self.inner.next_node.store(index, Ordering::SeqCst);
                let failover_task = FailoverTask {
                    last_index: index,
                    config_ver: conf_ver,
                    inner: task,
                    retry: retry_count,
                    should_retry: false,
                    max_retries: max_retries.unwrap_or(0),
                };
                pool.send_req(failover_task).await;
                return;
            }
            Err(last_index) => {
                let cluster = self.inner.pools.load();
                // Fallback to select next node
                if let Some((pool, index)) = cluster.select(self.inner.stateless, Err(*last_index))
                {
                    let failover_task = FailoverTask {
                        last_index: index,
                        config_ver: cluster.ver,
                        inner: task,
                        retry: 0,
                        should_retry: false,
                        max_retries: 0,
                    };
                    pool.send_req(failover_task).await;
                    return;
                }
                // No pools available
                let mut task = task;
                task.set_rpc_error(RpcIntErr::Unreachable);
                task.done();
            }
        }
    }

    // return pool, idx, config_ver
    fn get_or_add_addr(&self, addr: &str) -> (ConnPool<FailoverFacts<F>, P>, usize, u64) {
        let cluster = self.inner.pools.load();
        if let Some((pool, idx)) = cluster.get_by_addr(addr) {
            return (pool.clone(), idx, cluster.ver);
        } else {
            // to add new ConnPool, we need runtime
            todo!();
        }
    }

    pub fn update_addrs<RT: AsyncRuntime + Clone>(&self, addrs: Vec<String>, rt: &RT) {
        let inner = &self.inner;
        let old_pools = inner.pools.load_full();
        let mut new_pools: Vec<ConnPool<FailoverFacts<F>, P>> = Vec::with_capacity(addrs.len());

        let mut old_pools_map = AHashMap::with_capacity(old_pools.pools.len());
        for pool in &old_pools.pools {
            old_pools_map.insert(pool.get_addr().to_string(), pool);
        }

        for addr in addrs {
            if let Some(reused_pool) = old_pools_map.remove(&addr) {
                new_pools.push(reused_pool.clone());
            } else {
                // Create a new pool for the new address
                let new_pool =
                    ConnPool::new::<RT>(inner.facts.clone(), rt, &addr, inner.pool_channel_size);
                new_pools.push(new_pool);
            }
        }
        let new_ver = inner.ver.fetch_add(1, Ordering::Relaxed) + 1;
        let new_cluster = ClusterConfig { pools: new_pools, ver: new_ver };
        inner.pools.store(Arc::new(new_cluster));
    }
}

impl<F, P> ClusterConfig<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    /// Select a pool based on routing strategy
    /// - stateless=true: round-robin selection using rr_counter
    /// - stateless=false: leader-based selection, fallback to round-robin if leader unhealthy
    /// - route: Ok(next_node), Err(last_index)
    #[inline]
    fn select(
        &self, stateless: bool, route: Result<&AtomicUsize, usize>,
    ) -> Option<(&ConnPool<FailoverFacts<F>, P>, usize)> {
        let l = self.pools.len();
        if l == 0 {
            return None;
        }
        // TODO should compare config version (if version is changed, the order or addr of the pool
        // might be changed)
        let seed = match &route {
            Err(index) => *index + 1, // try next backup node
            Ok(next_node) => {
                // first time
                if stateless {
                    // round-robin
                    next_node.fetch_add(1, Ordering::Relaxed)
                } else {
                    next_node.load(Ordering::SeqCst)
                }
            }
        };
        for i in seed..seed + l {
            let pool = &self.pools[i % l];
            if pool.is_healthy() {
                return Some((pool, i));
            }
        }
        None
    }

    /// Find pool by address
    fn get_by_addr(&self, addr: &str) -> Option<(&ConnPool<FailoverFacts<F>, P>, usize)> {
        for (i, pool) in self.pools.iter().enumerate() {
            if pool.get_addr() == addr {
                // NOTE: we ignore pool healthy state, we don't know the knowledge of server is
                // more up to day than the client, just try it.
                return Some((pool, i));
            }
        }
        None
    }
}

impl<F, P> FailoverPoolInner<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    async fn retry_worker(
        weak_self: Weak<Self>, logger: Arc<LogFilter>,
        retry_rx: AsyncRx<mpsc::List<FailoverTask<F::Task>>>,
    ) {
        while let Ok(mut task) = retry_rx.recv().await {
            if let Some(inner) = weak_self.upgrade() {
                let cluster = inner.pools.load();
                let route = if cluster.ver == task.config_ver {
                    Err(task.last_index)
                } else {
                    // if cluster config changed (outside source update the address or leader
                    // changed), restart selection
                    task.config_ver = cluster.ver;
                    Ok(&inner.next_node) // restart selection
                };
                if let Some((pool, index)) = cluster.select(inner.stateless, route) {
                    if let Err(last) = &route {
                        logger_trace!(
                            logger,
                            "FailoverPool: task {:?} retry {}->{}",
                            task.inner,
                            last,
                            index
                        );
                    }
                    task.last_index = index;
                    pool.send_req(task).await; // retry is async
                    continue;
                }
                // if we are here, something is wrong, no pool available or selection failed
                logger_debug!(logger, "FailoverPool: no next hoop for {:?}", task.inner);
                task.done();
            } else {
                logger_trace!(logger, "FailoverPool: skip {:?} due to drop", task.inner);
                task.done();
            }
        }
        logger_trace!(logger, "FailoverPool retry worker exit");
    }
}

impl<F, P> Drop for FailoverPoolInner<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    #[inline]
    fn drop(&mut self) {
        logger_trace!(self.facts.logger, "FailoverPool dropped");
    }
}

/// `orb::AsyncRuntime` will follow deref to blanket impl it for wrapper types
impl<F> std::ops::Deref for FailoverFacts<F>
where
    F: ClientFacts,
{
    type Target = F;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.facts.as_ref()
    }
}

impl<F> ClientFacts for FailoverFacts<F>
where
    F: ClientFacts,
{
    type Codec = F::Codec;

    type Task = FailoverTask<F::Task>;

    #[inline]
    fn new_logger(&self) -> Arc<LogFilter> {
        self.facts.new_logger()
    }

    #[inline]
    fn get_config(&self) -> &ClientConfig {
        self.facts.get_config()
    }

    #[inline]
    fn error_handle(&self, task: FailoverTask<F::Task>) {
        // Use the max_retries from the task if set (non-zero), otherwise use the default retry_limit
        let retry_limit = if task.max_retries > 0 { task.max_retries } else { self.retry_limit };
        if task.should_retry && task.retry <= retry_limit {
            if let Err(SendError(_task)) = self.retry_tx.send(task) {
                _task.done();
            }
            return;
        }
        task.inner.done();
    }
}

impl<F, P> Clone for FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    #[inline]
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<F, P> ClientCaller for FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    type Facts = F;

    async fn send_req(&self, mut task: F::Task) {
        let cluster = self.inner.pools.load();
        if let Some((pool, index)) = cluster.select(self.inner.stateless, Ok(&self.inner.next_node))
        {
            let failover_task = FailoverTask {
                last_index: index,
                config_ver: cluster.ver,
                inner: task,
                retry: 0,
                should_retry: false,
                max_retries: 0, // Use default retry_limit from FailoverFacts
            };
            pool.send_req(failover_task).await;
            return;
        }

        // No pools available
        task.set_rpc_error(RpcIntErr::Unreachable);
        task.done();
    }
}

impl<F, P> ClientCallerBlocking for FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    type Facts = F;
    fn send_req_blocking(&self, mut task: F::Task) {
        let cluster = self.inner.pools.load();
        if let Some((pool, index)) = cluster.select(self.inner.stateless, Ok(&self.inner.next_node))
        {
            let failover_task = FailoverTask {
                last_index: index,
                config_ver: cluster.ver,
                inner: task,
                retry: 0,
                should_retry: false,
                max_retries: 0, // Use default retry_limit from FailoverFacts
            };
            pool.send_req_blocking(failover_task);
            return;
        }

        // No pools available
        task.set_rpc_error(RpcIntErr::Unreachable);
        task.done();
    }
}

pub struct FailoverTask<T: ClientTask> {
    last_index: usize,
    config_ver: u64,
    inner: T,
    retry: usize,
    should_retry: bool,
    /// default to be 0, only set by resubmit with custom value for specified interface
    max_retries: usize,
}

impl<T: ClientTask> ClientTaskEncode for FailoverTask<T> {
    #[inline(always)]
    fn encode_req<C: Codec>(&self, codec: &C, buf: &mut Vec<u8>) -> Result<usize, ()> {
        self.inner.encode_req(codec, buf)
    }

    #[inline(always)]
    fn get_req_blob(&self) -> Option<&[u8]> {
        self.inner.get_req_blob()
    }
}

impl<T: ClientTask> ClientTaskDecode for FailoverTask<T> {
    #[inline(always)]
    fn decode_resp<C: Codec>(&mut self, codec: &C, buf: &[u8]) -> Result<(), ()> {
        self.inner.decode_resp(codec, buf)
    }

    #[inline(always)]
    fn reserve_resp_blob(&mut self, _size: i32) -> Option<&mut [u8]> {
        self.inner.reserve_resp_blob(_size)
    }
}

impl<T: ClientTask> ClientTaskDone for FailoverTask<T> {
    #[inline(always)]
    fn set_custom_error<C: Codec>(
        &mut self, codec: &C, e: EncodedErr, _last_index: usize, _conf_ver: u64,
    ) {
        self.should_retry = false;
        self.inner.set_custom_error(codec, e, self.last_index, self.config_ver);
    }

    #[inline(always)]
    fn set_rpc_error(&mut self, e: RpcIntErr) {
        if e < RpcIntErr::Method {
            self.should_retry = true;
            self.retry += 1;
        } else {
            self.should_retry = false;
        }
        self.inner.set_rpc_error(e.clone());
    }

    #[inline(always)]
    fn set_ok(&mut self) {
        self.inner.set_ok();
    }

    #[inline(always)]
    fn done(self) {
        self.inner.done();
    }
}

impl<T: ClientTask> ClientTaskAction for FailoverTask<T> {
    #[inline(always)]
    fn get_action<'a>(&'a self) -> RpcAction<'a> {
        self.inner.get_action()
    }
}

impl<T: ClientTask> std::ops::Deref for FailoverTask<T> {
    type Target = ClientTaskCommon;
    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T: ClientTask> std::ops::DerefMut for FailoverTask<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

impl<T: ClientTask> ClientTask for FailoverTask<T> {}

impl<T: ClientTask> fmt::Debug for FailoverTask<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}
