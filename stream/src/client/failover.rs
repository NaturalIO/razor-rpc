use crate::client::task::*;
use crate::client::{
    ClientCaller, ClientCallerBlocking, ClientConfig, ClientFacts, ClientPool, ClientTransport,
};
use crate::proto::RpcAction;
use crate::{
    Codec,
    error::{EncodedErr, RpcIntErr},
};
use arc_swap::ArcSwap;
use captains_log::filter::LogFilter;
use crossfire::{AsyncRx, MTx, SendError, mpsc};
use std::fmt;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// A pool supports failover to multiple address with bias or round_robin strategy
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
    round_robin: bool,
    ver: AtomicU64,
    rr_counter: AtomicUsize,
    pool_channel_size: usize,
    facts: Arc<FailoverFacts<F>>,
}

struct ClusterConfig<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    pools: Vec<ClientPool<FailoverFacts<F>, P>>,
    ver: u64,
}

impl<F, P> FailoverPool<F, P>
where
    F: ClientFacts,
    P: ClientTransport,
{
    /// Initiate the pool with multiple address.
    /// When round_robin == true, all address in the pool will be select with equal chanced;
    /// When round_robin == false, the first address will always be pick unless error happens.
    pub fn new(
        facts: Arc<F>, addrs: Vec<String>, round_robin: bool, retry_limit: usize,
        pool_channel_size: usize,
    ) -> Self {
        let (retry_tx, retry_rx) = mpsc::unbounded_async();
        let retry_logger = facts.new_logger();
        let wrapped_facts =
            Arc::new(FailoverFacts { retry_limit, retry_tx, logger: facts.new_logger(), facts });
        let mut pools = Vec::with_capacity(addrs.len());
        for addr in addrs.iter() {
            let pool = ClientPool::new(wrapped_facts.clone(), addr, pool_channel_size);
            pools.push(pool);
        }
        // NOTE: the ClientPool has cycle reference with FailoverPoolInner
        let inner = Arc::new(FailoverPoolInner::<F, P> {
            pools: ArcSwap::new(Arc::new(ClusterConfig { ver: 0, pools })),
            round_robin,
            facts: wrapped_facts,
            ver: AtomicU64::new(1),
            rr_counter: AtomicUsize::new(0),
            pool_channel_size,
        });
        let weak_self = Arc::downgrade(&inner);
        inner.facts.spawn_detach(async move {
            FailoverPoolInner::retry_worker(weak_self, retry_logger, retry_rx).await;
        });
        Self { inner }
    }

    pub fn update_addrs(&self, addrs: Vec<String>) {
        let inner = &self.inner;
        let old_pools = inner.pools.load_full();
        let mut new_pools: Vec<ClientPool<FailoverFacts<F>, P>> = Vec::with_capacity(addrs.len());

        let mut old_pools_map = std::collections::HashMap::with_capacity(old_pools.pools.len());
        for pool in &old_pools.pools {
            old_pools_map.insert(pool.get_addr().to_string(), pool);
        }

        for addr in addrs {
            if let Some(reused_pool) = old_pools_map.remove(&addr) {
                new_pools.push(reused_pool.clone());
            } else {
                // Create a new pool for the new address
                let new_pool = ClientPool::new(inner.facts.clone(), &addr, inner.pool_channel_size);
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
    #[inline]
    fn select(
        &self, round_robin: bool, rr_counter: &AtomicUsize, last_index: Option<usize>,
    ) -> Option<(usize, &ClientPool<FailoverFacts<F>, P>)> {
        let l = self.pools.len();
        if l == 0 {
            return None;
        }
        let seed = if let Some(index) = last_index {
            // last_index is address with error occurs
            index + 1
        } else if round_robin {
            rr_counter.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        };
        for i in seed..seed + l {
            let pool = &self.pools[i % l];
            if pool.is_healthy() {
                return Some((i, pool));
            }
        }
        return None;
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
                // if cluster config changed, restart selection
                let last_index = if cluster.ver == task.cluster_ver {
                    Some(task.last_index)
                } else {
                    task.cluster_ver = cluster.ver;
                    None // restart selection
                };
                if let Some((index, pool)) =
                    cluster.select(inner.round_robin, &inner.rr_counter, last_index)
                {
                    if let Some(last) = last_index {
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
        if task.should_retry && task.retry <= self.retry_limit {
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
        if let Some((index, pool)) =
            cluster.select(self.inner.round_robin, &self.inner.rr_counter, None)
        {
            let failover_task = FailoverTask {
                last_index: index,
                cluster_ver: cluster.ver,
                inner: task,
                retry: 0,
                should_retry: false,
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
        if let Some((index, pool)) =
            cluster.select(self.inner.round_robin, &self.inner.rr_counter, None)
        {
            let failover_task = FailoverTask {
                last_index: index,
                cluster_ver: cluster.ver,
                inner: task,
                retry: 0,
                should_retry: false,
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
    cluster_ver: u64,
    inner: T,
    retry: usize,
    should_retry: bool,
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
    fn set_custom_error<C: Codec>(&mut self, codec: &C, e: EncodedErr) {
        self.should_retry = false;
        self.inner.set_custom_error(codec, e);
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
