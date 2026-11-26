//! The struct that implements a timer for ClientTask
//!
//! This module is only for transport implementation, not for the user.

use std::{
    collections::vec_deque::VecDeque, future::Future, mem::swap, ops::Add, pin::Pin,
    sync::atomic::Ordering::Release, task::*,
};

use super::{stream::ClientStreamInner, task::ClientTaskDone, *};
use crossfire::{AsyncTx, spsc, stream::AsyncStream, waitgroup::WaitGroupGuard};
use embed_btree::VariousMap;
use seqnum::SeqU32;

pub struct ClientTaskItem<T: ClientTask> {
    pub task: T,
    _wg: WaitGroupGuard<()>,
}

struct TasksBatch<T: ClientTask> {
    tasks: VariousMap<u32, ClientTaskItem<T>>,
    max_seq: SeqU32,
    ts: SeqU32,
}

/* NOTE:
 * The channel, Ok(task) is a normal task, Err(u32) is the seq of ping msg.
 * We need to maintain the seq in the TasksBatch to get the global min_seq right
 */

pub(super) struct TaskRegistry<F: ClientFacts> {
    pending_tasks_recv: AsyncStream<spsc::Array<Result<ClientTaskItem<F::Task>, u32>>>,
    cur_batch: VariousMap<u32, ClientTaskItem<F::Task>>, // sent tasks of the current second
    last_batch_ts: u32,
    delay_tasks_queue: VecDeque<TasksBatch<F::Task>>, // sent tasks of past seconds
    // sum of cur_batch and delay_tasks_queue
    pub stash_count: usize,
    processed_seq: SeqU32,
    task_timeout: u16,
}

pub(super) struct TaskRegistryTx<F: ClientFacts>(
    AsyncTx<spsc::Array<Result<ClientTaskItem<F::Task>, u32>>>,
);

impl<F: ClientFacts> TaskRegistryTx<F> {
    #[inline]
    pub async fn reg_task(&mut self, task: F::Task, _wg: WaitGroupGuard<()>) {
        // NOTE: we have to make sure receiver side never close first
        self.0.send(Ok(ClientTaskItem { task, _wg })).await.expect("send");
    }

    pub async fn send_ping(&mut self, seq: u32) {
        self.0.send(Err(seq)).await.expect("send");
    }
}

impl<F: ClientFacts> TaskRegistry<F> {
    #[inline]
    pub fn new(task_timeout: u16) -> (TaskRegistryTx<F>, Self) {
        let (pending_tx, pending_rx) =
            spsc::bounded_async::<Result<ClientTaskItem<F::Task>, u32>>(512);
        let recv = Self {
            pending_tasks_recv: pending_rx.into_stream(),
            cur_batch: VariousMap::new(),
            delay_tasks_queue: VecDeque::with_capacity(task_timeout as usize),
            task_timeout,
            processed_seq: 0.into(),
            stash_count: 0,
            last_batch_ts: 0,
        };
        (TaskRegistryTx(pending_tx), recv)
    }

    #[inline]
    pub(crate) async fn clean_pending_tasks(&mut self, facts: &F) {
        let mut temp = VariousMap::new();
        core::mem::swap(&mut self.cur_batch, &mut temp);
        for (_key, task_item) in temp {
            let mut task = task_item.task;
            task.set_rpc_error(RpcIntErr::IO);
            facts.error_handle(task);
        }
        for batch in self.delay_tasks_queue.drain(..) {
            for (_key, task_item) in batch.tasks {
                let mut task = task_item.task;
                task.set_rpc_error(RpcIntErr::IO);
                facts.error_handle(task);
            }
        }
        while let Ok(item) = self.pending_tasks_recv.recv().await {
            if let Ok(item) = item {
                let mut task = item.task;
                task.set_rpc_error(RpcIntErr::IO);
                facts.error_handle(task);
            }
        }
        self.stash_count = 0;
    }

    #[inline]
    pub async fn take_task(&mut self, seq: u32) -> Option<ClientTaskItem<F::Task>> {
        let _seq = SeqU32::from(seq);
        // ping resp won't readh here
        if _seq <= self.processed_seq {
        } else {
            let r = WaitRegTaskFuture { reg: self, target_seq: _seq }.await;
            match r {
                Ok(()) => {}
                Err(()) => return None,
            }
        }
        if let Some(_removed_task) = self.cur_batch.remove(&seq) {
            self.stash_count -= 1;
            return Some(_removed_task);
        }
        for old_batch in self.delay_tasks_queue.iter_mut() {
            if let Some(_task) = old_batch.tasks.remove(&seq) {
                self.stash_count -= 1;
                return Some(_task);
            }
        }
        // Task is already timeouted by us
        return None;
    }

    #[inline]
    pub fn poll_sent_task(&mut self, ctx: &mut Context) -> Poll<Result<(), ()>> {
        match self.pending_tasks_recv.poll_item(ctx) {
            Poll::Ready(Some(task)) => {
                self.got_pending_task(task);
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(Err(())),
        }
    }

    // return None if task is store in sent_tasks
    #[inline]
    fn got_pending_task(&mut self, task_item: Result<ClientTaskItem<F::Task>, u32>) {
        match task_item {
            Ok(mut task_item) => {
                let seq = task_item.task.as_mut().seq;
                self.processed_seq = SeqU32::from(seq);
                self.cur_batch.insert(seq, task_item);
                self.stash_count += 1;
            }
            Err(seq) => {
                self.processed_seq = SeqU32::from(seq);
            }
        }
    }

    pub fn adjust_task_queue<P: ClientTransport>(&mut self, inner: &ClientStreamInner<F, P>) {
        let last_batch_ts = self.last_batch_ts;
        self.last_batch_ts = last_batch_ts.wrapping_add(1);
        if let Some(batch) = self
            .delay_tasks_queue
            .pop_back_if(|b| b.ts.add(self.task_timeout as u32) <= SeqU32::from(self.last_batch_ts))
        {
            inner.min_seq.store(batch.max_seq.0, Release);
            self.stash_count -= batch.tasks.len();
            for (_seq, task_item) in batch.tasks {
                let mut task = task_item.task;
                warn!("{:?} task {:?} is timeout", inner, task);
                task.set_rpc_error(RpcIntErr::Timeout);
                inner.facts.error_handle(task);
            }
        }
        if !self.cur_batch.is_empty() {
            let mut new_batch = VariousMap::new();
            swap(&mut self.cur_batch, &mut new_batch);
            self.delay_tasks_queue.push_front(TasksBatch {
                tasks: new_batch,
                max_seq: self.processed_seq,
                ts: SeqU32::from(last_batch_ts),
            });
        } else if let Some(first_batch) = self.delay_tasks_queue.front_mut() {
            first_batch.max_seq = self.processed_seq;
        } else {
            inner.min_seq.store(self.processed_seq.0, Release);
        }
    }
}

struct WaitRegTaskFuture<'a, F>
where
    F: ClientFacts,
{
    reg: &'a mut TaskRegistry<F>,
    target_seq: SeqU32,
}

impl<'a, F> Future for WaitRegTaskFuture<'a, F>
where
    F: ClientFacts,
{
    type Output = Result<(), ()>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        while this.reg.processed_seq != this.target_seq {
            let r = this.reg.poll_sent_task(ctx);
            if r != Poll::Ready(Ok(())) {
                return r;
            }
        }
        Poll::Ready(Ok(()))
    }
}
