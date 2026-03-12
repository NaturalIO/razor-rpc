//! The struct that implements a timer for ClientTask
//!
//! This module is only for transport implementation, not for the user.

use std::{
    collections::{BTreeMap, vec_deque::VecDeque},
    future::Future,
    mem::swap,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    task::*,
};

use crate::client::task::ClientTaskDone;
use crate::client::*;
use crossfire::{stream::AsyncStream, waitgroup::WaitGroupGuard, *};

pub struct ClientTaskItem<T: ClientTask> {
    pub task: Option<T>,
    _upstream: WaitGroupGuard<()>,
}

pub(crate) struct DelayTasksBatch<T: ClientTask> {
    tasks: BTreeMap<u64, ClientTaskItem<T>>,
}

pub struct ClientTaskTimer<F: ClientFacts> {
    conn_id: String,
    pending_tasks_recv: AsyncStream<mpsc::Array<ClientTaskItem<F::Task>>>,
    pending_tasks_sender: MAsyncTx<mpsc::Array<ClientTaskItem<F::Task>>>,
    pending_task_count: AtomicU64,

    sent_tasks: BTreeMap<u64, ClientTaskItem<F::Task>>, // sent_tasks of the current second
    delay_tasks_queue: VecDeque<DelayTasksBatch<F::Task>>, // sent_tasks of past seconds

    min_delay_seq: u64,
    task_timeout: usize, // in seconds
    // TODO what if seq reach max u64, should exit client
    processed_seq: u64,
    reg_stopped_flag: AtomicBool,
}

unsafe impl<T: ClientFacts> Send for ClientTaskTimer<T> {}
unsafe impl<T: ClientFacts> Sync for ClientTaskTimer<T> {}

impl<F: ClientFacts> ClientTaskTimer<F> {
    pub fn new(conn_id: String, task_timeout: usize, mut thresholds: usize) -> Self {
        if thresholds == 0 {
            thresholds = 500;
        }
        let (pending_tx, pending_rx) = mpsc::bounded_async(thresholds * 2);
        Self {
            conn_id,
            pending_tasks_recv: pending_rx.into_stream(),
            pending_tasks_sender: pending_tx,
            pending_task_count: AtomicU64::new(0),
            sent_tasks: BTreeMap::new(),
            min_delay_seq: 0,
            task_timeout,
            delay_tasks_queue: VecDeque::with_capacity(task_timeout),
            processed_seq: 0,
            reg_stopped_flag: AtomicBool::new(false),
        }
    }

    pub fn pending_task_count_ref(&self) -> &AtomicU64 {
        &self.pending_task_count
    }

    pub fn clean_pending_tasks(&mut self, facts: &F) {
        while let Ok(task) = self.pending_tasks_recv.try_recv() {
            self.got_pending_task(task);
        }
        let mut task_seqs: Vec<u64> = Vec::with_capacity(self.sent_tasks.len());
        for (key, _) in self.sent_tasks.iter() {
            task_seqs.push(*key);
        }
        for key in task_seqs {
            let mut task_item = self.sent_tasks.remove(&key).unwrap();
            let mut task = task_item.task.take().unwrap();
            task.set_rpc_error(RpcIntErr::IO);
            facts.error_handle(task);
        }
        for tasks_batch_in_second in self.delay_tasks_queue.iter_mut() {
            let mut task_seqs: Vec<u64> = Vec::with_capacity(tasks_batch_in_second.tasks.len());
            for (key, _) in tasks_batch_in_second.tasks.iter() {
                task_seqs.push(*key);
            }
            for key in task_seqs {
                let mut task_item = tasks_batch_in_second.tasks.remove(&key).unwrap();
                let mut task = task_item.task.take().unwrap();
                task.set_rpc_error(RpcIntErr::IO);
                facts.error_handle(task);
            }
        }
    }

    pub fn check_pending_tasks_empty(&mut self) -> bool {
        while let Ok(task) = self.pending_tasks_recv.try_recv() {
            self.got_pending_task(task);
        }
        if !self.sent_tasks.is_empty() {
            return false;
        }
        for tasks_batch_in_second in self.delay_tasks_queue.iter() {
            if !tasks_batch_in_second.tasks.is_empty() {
                return false;
            }
        }
        return true;
    }

    // register noti for task.
    #[inline(always)]
    pub async fn reg_task(&self, task: F::Task, wg: WaitGroupGuard<()>) {
        let _ = self
            .pending_tasks_sender
            .send(ClientTaskItem { task: Some(task), _upstream: wg })
            .await;
    }

    // stop register.
    pub fn stop_reg_task(&mut self) {
        self.reg_stopped_flag.store(true, Ordering::SeqCst);
    }

    pub async fn take_task(&mut self, seq: u64) -> Option<ClientTaskItem<F::Task>> {
        // ping resp won't readh here
        if seq < self.min_delay_seq {
            return None; // Task is already timeouted by us
        }
        if seq > self.processed_seq {
            let f = WaitRegTaskFuture { noti: self, target_seq: seq };
            if f.await.is_err() {
                return None;
            }
        }

        if let Some(_removed_task) = self.sent_tasks.remove(&seq) {
            return Some(_removed_task);
        }
        for tasks_batch_in_second in self.delay_tasks_queue.iter_mut() {
            if let Some(_task) = tasks_batch_in_second.tasks.remove(&seq) {
                return Some(_task);
            }
        }
        return None;
    }

    #[inline]
    pub fn poll_sent_task(&mut self, ctx: &mut Context) -> bool {
        let mut got = false;
        // Need to poll_item in order to register waker
        while let Poll::Ready(Some(_task)) = self.pending_tasks_recv.poll_item(ctx) {
            self.got_pending_task(_task);
            got = true;
        }
        // break on empty or disconnect
        got
    }

    // return None if task is store in sent_tasks
    #[inline]
    fn got_pending_task(&mut self, task_item: ClientTaskItem<F::Task>) {
        self.pending_task_count.fetch_sub(1, Ordering::SeqCst);
        let t = task_item.task.as_ref().unwrap();
        let task_seq = t.seq();
        self.processed_seq = task_seq;
        self.sent_tasks.insert(task_seq, task_item);
    }

    pub fn adjust_task_queue(&mut self, facts: &F) {
        // 1. move wait_confirmed to overtime
        let mut tasks_batch_in_second = BTreeMap::new();
        swap(&mut self.sent_tasks, &mut tasks_batch_in_second);

        // 2.notify req with timeout err
        self.delay_tasks_queue.push_front(DelayTasksBatch { tasks: tasks_batch_in_second });

        if self.delay_tasks_queue.len() > self.task_timeout {
            let real_timeout = self.delay_tasks_queue.pop_back().unwrap();
            if !real_timeout.tasks.is_empty() {
                let mut min_seq = 0;
                for (_seq, mut task_item) in real_timeout.tasks {
                    let mut task = task_item.task.take().unwrap();
                    let seq = task.seq();
                    if min_seq == 0 || min_seq > seq {
                        min_seq = seq;
                    }
                    warn!("{} task {:?} is timeout", self.conn_id, task,);
                    task.set_rpc_error(RpcIntErr::Timeout);
                    facts.error_handle(task);
                }
                self.min_delay_seq = min_seq;
            }
        }
    }
}

struct WaitRegTaskFuture<'a, F>
where
    F: ClientFacts,
{
    noti: &'a mut ClientTaskTimer<F>,
    target_seq: u64,
}

impl<'a, F> Future for WaitRegTaskFuture<'a, F>
where
    F: ClientFacts,
{
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        if _self.noti.processed_seq >= _self.target_seq {
            return Poll::Ready(Ok(()));
        }
        if _self.noti.reg_stopped_flag.load(Ordering::SeqCst) {
            return Poll::Ready(Err(()));
        }
        if _self.noti.poll_sent_task(ctx) && _self.noti.processed_seq >= _self.target_seq {
            return Poll::Ready(Ok(()));
        }
        Poll::Pending
    }
}
