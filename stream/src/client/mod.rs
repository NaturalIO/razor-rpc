//! The module contains traits defined for the client-side

use crate::{Codec, error::RpcIntErr};
use captains_log::filter::LogFilter;
use crossfire::{AsyncRx, mpsc};
use orb::AsyncRuntime;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fmt, io};

pub mod task;
use task::{ClientTask, ClientTaskDone};

pub mod stream;
pub mod timer;
use timer::ClientTaskTimer;

mod pool;
pub use pool::ConnPool;
mod failover;
pub use failover::FailoverPool;

mod throttler;

/// General config for client-side
#[derive(Clone)]
pub struct ClientConfig {
    /// timeout of RpcTask waiting for response, in seconds.
    pub task_timeout: usize,
    /// socket read timeout
    pub read_timeout: Duration,
    /// Socket write timeout
    pub write_timeout: Duration,
    /// Socket idle time to be close. for connection pool.
    pub idle_timeout: Duration,
    /// connect timeout
    pub connect_timeout: Duration,
    /// How many async RpcTask in the queue, prevent overflow server capacity
    pub thresholds: usize,
    /// In bytes. when non-zero, overwrite the default DEFAULT_BUF_SIZE of transport
    pub stream_buf_size: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            task_timeout: 20,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(10),
            thresholds: 128,
            stream_buf_size: 0,
        }
    }
}

/// A trait implemented by the user for the client-side, to define the customizable plugin.
pub trait ClientFacts: Send + Sync + Sized + 'static {
    /// Define the codec to serialization and deserialization
    ///
    /// Refers to [Codec]
    type Codec: Codec;

    /// Define the RPC task from client-side
    ///
    /// Either one ClientTask or an enum of multiple ClientTask.
    /// If you have multiple task type, recommend to use the `enum_dispatch` crate.
    ///
    /// You can use macro [client_task_enum](crate::client::task::client_task_enum) and [client_task](crate::client::task::client_task) on task type
    type Task: ClientTask;

    /// You should keep ClientConfig inside, get_config() will return the reference.
    fn get_config(&self) -> &ClientConfig;

    /// Construct a [captains_log::filter::Filter](https://docs.rs/captains-log/latest/captains_log/filter/trait.Filter.html) to oganize log of a client
    ///
    /// TODO: Fix the logger interface
    fn new_logger(&self) -> Arc<LogFilter>;

    /// How to deal with error
    ///
    /// The FailoverPool will overwrite this to implement retry logic
    #[inline(always)]
    fn error_handle(&self, task: Self::Task) {
        task.done();
    }

    /// You can overwrite this to assign a client_id
    #[inline(always)]
    fn get_client_id(&self) -> u64 {
        0
    }

    /// NOTE: you may overwrite this to use coarstime or quanta
    #[inline]
    fn get_timestamp(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}

/// A trait to support sending request task in async text, for all router and connection pool
/// implementations
pub trait ClientCaller: Send {
    type Facts: ClientFacts;
    fn send_req(&self, task: <Self::Facts as ClientFacts>::Task)
    -> impl Future<Output = ()> + Send;

    fn get_codec(&self) -> <Self::Facts as ClientFacts>::Codec {
        <Self::Facts as ClientFacts>::Codec::default()
    }
}

/// A trait to support sending request task in blocking text, for all router and connection pool
/// implementations
pub trait ClientCallerBlocking: Send {
    type Facts: ClientFacts;
    fn send_req_blocking(&self, task: <Self::Facts as ClientFacts>::Task);

    fn get_codec(&self) -> <Self::Facts as ClientFacts>::Codec {
        <Self::Facts as ClientFacts>::Codec::default()
    }
}

impl<C: ClientCaller + Send + Sync> ClientCaller for Arc<C> {
    type Facts = C::Facts;

    #[inline(always)]
    async fn send_req(&self, task: <Self::Facts as ClientFacts>::Task) {
        self.as_ref().send_req(task).await
    }
}

impl<C: ClientCallerBlocking + Send + Sync> ClientCallerBlocking for Arc<C> {
    type Facts = C::Facts;

    #[inline(always)]
    fn send_req_blocking(&self, task: <Self::Facts as ClientFacts>::Task) {
        self.as_ref().send_req_blocking(task);
    }
}

/// This trait is for client-side transport layer protocol.
///
/// The implementation can be found on:
///
/// - [razor-rpc-tcp](https://docs.rs/razor-rpc-tcp): For TCP and Unix socket
pub trait ClientTransport: fmt::Debug + Send + Sized + 'static {
    type RT: AsyncRuntime + Clone;

    /// How to establish an async connection.
    ///
    /// conn_id: used for log fmt, can by the same of addr.
    fn connect(
        addr: &str, conn_id: &str, config: &ClientConfig,
    ) -> impl Future<Output = Result<Self, RpcIntErr>> + Send;

    /// Shutdown the write direction of the connection
    fn close_conn<F: ClientFacts>(&self, logger: &LogFilter) -> impl Future<Output = ()> + Send;

    /// Flush the request for the socket writer, if the transport has buffering logic
    fn flush_req<F: ClientFacts>(
        &self, logger: &LogFilter,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Write out the encoded request task
    fn write_req<'a, F: ClientFacts>(
        &'a self, logger: &LogFilter, buf: &'a [u8], blob: Option<&'a [u8]>, need_flush: bool,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Read the response and decode it from the socket, find and notify the registered ClientTask
    fn read_resp<F: ClientFacts>(
        &self, facts: &F, logger: &LogFilter, codec: &F::Codec,
        close_ch: Option<&mut AsyncRx<mpsc::Null>>, task_reg: &mut ClientTaskTimer<F>,
    ) -> impl std::future::Future<Output = Result<bool, RpcIntErr>> + Send;
}

/// An example ClientFacts for general use
pub struct ClientDefault<T: ClientTask, C: Codec> {
    pub logger: Arc<LogFilter>,
    config: ClientConfig,
    _phan: std::marker::PhantomData<fn(&C, &T)>,
}

impl<T: ClientTask, C: Codec> ClientDefault<T, C> {
    pub fn new(config: ClientConfig) -> Arc<Self> {
        Arc::new(Self { logger: Arc::new(LogFilter::new()), config, _phan: Default::default() })
    }

    #[inline]
    pub fn set_log_level(&self, level: log::Level) {
        self.logger.set_level(level);
    }
}

impl<T: ClientTask, C: Codec> ClientFacts for ClientDefault<T, C> {
    type Codec = C;
    type Task = T;

    #[inline]
    fn new_logger(&self) -> Arc<LogFilter> {
        self.logger.clone()
    }

    #[inline]
    fn get_config(&self) -> &ClientConfig {
        &self.config
    }
}
