//! This module contains traits defined for the server-side
//!

use crate::proto::RpcAction;
use crate::{Codec, error::*};
use captains_log::filter::LogFilter;
use crossfire::{MAsyncRx, mpmc};
use io_buffer::Buffer;
use orb::prelude::*;
use std::time::Duration;
use std::{fmt, future::Future, io, sync::Arc};

pub mod task;
use task::*;

mod server;
pub use server::RpcServer;

pub mod graceful;

pub mod dispatch;
use dispatch::Dispatch;

/// General config for server-side
#[derive(Clone)]
pub struct ServerConfig {
    /// socket read timeout
    pub read_timeout: Duration,
    /// Socket write timeout
    pub write_timeout: Duration,
    /// Socket idle time to be close.
    pub idle_timeout: Duration,
    /// wait for all connections to be close with a timeout
    pub server_close_wait: Duration,
    /// In bytes. when non-zero, overwrite the default DEFAULT_BUF_SIZE of transport
    pub stream_buf_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(120),
            server_close_wait: Duration::from_secs(90),
            stream_buf_size: 0,
        }
    }
}

/// A central hub defined by the user for the server-side, to define the customizable plugin.
///
/// # NOTE
pub trait ServerFacts: Sync + Send + 'static + Sized {
    /// You should keep ServerConfig inside, get_config() will return the reference.
    fn get_config(&self) -> &ServerConfig;

    /// Construct a [captains_log::filter::Filter](https://docs.rs/captains-log/latest/captains_log/filter/trait.Filter.html) to oganize log of a client
    fn new_logger(&self) -> Arc<LogFilter>;
}

/// This trait is for server-side transport layer protocol.
///
/// The implementation can be found on:
///
/// - [razor-rpc-tcp](https://docs.rs/razor-rpc-tcp): For TCP and Unix socket
pub trait ServerTransport: Send + Sync + Sized + 'static + fmt::Debug {
    type RT: AsyncRuntime;

    type Listener: AsyncListener;

    fn bind(addr: &str) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// The implementation is expected to store the conn_count until dropped
    fn new_conn(
        stream: <Self::Listener as AsyncListener>::Conn, config: &ServerConfig, conn_count: Arc<()>,
    ) -> Self;

    /// Read a request from the socket
    fn read_req<'a>(
        &'a self, logger: &LogFilter, close_ch: &MAsyncRx<mpmc::Null>,
    ) -> impl Future<Output = Result<RpcSvrReq<'a>, RpcIntErr>> + Send;

    /// Write our user task response
    fn write_resp<T: ServerTaskEncode>(
        &self, logger: &LogFilter, codec: &impl Codec, task: T,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Write out ping resp or error
    fn write_resp_internal(
        &self, logger: &LogFilter, seq: u64, err: Option<RpcIntErr>,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Flush the response for the socket writer, if the transport has buffering logic
    fn flush_resp(&self, logger: &LogFilter) -> impl Future<Output = io::Result<()>> + Send;

    /// Shutdown the write direction of the connection
    fn close_conn(&self, logger: &LogFilter) -> impl Future<Output = ()> + Send;
}

/// A temporary struct to hold data buffer return by ServerTransport
///
/// NOTE: `RpcAction` and `msg` contains slice that reference to ServerTransport's internal buffer,
/// you should parse and clone them.
pub struct RpcSvrReq<'a> {
    pub seq: u64,
    pub action: RpcAction<'a>,
    pub msg: &'a [u8],
    pub blob: Option<Buffer>, // for write, this contains data
}

impl<'a> fmt::Debug for RpcSvrReq<'a> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "req(seq={}, action={:?})", self.seq, self.action)
    }
}

/// A Struct to hold pre encoded buffer for server response
#[allow(dead_code)]
#[derive(Debug)]
pub struct RpcSvrResp {
    pub seq: u64,

    pub msg: Option<Vec<u8>>,

    pub blob: Option<Buffer>,

    pub res: Option<Result<(), EncodedErr>>,
}

impl task::ServerTaskEncode for RpcSvrResp {
    #[inline]
    fn encode_resp<'a, 'b, C: Codec>(
        &'a mut self, _codec: &'b C, buf: &'b mut Vec<u8>,
    ) -> (u64, Result<(usize, Option<&'a [u8]>), EncodedErr>) {
        match self.res.take().unwrap() {
            Ok(_) => {
                if let Some(msg) = self.msg.as_ref() {
                    use std::io::Write;
                    buf.write_all(msg).expect("fill msg");
                    return (self.seq, Ok((msg.len(), self.blob.as_deref())));
                } else {
                    return (self.seq, Ok((0, self.blob.as_deref())));
                }
            }
            Err(e) => return (self.seq, Err(e)),
        }
    }
}

/// An ServerFacts for general use
pub struct ServerDefault {
    pub logger: Arc<LogFilter>,
    config: ServerConfig,
}

impl ServerDefault {
    pub fn new(config: ServerConfig) -> Arc<Self> {
        Arc::new(Self { logger: Arc::new(LogFilter::new()), config })
    }

    #[inline]
    pub fn set_log_level(&self, level: log::Level) {
        self.logger.set_level(level);
    }
}

impl ServerFacts for ServerDefault {
    #[inline]
    fn new_logger(&self) -> Arc<LogFilter> {
        self.logger.clone()
    }

    #[inline]
    fn get_config(&self) -> &ServerConfig {
        &self.config
    }
}
