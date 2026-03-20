pub mod api;
pub mod stream;

extern crate captains_log;
extern crate log;
pub use captains_log::logfn;
use captains_log::*;
pub use orb::prelude::*;
use rstest::*;
use std::fmt;

#[cfg(feature = "tokio")]
pub type RT = orb_tokio::TokioRT;
#[cfg(not(feature = "tokio"))]
pub type RT = orb_smol::SmolRT;

pub fn new_exec() -> <RT as orb::AsyncRuntime>::Exec {
    #[cfg(feature = "tokio")]
    {
        RT::multi(
            std::thread::available_parallelism()
                .unwrap_or(std::num::NonZero::new(1).unwrap())
                .into(),
        )
    }
    #[cfg(not(feature = "tokio"))]
    {
        RT::multi(2)
    }
}

pub type Codec = razor_rpc_codec::MsgpCodec;

#[macro_export]
macro_rules! async_spawn {
    ($f: expr) => {{
        #[cfg(feature = "tokio")]
        {
            tokio::spawn($f)
        }
        #[cfg(not(feature = "tokio"))]
        {
            smol::spawn($f)
        }
    }};
}

#[macro_export]
macro_rules! async_spawn_detach {
    ($f: expr) => {{
        #[cfg(feature = "tokio")]
        {
            let _ = tokio::spawn($f);
        }
        #[cfg(not(feature = "tokio"))]
        {
            // smol feature is enabled by default
            let _ = smol::spawn($f).detach();
        }
    }};
}

#[macro_export]
macro_rules! async_join_result {
    ($th: expr) => {{
        #[cfg(feature = "tokio")]
        {
            $th.await.expect("join")
        }
        #[cfg(not(feature = "tokio"))]
        {
            // smol feature is enabled by default
            $th.await
        }
    }};
}

#[fixture]
pub fn runner() -> TestRunner {
    TestRunner::new()
}

impl fmt::Debug for TestRunner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "")
    }
}

pub struct TestRunner {
    pub exec: <crate::RT as orb::AsyncRuntime>::Exec,
}

impl TestRunner {
    pub fn new() -> Self {
        recipe::raw_file_logger("/tmp/rpc_test.log", Level::Trace).test().build().expect("log");
        Self { exec: crate::new_exec() }
    }

    pub fn block_on<F: Future<Output = ()> + Send + 'static>(&self, f: F) {
        self.exec.block_on(f);
    }
}
