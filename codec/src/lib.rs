#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! # razor-rpc-codec
//!
//! This crate provides codec implementations for [`razor-rpc`](https://docs.rs/razor-rpc) and [`razor-stream`](https://docs.rs/razor-stream).

pub use razor_stream::{Codec, buffer::WriteBuf};
use serde::{Deserialize, Serialize};

#[cfg(feature = "msgpack")]
mod msgpack;
#[cfg(feature = "msgpack")]
pub use msgpack::*;

/// A codec just panics when called.
///
/// # NOTE:
///
/// Because ClientTask traits is not bound to Codec type,
/// you can assign `NullCodec` to ClientFacts when tasks need determined type of `Codec`
#[derive(Default)]
pub struct NullCodec();

impl Codec for NullCodec {
    fn encode<T>(&self, _task: &T) -> Result<Vec<u8>, ()>
    where
        T: Serialize,
    {
        unreachable!("DummyCodec");
    }

    fn encode_into<T: Serialize, B: WriteBuf>(&self, _task: &T, _buf: &mut B) -> Result<usize, ()> {
        unreachable!("DummyCodec");
    }

    fn decode<'a, T>(&self, _buf: &'a [u8]) -> Result<T, ()>
    where
        T: Deserialize<'a>,
    {
        unreachable!("DummyCodec");
    }
}
