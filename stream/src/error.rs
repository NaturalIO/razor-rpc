//! Error handling for razor-stream RPC framework.
//!
//! This module provides the error types and traits used for RPC error handling.
//! It supports both internal RPC errors and user-defined custom errors.
//!
//! # Default Supported Error Types
//!
//! The following types have built-in `RpcErrCodec` implementations:
//!
//! - **Numeric types**: `i8`, `u8`, `i16`, `u16`, `i32`, `u32`
//!   - Encoded as `u32` values for efficient transport
//!   - Useful for errno-style error codes
//!
//! - **`()` (unit type)**
//!   - Encoded as `0u32`
//!   - Used when no additional error information is needed
//!
//! - `String` and &str
//!   - Encoded as UTF-8 bytes
//!   - Useful for descriptive error messages
//!
//! - `nix::errno::Errno`
//!   - Encoded as `u32` values
//!   - For system-level error codes
//!
//! # Custom Error Types
//!
//! To use your own error type in RPC methods, implement the [`RpcErrCodec`] trait.
//!
//! Keep in mind that your type should can be serialized into or deserialized from one of the
//! variants of [EncodedErr].
//! You have to choose in-between numeric or string:
//!
//! ## Approach 1: Numeric Encoding (errno-style)
//!
//! Use this when you want compact, efficient numeric error codes.
//!
//! ```rust
//! use num_enum::TryFromPrimitive;
//! use razor_stream::{Codec, error::{RpcErrCodec, RpcIntErr, EncodedErr}};
//!
//! #[derive(Debug, Clone, Copy, PartialEq, TryFromPrimitive)]
//! #[repr(u32)]
//! pub enum MyErrorCode {
//!     NotFound = 1,
//!     PermissionDenied = 2,
//!     Timeout = 3,
//! }
//!
//! impl RpcErrCodec for MyErrorCode {
//!     fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
//!         EncodedErr::Num(*self as u32)
//!     }
//!
//!     fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
//!         if let Ok(code) = buf {
//!             return MyErrorCode::try_from(code).map_err(|_| ());
//!         }
//!         Err(())
//!     }
//!
//!     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//!         std::fmt::Debug::fmt(self, f)
//!     }
//! }
//! ```
//!
//! ## Approach 2: String Encoding (with strum)
//!
//! Use this when you want human-readable error strings that can be serialized/deserialized.
//! Uses `EncodedErr::Static` to avoid heap allocation during encoding.
//!
//! ```rust
//! use razor_stream::{Codec, error::{RpcErrCodec, RpcIntErr, EncodedErr}};
//! use std::str::FromStr;
//! use strum::{Display, EnumString, IntoStaticStr};
//!
//! #[derive(Debug, Clone, Display, EnumString, IntoStaticStr, PartialEq)]
//! pub enum MyStringError {
//!     #[strum(serialize = "not_found")]
//!     NotFound,
//!     #[strum(serialize = "permission_denied")]
//!     PermissionDenied,
//!     #[strum(serialize = "timeout")]
//!     Timeout,
//! }
//!
//! impl RpcErrCodec for MyStringError {
//!     fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
//!         // Use EncodedErr::Static to avoid heap allocation, with the help of strum::IntoStaticStr
//!         EncodedErr::Static(self.into())
//!     }
//!
//!     fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
//!         // Decode with zero-copy: directly parse from &[u8] without allocating
//!         if let Err(bytes) = buf {
//!             // Safety: error strings are valid ASCII (subset of UTF-8), so unchecked is safe
//!             let s = unsafe { std::str::from_utf8_unchecked(bytes) };
//!             // Use strum's EnumString derive to parse from string
//!             return MyStringError::from_str(s).map_err(|_| ());
//!         }
//!         Err(())
//!     }
//!
//!     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//!         std::fmt::Display::fmt(self, f)
//!     }
//! }
//! ```

use crate::Codec;
use std::fmt;

/// "rpc_" prefix is reserved for internal error, you should avoid conflict with it
pub const RPC_ERR_PREFIX: &str = "rpc_";

/// A error type defined by client-side user logic
///
/// Due to possible decode
#[derive(thiserror::Error)]
pub enum RpcError<E: RpcErrCodec> {
    User(#[from] E),
    Rpc(#[from] RpcIntErr),
}

impl<E: RpcErrCodec> fmt::Display for RpcError<E> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::User(e) => RpcErrCodec::fmt(e, f),
            Self::Rpc(e) => fmt::Display::fmt(e, f),
        }
    }
}

impl<E: RpcErrCodec> fmt::Debug for RpcError<E> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<E: RpcErrCodec> std::cmp::PartialEq<RpcIntErr> for RpcError<E> {
    #[inline]
    fn eq(&self, other: &RpcIntErr) -> bool {
        if let Self::Rpc(r) = self
            && r == other
        {
            return true;
        }
        false
    }
}

impl<E: RpcErrCodec + PartialEq> std::cmp::PartialEq<E> for RpcError<E> {
    #[inline]
    fn eq(&self, other: &E) -> bool {
        if let Self::User(r) = self {
            return r == other;
        }
        false
    }
}

impl<E: RpcErrCodec + PartialEq> std::cmp::PartialEq<RpcError<E>> for RpcError<E> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Rpc(r) => {
                if let Self::Rpc(o) = other {
                    return r == o;
                }
            }
            Self::User(r) => {
                if let Self::User(o) = other {
                    return r == o;
                }
            }
        }
        false
    }
}

impl From<&str> for RpcError<String> {
    #[inline]
    fn from(e: &str) -> Self {
        Self::User(e.to_string())
    }
}

/// Serialize and Deserialize trait for custom RpcError
///
/// There is only two forms for rpc transport layer, u32 and String, choose one of them.
///
/// Because Rust does not allow overlapping impl, we only imple RpcErrCodec trait by default for the following types:
/// - ()
/// - from i8 to u32
/// - String
/// - nix::errno::Errno
///
/// If you use other type as error, you can implement manually:
///
/// # Example with serde_derive
/// ```rust
/// use serde_derive::{Serialize, Deserialize};
/// use razor_stream::{Codec, error::{RpcErrCodec, RpcIntErr, EncodedErr}};
/// use strum::Display;
/// #[derive(Serialize, Deserialize, Debug)]
/// pub enum MyError {
///     NoSuchFile,
///     TooManyRequest,
/// }
///
/// impl RpcErrCodec for MyError {
///     #[inline(always)]
///     fn encode<C: Codec>(&self, codec: &C) -> EncodedErr {
///         match codec.encode(self) {
///             Ok(buf)=>EncodedErr::Buf(buf),
///             Err(())=>EncodedErr::Rpc(RpcIntErr::Encode),
///         }
///     }
///
///     #[inline(always)]
///     fn decode<C: Codec>(codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
///         if let Err(b) = buf {
///             return codec.decode(b);
///         } else {
///             Err(())
///         }
///     }
///     #[inline(always)]
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         std::fmt::Debug::fmt(self, f)
///     }
/// }
/// ```
///
/// # Example with num_enum
///
/// ```rust
/// use num_enum::TryFromPrimitive;
/// use razor_stream::{Codec, error::{RpcErrCodec, RpcIntErr, EncodedErr}};
///
/// // Define your error codes as a C-like enum with explicit values
/// // You can use num_enum's TryFromPrimitive for safer deserialization
/// #[derive(Debug, Clone, Copy, PartialEq, TryFromPrimitive)]
/// #[repr(u32)]
/// pub enum MyRpcErrorCode {
///     /// Service is not available
///     ServiceUnavailable = 1,
///     /// Request timed out
///     RequestTimeout = 2,
///     /// Invalid parameter
///     InvalidParameter = 3,
///     /// Resource not found
///     NotFound = 4,
/// }
///
/// impl RpcErrCodec for MyRpcErrorCode {
///     #[inline(always)]
///     fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
///         // Manual conversion to u32 (no IntoPrimitive needed)
///         let code: u32 = *self as u32;
///         EncodedErr::Num(code)
///     }
///
///     #[inline(always)]
///     fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
///         if let Ok(code) = buf {
///             // Using num_enum for safe deserialization (TryFromPrimitive)
///             return MyRpcErrorCode::try_from(code).map_err(|_| ());
///         }
///         Err(())
///     }
///
///     #[inline(always)]
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         std::fmt::Debug::fmt(self, f)
///     }
/// }
/// ```
pub trait RpcErrCodec: Send + Sized + 'static + Unpin {
    fn encode<C: Codec>(&self, codec: &C) -> EncodedErr;

    fn decode<C: Codec>(codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()>;

    /// You can choose to use std::fmt::Debug or std::fmt::Display for the type.
    ///
    /// NOTE that this method exists because rust does not have Display for ().
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Check if this error should trigger a failover/retry and return the redirect address.
    ///
    /// This is used by FailoverPool to implement leader redirection for multi-node
    /// master-slave services. When a follower node receives a write request, it can
    /// return a redirect error with the leader's address.
    ///
    /// Returns:
    /// - `Ok(Some(addr))`: Retry to the specific address
    /// - `Ok(None)`: Retry to next available node (round-robin or leader election)
    /// - `Err(())`: Don't retry, return error to user
    ///
    /// Default implementation returns `Err(())`, meaning no retry.
    ///
    /// # Example
    /// ```rust
    /// use razor_stream::{Codec, error::{RpcErrCodec, EncodedErr}};
    ///
    /// #[derive(Debug, Clone, PartialEq)]
    /// pub enum MyError {
    ///     Redirect(String),
    ///     NotLeader,
    ///     OtherError,
    /// }
    ///
    /// impl RpcErrCodec for MyError {
    ///     fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
    ///         // ... encode implementation
    ///         # EncodedErr::Static("error")
    ///     }
    ///
    ///     fn decode<C: Codec>(_codec: &C, _buf: Result<u32, &[u8]>) -> Result<Self, ()> {
    ///         // ... decode implementation
    ///         # Err(())
    ///     }
    ///
    ///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
    ///         std::fmt::Debug::fmt(self, f)
    ///     }
    ///
    ///     fn should_failover(&self) -> Result<Option<&str>, ()> {
    ///         match self {
    ///             Self::Redirect(addr) => Ok(Some(addr)),
    ///             Self::NotLeader => Ok(None),  // Retry to next node
    ///             Self::OtherError => Err(()),  // Don't retry
    ///         }
    ///     }
    /// }
    /// ```
    #[inline(always)]
    fn should_failover(&self) -> Result<Option<&str>, ()> {
        Err(())
    }
}

macro_rules! impl_rpc_error_for_num {
    ($t: tt) => {
        impl RpcErrCodec for $t {
            #[inline(always)]
            fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
                EncodedErr::Num(*self as u32)
            }

            #[inline(always)]
            fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
                if let Ok(i) = buf {
                    if i <= $t::MAX as u32 {
                        return Ok(i as Self);
                    }
                }
                Err(())
            }

            #[inline(always)]
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "errno {}", self)
            }
        }
    };
}

impl_rpc_error_for_num!(i8);
impl_rpc_error_for_num!(u8);
impl_rpc_error_for_num!(i16);
impl_rpc_error_for_num!(u16);
impl_rpc_error_for_num!(i32);
impl_rpc_error_for_num!(u32);

impl RpcErrCodec for nix::errno::Errno {
    #[inline(always)]
    fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
        EncodedErr::Num(*self as u32)
    }

    #[inline(always)]
    fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
        if let Ok(i) = buf
            && i <= i32::MAX as u32
        {
            return Ok(Self::from_raw(i as i32));
        }
        Err(())
    }

    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RpcErrCodec for () {
    #[inline(always)]
    fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
        EncodedErr::Num(0u32)
    }

    #[inline(always)]
    fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
        if let Ok(i) = buf
            && i == 0
        {
            return Ok(());
        }
        Err(())
    }

    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "err")
    }
}

impl RpcErrCodec for String {
    #[inline(always)]
    fn encode<C: Codec>(&self, _codec: &C) -> EncodedErr {
        EncodedErr::Buf(Vec::from(self.as_bytes()))
    }
    #[inline(always)]
    fn decode<C: Codec>(_codec: &C, buf: Result<u32, &[u8]>) -> Result<Self, ()> {
        if let Err(s) = buf
            && let Ok(s) = str::from_utf8(s)
        {
            return Ok(s.to_string());
        }
        Err(())
    }

    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// RpcIntErr represent internal error from the framework
///
/// **NOTE**:
/// - This error type is serialized in string, "rpc_" prefix is reserved for internal error, you
///   should avoid conflict with it.
/// - We presume the variants less than RpcIntErr::Method is retriable errors
#[derive(
    strum::Display,
    strum::EnumString,
    strum::AsRefStr,
    PartialEq,
    PartialOrd,
    Clone,
    thiserror::Error,
)]
#[repr(u8)]
pub enum RpcIntErr {
    /// Ping or connect error
    #[strum(serialize = "rpc_unreachable")]
    Unreachable = 0,
    /// IO error
    #[strum(serialize = "rpc_io_err")]
    IO = 1,
    /// Task timeout
    #[strum(serialize = "rpc_timeout")]
    Timeout = 2,
    /// Method not found
    #[strum(serialize = "rpc_method_notfound")]
    Method = 3,
    /// service notfound
    #[strum(serialize = "rpc_service_notfound")]
    Service = 4,
    /// Encode Error
    #[strum(serialize = "rpc_encode")]
    Encode = 5,
    /// Decode Error
    #[strum(serialize = "rpc_decode")]
    Decode = 6,
    /// Internal error
    #[strum(serialize = "rpc_internal_err")]
    Internal = 7,
    /// invalid version number in rpc header
    #[strum(serialize = "rpc_invalid_ver")]
    Version = 8,
}

// The default Debug derive just ignore strum customized string, by strum only have a Display derive
impl fmt::Debug for RpcIntErr {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl RpcIntErr {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
}

impl From<std::io::Error> for RpcIntErr {
    #[inline(always)]
    fn from(_e: std::io::Error) -> Self {
        Self::IO
    }
}

/// A container for error message parse from / send into transport
#[derive(Debug)]
pub enum EncodedErr {
    /// The ClientTransport should try the best to parse it from string with "rpc_" prefix
    Rpc(RpcIntErr),
    /// For nix errno and the like
    Num(u32),
    /// Only for server-side to encode err.
    ///
    /// The ClientTransport cannot decode into static type
    Static(&'static str),
    /// The ClientTransport will fallback to `Vec<u8>` after try to parse  RpcIntErr and  num
    Buf(Vec<u8>),
}

impl EncodedErr {
    #[inline]
    pub fn try_as_str(&self) -> Result<&str, ()> {
        match self {
            Self::Static(s) => return Ok(s),
            Self::Buf(b) => {
                if let Ok(s) = str::from_utf8(b) {
                    return Ok(s);
                }
            }
            _ => {}
        }
        Err(())
    }
}

/// Just for macro test
impl std::cmp::PartialEq<EncodedErr> for EncodedErr {
    fn eq(&self, other: &EncodedErr) -> bool {
        match self {
            Self::Rpc(e) => {
                if let Self::Rpc(o) = other {
                    return e == o;
                }
            }
            Self::Num(e) => {
                if let Self::Num(o) = other {
                    return e == o;
                }
            }
            Self::Static(s) => {
                if let Ok(o) = other.try_as_str() {
                    return *s == o;
                }
            }
            Self::Buf(s) => {
                if let Self::Buf(o) = other {
                    return s == o;
                } else if let Ok(o) = other.try_as_str() {
                    // other's type is not Buf
                    if let Ok(_s) = str::from_utf8(s) {
                        return _s == o;
                    }
                }
            }
        }
        false
    }
}

impl fmt::Display for EncodedErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rpc(e) => e.fmt(f),
            Self::Num(no) => write!(f, "errno {}", no),
            Self::Static(s) => write!(f, "{}", s),
            Self::Buf(b) => match str::from_utf8(b) {
                Ok(s) => {
                    write!(f, "{}", s)
                }
                Err(_) => {
                    write!(f, "err blob {} length", b.len())
                }
            },
        }
    }
}

impl From<RpcIntErr> for EncodedErr {
    #[inline(always)]
    fn from(e: RpcIntErr) -> Self {
        Self::Rpc(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::errno::Errno;
    use std::str::FromStr;

    #[test]
    fn test_internal_error() {
        println!("{}", RpcIntErr::Internal);
        println!("{:?}", RpcIntErr::Internal);
        let s = RpcIntErr::Timeout.as_ref();
        println!("RpcIntErr::Timeout as {}", s);
        let e = RpcIntErr::from_str(s).expect("parse");
        assert_eq!(e, RpcIntErr::Timeout);
        assert!(RpcIntErr::from_str("timeoutss").is_err());
        assert!(RpcIntErr::Timeout < RpcIntErr::Method);
        assert!(RpcIntErr::IO < RpcIntErr::Method);
        assert!(RpcIntErr::Unreachable < RpcIntErr::Method);
    }

    #[test]
    fn test_rpc_error_default() {
        let e = RpcError::<i32>::from(1i32);
        println!("err {:?} {}", e, e);

        let e = RpcError::<Errno>::from(Errno::EIO);
        println!("err {:?} {}", e, e);

        let e = RpcError::<String>::from("err_str");
        println!("err {:?} {}", e, e);
        let e2 = RpcError::<String>::from("err_str".to_string());
        assert_eq!(e, e2);

        let e = RpcError::<String>::from(RpcIntErr::IO);
        println!("err {:?} {}", e, e);

        let _e: Result<(), RpcIntErr> = Err(RpcIntErr::IO);

        // let e: Result<(), RpcError::<String>> = _e.into();
        // Not allow by rust, and orphan rule prevent we do
        // `impl<E: RpcErrCodec> From<Result<(), RpcIntErr>> for Result<(), RpcError<E>>`

        // it's ok to use map_err
        let e: Result<(), RpcError<String>> = _e.map_err(|e| e.into());
        println!("err {:?}", e);
    }

    //#[test]
    //fn test_rpc_error_string_enum() {
    //    not supported by default, should provide a derive
    //    #[derive(
    //        Debug, strum::Display, strum::EnumString, strum::AsRefStr, PartialEq, Clone, thiserror::Error,
    //    )]
    //    enum MyError {
    //        OhMyGod,
    //    }
    //    let e = RpcError::<MyError>::from(MyError::OhMyGod);
    //    println!("err {:?} {}", e, e);
    //}
}
