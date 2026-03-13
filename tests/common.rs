use razor_rpc::server::task::{APIServerReq, APIServerResp};
use razor_rpc::{Codec, error::RpcError};
use razor_rpc_codec::MsgpCodec;
use razor_rpc_macros::{service, service_mux_struct};
use razor_stream::server::task::RespNoti;
use serde_derive::{Deserialize, Serialize};
use std::sync::Arc;

pub fn create_mock_request<T: serde::Serialize>(
    seq: u64, service: String, method: String, req: &T, noti: RespNoti<APIServerResp>,
) -> APIServerReq<MsgpCodec> {
    let codec = Arc::new(MsgpCodec::default());
    let req_data = codec.encode(req).expect("encode");
    return APIServerReq { seq, service, method, req: Some(req_data), codec, noti };
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct MyArg {
    pub value: u32,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct MyResp {
    pub result: u32,
}

// Service trait with multiple error types
#[async_trait::async_trait]
pub trait MultiErrorService {
    async fn success_method(&self, arg: MyArg) -> Result<MyResp, RpcError<String>>;
    async fn string_error(&self, arg: MyArg) -> Result<MyResp, RpcError<String>>;
    async fn i32_error(&self, arg: MyArg) -> Result<MyResp, RpcError<i32>>;
    async fn errno_error(&self, arg: MyArg) -> Result<MyResp, RpcError<nix::errno::Errno>>;
}

pub struct MultiErrorServiceImpl;

#[async_trait::async_trait]
#[service]
impl MultiErrorService for MultiErrorServiceImpl {
    async fn success_method(&self, arg: MyArg) -> Result<MyResp, RpcError<String>> {
        Ok(MyResp { result: arg.value + 1 })
    }

    async fn string_error(&self, _arg: MyArg) -> Result<MyResp, RpcError<String>> {
        Err("string error".to_string().into())
    }

    async fn i32_error(&self, _arg: MyArg) -> Result<MyResp, RpcError<i32>> {
        Err(42.into())
    }

    async fn errno_error(&self, _arg: MyArg) -> Result<MyResp, RpcError<nix::errno::Errno>> {
        Err(nix::errno::Errno::EPERM.into())
    }
}

// Service trait with `impl Future` return type (non-async fn)
pub trait ImplFutureServiceTrait {
    fn add(
        &self, arg: MyArg,
    ) -> impl std::future::Future<Output = Result<MyResp, RpcError<String>>> + Send;
}

pub struct ImplFutureService;

#[service]
impl ImplFutureServiceTrait for ImplFutureService {
    fn add(
        &self, arg: MyArg,
    ) -> impl std::future::Future<Output = Result<MyResp, RpcError<String>>> + Send {
        async move { Ok(MyResp { result: arg.value + 1 }) }
    }
}

// Service using async_trait
#[async_trait::async_trait]
pub trait MyAsyncTraitService {
    async fn mul(&self, arg: MyArg) -> Result<MyResp, RpcError<String>>;
}
pub struct MyAsyncTraitServiceImpl;
#[async_trait::async_trait]
#[service]
impl MyAsyncTraitService for MyAsyncTraitServiceImpl {
    async fn mul(&self, arg: MyArg) -> Result<MyResp, RpcError<String>> {
        Ok(MyResp { result: arg.value * 2 })
    }
}

// Service Dispatcher Struct
#[service_mux_struct]
pub struct MyServices {
    pub multi: Arc<MultiErrorServiceImpl>,
    pub impl_future: Arc<ImplFutureService>,
}
