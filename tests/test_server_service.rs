use razor_rpc::server::ServiceStatic;
use razor_rpc::{
    Codec,
    error::{EncodedErr, RpcIntErr},
};
use razor_rpc_codec::MsgpCodec;
use razor_stream::server::task::RespNoti;
use std::sync::Arc;

mod common;
use common::*;

#[tokio::test]
async fn test_multi_error_service() {
    assert_eq!(
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME,
        "MultiErrorService"
    );
    let service_impl = MultiErrorServiceImpl;
    let codec = MsgpCodec::default();
    let (tx, rx) = crossfire::mpsc::unbounded_async();
    let noti = RespNoti::new(tx);

    // Test success case
    let req = create_mock_request(
        1,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "success_method".to_string(),
        &MyArg { value: 10 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 1);
    assert!(resp.res.as_ref().unwrap().is_ok());
    let decoded_resp: MyResp = codec.decode(&resp.msg.unwrap()).unwrap();
    assert_eq!(decoded_resp.result, 11);

    // Test string error
    let req = create_mock_request(
        2,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "string_error".to_string(),
        &MyArg { value: 0 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 2);
    assert_eq!(
        resp.res.unwrap().unwrap_err(),
        EncodedErr::Buf("string error".to_string().into_bytes())
    );

    // Test i32 error
    let req = create_mock_request(
        3,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "i32_error".to_string(),
        &MyArg { value: 0 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 3);
    assert_eq!(resp.res.unwrap().unwrap_err(), EncodedErr::Num(42));

    // Test errno error
    let req = create_mock_request(
        4,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "errno_error".to_string(),
        &MyArg { value: 0 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 4);
    assert_eq!(resp.res.unwrap().unwrap_err(), EncodedErr::Num(nix::errno::Errno::EPERM as u32));

    // Test unknown method
    let req = create_mock_request(
        5,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "unknown_method".to_string(),
        &MyArg { value: 0 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 5);
    assert_eq!(resp.res.unwrap().unwrap_err(), EncodedErr::Rpc(RpcIntErr::Method));
}

#[tokio::test]
async fn test_impl_future_service() {
    assert_eq!(
        <ImplFutureService as ServiceStatic<MsgpCodec>>::SERVICE_NAME,
        "ImplFutureServiceTrait"
    );
    let service_impl = ImplFutureService;
    let codec = MsgpCodec::default();
    let (tx, rx) = crossfire::mpsc::unbounded_async();
    let noti = RespNoti::new(tx);

    let req = create_mock_request(
        1,
        <ImplFutureService as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "add".to_string(),
        &MyArg { value: 10 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 1);
    assert!(resp.res.as_ref().unwrap().is_ok());
    let decoded_resp: MyResp = codec.decode(&resp.msg.unwrap()).unwrap();
    assert_eq!(decoded_resp.result, 11);
}

#[tokio::test]
async fn test_async_trait_service() {
    assert_eq!(
        <MyAsyncTraitServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME,
        "MyAsyncTraitService"
    );
    let service_impl = MyAsyncTraitServiceImpl;
    let codec = MsgpCodec::default();
    let (tx, rx) = crossfire::mpsc::unbounded_async();
    let noti = RespNoti::new(tx);

    let req = create_mock_request(
        1,
        <MyAsyncTraitServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "mul".to_string(),
        &MyArg { value: 10 },
        noti.clone(),
    );
    ServiceStatic::serve(&service_impl, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 1);
    assert!(resp.res.as_ref().unwrap().is_ok());
    let decoded_resp: MyResp = codec.decode(&resp.msg.unwrap()).unwrap();
    assert_eq!(decoded_resp.result, 20);
}

#[tokio::test]
async fn test_service_mux_struct() {
    let services = MyServices {
        multi: Arc::new(MultiErrorServiceImpl),
        impl_future: Arc::new(ImplFutureService),
    };
    let codec = MsgpCodec::default();
    let (tx, rx) = crossfire::mpsc::unbounded_async();
    let noti = RespNoti::new(tx);

    // Test routing to the 'multi' service
    let req = create_mock_request(
        1,
        <MultiErrorServiceImpl as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "success_method".to_string(),
        &MyArg { value: 10 },
        noti.clone(),
    );
    ServiceStatic::serve(&services, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 1);
    assert!(resp.res.as_ref().unwrap().is_ok());
    let decoded_resp: MyResp = codec.decode(&resp.msg.unwrap()).unwrap();
    assert_eq!(decoded_resp.result, 11);

    // Test routing to the 'impl_future' service
    let req = create_mock_request(
        2,
        <ImplFutureService as ServiceStatic<MsgpCodec>>::SERVICE_NAME.to_string(),
        "add".to_string(),
        &MyArg { value: 20 },
        noti.clone(),
    );
    ServiceStatic::serve(&services, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 2);
    assert!(resp.res.as_ref().unwrap().is_ok());
    let decoded_resp: MyResp = codec.decode(&resp.msg.unwrap()).unwrap();
    assert_eq!(decoded_resp.result, 21);

    // Test unknown service
    let req = create_mock_request(
        3,
        "unknown_service".to_string(),
        "add".to_string(),
        &MyArg { value: 30 },
        noti.clone(),
    );
    ServiceStatic::serve(&services, req).await;
    let resp = rx.recv().await.unwrap().unwrap();
    assert_eq!(resp.seq, 3);
    assert_eq!(resp.res.unwrap().unwrap_err(), EncodedErr::Rpc(RpcIntErr::Service));
}
