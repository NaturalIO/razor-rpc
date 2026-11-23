use nix::errno::Errno;
use razor_rpc_codec::MsgpCodec;
use razor_stream::{Codec, error::RpcErrCodec};
use razor_stream::{
    proto::{RpcAction, RpcActionOwned},
    server::RpcSvrResp,
    server::task::*,
};
use razor_stream_macros::server_task_enum;
use serde_derive::{Deserialize, Serialize}; // Added this import

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct ReqMsg1 {
    pub val: u32,
}
#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct ReqMsg2 {
    pub name: String,
}

// Dummy message structs
#[derive(Serialize, Deserialize, Debug, Default)]
struct Msg1;
#[derive(Serialize, Deserialize, Debug, Default)]
struct Msg2;

// Define a dummy struct that implements ServerTaskDecode for static action testing
#[derive(Debug, Default, PartialEq)]
struct StaticActionTask;

impl<R: Send + Unpin + 'static> ServerTaskDecode<R> for StaticActionTask {
    fn decode_req<'a, C: Codec>(
        _codec: &'a C, _action: RpcAction<'a>, _seq: u32, _req: &'a [u8],
        _blob: Option<io_buffer::Buffer>, _noti: RespNoti<R>,
    ) -> Result<Self, ()> {
        Ok(StaticActionTask)
    }
}

#[test]
fn test_server_task_enum_req_macro() {
    #[server_task_enum(req, resp_type=RpcSvrResp, error = Errno)]
    #[derive(Debug)]
    pub enum ExampleServerTaskReq {
        #[action(1)]
        Task1(ServerTaskVariant<RpcSvrResp, ReqMsg1, Errno>),
        #[action("sub_task_2")]
        Task2(ServerTaskVariant<RpcSvrResp, ReqMsg2, Errno>),
        #[action("static_action")] // New variant for static action
        Task3(StaticActionTask),
    }

    let codec = MsgpCodec::default();
    let (tx, _rx) = crossfire::mpsc::unbounded_async();
    let noti: RespNoti<RpcSvrResp> = RespNoti::new(tx);

    // Test decode_req with numeric action and actual data
    let req_msg_1_data = ReqMsg1 { val: 100 };
    let req_msg_1_buf = codec.encode(&req_msg_1_data).unwrap();
    let task1 = <ExampleServerTaskReq as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Num(1),
        123,
        &req_msg_1_buf,
        None,
        noti.clone(),
    )
    .unwrap();
    if let ExampleServerTaskReq::Task1(req_task_variant) = task1 {
        assert_eq!(req_task_variant.seq, 123);
        assert_eq!(req_task_variant.msg, req_msg_1_data);
        assert!(req_task_variant.blob.is_none()); // Assuming no blob is sent for this test
    } else {
        panic!("Decoded to wrong variant");
    }

    // Test decode_req with string action and actual data
    let req_msg_2_data = ReqMsg2 { name: "test_name".to_string() };
    let req_msg_2_buf = codec.encode(&req_msg_2_data).unwrap();
    let task2 = <ExampleServerTaskReq as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Str("sub_task_2"),
        456,
        &req_msg_2_buf,
        None,
        noti.clone(),
    )
    .unwrap();
    assert_eq!(task2.get_action(), RpcAction::Str("sub_task_2"));
    if let ExampleServerTaskReq::Task2(req_task_variant) = task2 {
        assert_eq!(req_task_variant.seq, 456);
        assert_eq!(req_task_variant.msg, req_msg_2_data);
        assert!(req_task_variant.blob.is_none()); // Assuming no blob is sent for this test
    } else {
        panic!("Decoded to wrong variant");
    }

    // Test for Task3 (static action)
    let task3 = <ExampleServerTaskReq as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Str("static_action"),
        0,    // Dummy seq
        &[],  // Dummy req
        None, // Dummy blob
        noti.clone(),
    )
    .unwrap();
    assert_eq!(task3.get_action(), RpcAction::Str("static_action"));
    if let ExampleServerTaskReq::Task3(inner_task) = task3 {
        assert_eq!(inner_task, StaticActionTask);
    } else {
        panic!("Decoded to wrong variant");
    }

    // Test From impls
    let req_task_variant_from_sub = ServerTaskVariant {
        seq: 3,
        action: RpcActionOwned::Num(1),
        msg: ReqMsg1::default(),
        blob: None,
        res: None,
        noti: Some(noti.clone()),
    };
    let server_task_from_sub: ExampleServerTaskReq = req_task_variant_from_sub.into();
    assert!(matches!(server_task_from_sub, ExampleServerTaskReq::Task1(_)));
    assert_eq!(server_task_from_sub.get_action(), RpcAction::Num(1));
}

#[test]
fn test_server_task_enum_resp_macro() {
    #[server_task_enum(resp, error = Errno)]
    #[derive(Debug)]
    pub enum ExampleServerTaskResp {
        Task1(ServerTaskVariant<ExampleServerTaskResp, Msg1, Errno>),
        Task2(ServerTaskVariant<ExampleServerTaskResp, Msg2, Errno>),
    }
    let codec = MsgpCodec::default();
    let (tx, _rx) = crossfire::mpsc::unbounded_async();
    let noti: RespNoti<ExampleServerTaskResp> = RespNoti::new(tx);

    // Create ServerTaskVariant instances using decode_req
    let msg1_buf = codec.encode(&Msg1::default()).unwrap();
    let variant1: ServerTaskVariant<ExampleServerTaskResp, Msg1, Errno> =
        <ServerTaskVariant<ExampleServerTaskResp, Msg1, Errno> as ServerTaskDecode<
            ExampleServerTaskResp,
        >>::decode_req(&codec, RpcAction::Num(1), 123, &msg1_buf, None, noti.clone())
        .unwrap();

    let msg2_buf = codec.encode(&Msg2::default()).unwrap();
    let variant2: ServerTaskVariant<ExampleServerTaskResp, Msg2, Errno> =
        <ServerTaskVariant<ExampleServerTaskResp, Msg2, Errno> as ServerTaskDecode<
            ExampleServerTaskResp,
        >>::decode_req(&codec, RpcAction::Num(2), 456, &msg2_buf, None, noti.clone())
        .unwrap();

    let mut task1_for_encode = ExampleServerTaskResp::Task1(variant1);
    <ExampleServerTaskResp as ServerTaskDone<ExampleServerTaskResp, Errno>>::_set_result(
        &mut task1_for_encode,
        Err(Errno::EPERM.into()),
    );
    let mut buf = Vec::new();
    let (seq1, resp1) = <ExampleServerTaskResp as ServerTaskEncode>::encode_resp(
        &mut task1_for_encode,
        &codec,
        &mut buf,
    );
    assert_eq!(seq1, 123);
    assert_eq!(resp1.unwrap_err(), Errno::EPERM.encode(&codec));

    let mut task2_for_encode = ExampleServerTaskResp::Task2(variant2);
    <ExampleServerTaskResp as ServerTaskDone<ExampleServerTaskResp, Errno>>::_set_result(
        &mut task2_for_encode,
        Ok(()),
    );
    let mut buf = Vec::new();
    let (seq2, resp2) = <ExampleServerTaskResp as ServerTaskEncode>::encode_resp(
        &mut task2_for_encode,
        &codec,
        &mut buf,
    );
    assert_eq!(seq2, 456);
    assert!(resp2.is_ok());

    let mut task_for_set_result = ExampleServerTaskResp::Task1(
        <ServerTaskVariant<ExampleServerTaskResp, Msg1, Errno> as ServerTaskDecode<
            ExampleServerTaskResp,
        >>::decode_req(&codec, RpcAction::Num(1), 789, &msg1_buf, None, noti.clone())
        .unwrap(),
    );
    let _: RespNoti<ExampleServerTaskResp> = <ExampleServerTaskResp as ServerTaskDone<
        ExampleServerTaskResp,
        Errno,
    >>::_set_result(&mut task_for_set_result, Ok(()));

    // Test set_result
    #[allow(unused_mut)]
    let mut task_for_set_result_done = ExampleServerTaskResp::Task2(
        <ServerTaskVariant<ExampleServerTaskResp, Msg2, Errno> as ServerTaskDecode<
            ExampleServerTaskResp,
        >>::decode_req(&codec, RpcAction::Num(2), 101, &msg2_buf, None, noti.clone())
        .unwrap(),
    );
    task_for_set_result_done.set_result(Ok(()));
}

#[test]
fn test_server_task_enum_multiple_actions() {
    #[server_task_enum(req, resp_type=RpcSvrResp, error = Errno)]
    #[derive(Debug)]
    pub enum MultiActionServerTask {
        #[action(1, 2, "action_str")]
        TaskA(ServerTaskVariant<RpcSvrResp, ReqMsg1, Errno>),
    }

    let codec = MsgpCodec::default();
    let (tx, _rx) = crossfire::mpsc::unbounded_async();
    let noti: RespNoti<RpcSvrResp> = RespNoti::new(tx);

    let req_msg_data = ReqMsg1 { val: 100 };
    let req_msg_buf = codec.encode(&req_msg_data).unwrap();

    // Test decoding with first numeric action
    let task1 = <MultiActionServerTask as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Num(1),
        123,
        &req_msg_buf,
        None,
        noti.clone(),
    )
    .unwrap();
    assert_eq!(task1.get_action(), RpcAction::Num(1));
    let MultiActionServerTask::TaskA(req_task_variant) = task1;
    assert_eq!(req_task_variant.seq, 123);
    assert_eq!(req_task_variant.msg, req_msg_data);

    // Test decoding with second numeric action
    let task2 = <MultiActionServerTask as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Num(2),
        456,
        &req_msg_buf,
        None,
        noti.clone(),
    )
    .unwrap();
    assert_eq!(task2.get_action(), RpcAction::Num(2));
    let MultiActionServerTask::TaskA(req_task_variant) = task2;
    assert_eq!(req_task_variant.seq, 456);
    assert_eq!(req_task_variant.msg, req_msg_data);

    // Test decoding with string action
    let task3 = <MultiActionServerTask as ServerTaskDecode<RpcSvrResp>>::decode_req(
        &codec,
        RpcAction::Str("action_str"),
        789,
        &req_msg_buf,
        None,
        noti.clone(),
    )
    .unwrap();
    assert_eq!(task3.get_action(), RpcAction::Str("action_str"));
    let MultiActionServerTask::TaskA(req_task_variant) = task3;
    assert_eq!(req_task_variant.seq, 789);
    assert_eq!(req_task_variant.msg, req_msg_data);
}
