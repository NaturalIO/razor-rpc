use razor_stream::client::task::*;
use serde_derive::{Deserialize, Serialize};

use crossfire::{MTx, mpsc};
use nix::errno::Errno;
use razor_rpc_codec::{Codec, MsgpCodec};
use razor_stream::error::*;
use razor_stream::proto::RpcAction;
use razor_stream_macros::client_task;
use std::ops::{Deref, DerefMut};

#[derive(Default, Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct FileIOReq {
    pub inode: u64,
    pub offset: i64,
}

#[derive(Default, Deserialize, Serialize, Debug, PartialEq)]
pub struct FileIOResp {
    pub read_size: u64,
}

#[test]
fn test_client_task_macro() {
    #[client_task]
    #[derive(Debug)]
    pub struct FileTask {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: FileIOReq,
        #[field(resp)]
        resp: Option<FileIOResp>,
    }
    let mut task = FileTask {
        common: ClientTaskCommon { seq: 123, ..Default::default() },
        req: FileIOReq { inode: 1, offset: 100 },
        resp: None,
    };
    let codec = MsgpCodec::default();

    // Test Deref and DerefMut
    assert_eq!(task.seq, 123);
    task.seq = 456;
    assert_eq!(task.deref().seq, 456);
    task.deref_mut().seq = 789;
    assert_eq!(task.seq, 789);

    // Test ClientTaskEncode
    let mut req_buf = Vec::new();
    task.encode_req(&codec, &mut req_buf).expect("encode");
    let req_copy: FileIOReq = codec.decode(&req_buf).expect("decode");
    assert_eq!(task.req, req_copy);

    // Test ClientTaskDecode
    let resp = FileIOResp { read_size: 1024 };
    let resp_buffer = codec.encode(&resp).expect("encode");
    let result = task.decode_resp(&codec, &resp_buffer);
    assert!(result.is_ok());
    assert_eq!(task.resp, Some(resp));
}

#[client_task]
pub struct FileTaskWithBlob {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: FileIOReq,
    #[field(req_blob)]
    blob: Vec<u8>,
    #[field(resp)]
    resp: Option<FileIOResp>,
}

#[test]
fn test_client_task_macro_with_req_blob() {
    let req_data = FileIOReq { inode: 2, offset: 200 };
    let blob_data = vec![1, 2, 3, 4, 5];

    let task = FileTaskWithBlob {
        common: ClientTaskCommon { seq: 123, ..Default::default() },
        req: req_data.clone(),
        resp: None,
        blob: blob_data.clone(),
    };

    // Test ClientTaskEncode::get_req_blob
    let retrieved_blob = task.get_req_blob();
    assert_eq!(retrieved_blob, Some(blob_data.as_slice()));
    let codec = MsgpCodec::default();

    // Test ClientTaskEncode::encode_req (should not include blob)
    let mut encoded_req_only = Vec::new();
    task.encode_req(&codec, &mut encoded_req_only).expect("encode");
    let req_copy: FileIOReq = codec.decode(&encoded_req_only).expect("decode");
    assert_eq!(task.req, req_copy);

    // Test ClientTaskDecode (should still work for resp)
    let mut task_decode = FileTaskWithBlob {
        common: ClientTaskCommon { seq: 123, ..Default::default() },
        req: req_data.clone(),
        resp: None,
        blob: blob_data.clone(),
    };
    let resp = FileIOResp { read_size: 2048 };
    let resp_buffer = codec.encode(&resp).expect("encode");
    let result = task_decode.decode_resp(&codec, &resp_buffer);
    assert!(result.is_ok());
    assert_eq!(task_decode.resp, Some(resp));
}

#[client_task]
pub struct FileTaskWithRespBlob {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: FileIOReq,
    #[field(resp)]
    resp: Option<FileIOResp>,
    #[field(resp_blob)]
    resp_blob: Option<Vec<u8>>,
}

#[test]
fn test_client_task_macro_with_resp_blob() {
    let req_data = FileIOReq { inode: 3, offset: 300 };
    let resp_data = FileIOResp { read_size: 4096 };
    let initial_resp_blob = Some(vec![6, 7, 8, 9, 10]);

    let mut task = FileTaskWithRespBlob {
        common: ClientTaskCommon { seq: 123, ..Default::default() },
        req: req_data.clone(),
        resp: None,
        resp_blob: initial_resp_blob,
    };

    // Test ClientTaskDecode::reserve_resp_blob
    assert!(task.reserve_resp_blob(20).is_some());

    let codec = MsgpCodec::default();

    // Test ClientTaskEncode (should still work for req)
    let mut encoded_req_only = Vec::new();
    task.encode_req(&codec, &mut encoded_req_only).expect("encode");
    let req_copy: FileIOReq = codec.decode(&encoded_req_only).expect("decode");
    assert_eq!(task.req, req_copy);

    // Test ClientTaskDecode (should still work for resp)
    let resp_buffer = codec.encode(&resp_data).expect("encode");
    let result = task.decode_resp(&codec, &resp_buffer);
    assert!(result.is_ok());
    assert_eq!(task.resp, Some(resp_data));
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
#[allow(dead_code)] // Allow unused variants for test enum
enum Action {
    Open = 1,
    Read = 2,
    Write = 3,
}

#[test]
fn test_client_task_macro_field_action() {
    #[derive(Default, Deserialize, Serialize, Debug, PartialEq, Clone)]
    pub struct MyReq {
        pub data: u32,
    }

    #[derive(Default, Deserialize, Serialize, Debug, PartialEq)]
    pub struct MyResp {
        pub result: bool,
    }

    #[client_task]
    #[derive(Debug)]
    pub struct FieldActionTaskNum {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(action)]
        action_num: u8, // Numeric action field
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    #[client_task]
    #[derive(Debug)]
    pub struct FieldActionTaskStr {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(action)]
        action_str: String, // String action field
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    // Test FieldActionTaskNum
    let task_num = FieldActionTaskNum {
        common: ClientTaskCommon { seq: 1, ..Default::default() },
        action_num: 5,
        req: MyReq { data: 10 },
        resp: None,
    };
    assert_eq!(task_num.get_action(), RpcAction::Num(5));

    // Test FieldActionTaskStr
    let task_str = FieldActionTaskStr {
        common: ClientTaskCommon { seq: 2, ..Default::default() },
        action_str: "my_action".to_string(),
        req: MyReq { data: 20 },
        resp: None,
    };
    assert_eq!(task_str.get_action(), RpcAction::Str("my_action"));
}

#[test]
fn test_client_task_macro_static_action() {
    #[derive(Default, Deserialize, Serialize, Debug, PartialEq, Clone)]
    pub struct MyReq {
        pub data: u32,
    }

    #[derive(Default, Deserialize, Serialize, Debug, PartialEq)]
    pub struct MyResp {
        pub result: bool,
    }

    #[client_task(10)] // Numeric static action
    #[derive(Debug)]
    pub struct StaticActionTaskNum {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    #[client_task("static_str_action")] // String static action
    #[derive(Debug)]
    pub struct StaticActionTaskStr {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    #[client_task(Action::Open)] // Enum static action
    #[derive(Debug)]
    pub struct StaticActionTaskEnum {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    // Test StaticActionTaskNum
    let task_num = StaticActionTaskNum {
        common: ClientTaskCommon { seq: 1, ..Default::default() },
        req: MyReq { data: 10 },
        resp: None,
    };
    assert_eq!(task_num.get_action(), RpcAction::Num(10));

    // Test StaticActionTaskStr
    let task_str = StaticActionTaskStr {
        common: ClientTaskCommon { seq: 2, ..Default::default() },
        req: MyReq { data: 20 },
        resp: None,
    };
    assert_eq!(task_str.get_action(), RpcAction::Str("static_str_action"));

    // Test StaticActionTaskEnum
    let task_enum = StaticActionTaskEnum {
        common: ClientTaskCommon { seq: 3, ..Default::default() },
        req: MyReq { data: 30 },
        resp: None,
    };
    assert_eq!(task_enum.get_action(), RpcAction::Num(Action::Open as i32));

    // New syntax tests
    #[client_task(100)] // Numeric static action (new syntax)
    #[derive(Debug)]
    pub struct NewStaticActionTaskNum {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    #[client_task("new_static_str_action")] // String static action (new syntax)
    #[derive(Debug)]
    pub struct NewStaticActionTaskStr {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    #[client_task(Action::Write)] // Enum static action (new syntax)
    #[derive(Debug)]
    pub struct NewStaticActionTaskEnum {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: MyReq,
        #[field(resp)]
        resp: Option<MyResp>,
    }

    // Test NewStaticActionTaskNum
    let new_task_num = NewStaticActionTaskNum {
        common: ClientTaskCommon { seq: 4, ..Default::default() },
        req: MyReq { data: 40 },
        resp: None,
    };
    assert_eq!(new_task_num.get_action(), RpcAction::Num(100));

    // Test NewStaticActionTaskStr
    let new_task_str = NewStaticActionTaskStr {
        common: ClientTaskCommon { seq: 5, ..Default::default() },
        req: MyReq { data: 50 },
        resp: None,
    };
    assert_eq!(new_task_str.get_action(), RpcAction::Str("new_static_str_action"));

    // Test NewStaticActionTaskEnum
    let new_task_enum = NewStaticActionTaskEnum {
        common: ClientTaskCommon { seq: 6, ..Default::default() },
        req: MyReq { data: 60 },
        resp: None,
    };
    assert_eq!(new_task_enum.get_action(), RpcAction::Num(Action::Write as i32));
}

#[client_task]
pub struct TaskWithDone {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: (),
    #[field(resp)]
    resp: Option<()>,
    #[field(res)]
    res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<Self>>>,
}

#[test]
fn test_client_task_macro_with_done() {
    // Test with Ok result
    let (done_tx, done_rx) = mpsc::unbounded_blocking();
    let mut task_ok = TaskWithDone {
        common: ClientTaskCommon { seq: 1, ..Default::default() },
        req: (()),
        resp: None,
        res: None,
        noti: Some(done_tx.clone()),
    };
    assert_eq!(task_ok.get_result(), Err(&RpcError::Rpc(RpcIntErr::Internal)));

    task_ok.set_ok();
    task_ok.done();

    let received_task_ok = done_rx.recv().unwrap();
    assert_eq!(received_task_ok.common.seq, 1);
    assert_eq!(received_task_ok.res, Some(Ok(())));
    assert!(received_task_ok.noti.is_none());
    assert_eq!(received_task_ok.get_result(), Ok(()));

    // Test with Err result
    let codec = MsgpCodec::default();
    let mut task_err = TaskWithDone {
        common: ClientTaskCommon { seq: 2, ..Default::default() },
        req: (()),
        resp: None,
        res: None,
        noti: Some(done_tx.clone()),
    };
    assert_eq!(task_err.get_result(), Err(&RpcError::Rpc(RpcIntErr::Internal)));

    let custom_error = Errno::EAGAIN;
    let encoded_error = custom_error.encode(&codec);
    task_err.set_custom_error(&codec, encoded_error, 0, 0);
    task_err.done();

    let received_task_err = done_rx.recv().unwrap();
    assert_eq!(received_task_err.common.seq, 2);
    assert_eq!(received_task_err.res, Some(Err(RpcError::User(custom_error))));
    assert!(received_task_err.noti.is_none());
    assert_eq!(received_task_err.get_result(), Err(&RpcError::User(custom_error)));
}

#[test]
fn test_client_task_macro_with_debug() {
    // Test with debug attribute and no action
    #[client_task(debug)]
    pub struct TaskWithDebug {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: FileIOReq,
        #[field(resp)]
        resp: Option<FileIOResp>,
    }

    let task1 = TaskWithDebug {
        common: ClientTaskCommon { seq: 1, ..Default::default() },
        req: FileIOReq { inode: 10, offset: 100 },
        resp: None,
    };
    let debug_output1 = format!("{:?}", task1);
    assert!(debug_output1.contains("TaskWithDebug(seq=1 req=FileIOReq { inode: 10, offset: 100 }"));
    assert!(!debug_output1.contains(" resp="));

    let task2 = TaskWithDebug {
        common: ClientTaskCommon { seq: 2, ..Default::default() },
        req: FileIOReq { inode: 20, offset: 200 },
        resp: Some(FileIOResp { read_size: 512 }),
    };
    let debug_output2 = format!("{:?}", task2);
    assert!(debug_output2.contains("TaskWithDebug(seq=2 req=FileIOReq { inode: 20, offset: 200 } resp=FileIOResp { read_size: 512 })"));

    // Test with debug attribute and an action
    #[client_task(123, debug)]
    pub struct TaskWithDebugAndAction {
        #[field(common)]
        common: ClientTaskCommon,
        #[field(req)]
        req: (),
        #[field(resp)]
        resp: Option<()>,
    }

    let task3 = TaskWithDebugAndAction {
        common: ClientTaskCommon { seq: 3, ..Default::default() },
        req: (),
        resp: None,
    };
    let debug_output3 = format!("{:?}", task3);
    assert!(debug_output3.contains("TaskWithDebugAndAction(seq=3 req=()"));
    assert_eq!(task3.get_action(), RpcAction::Num(123));
}
