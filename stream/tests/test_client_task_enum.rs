use crossfire::{MTx, mpsc};
use nix::errno::Errno;
use razor_rpc_codec::MsgpCodec;
use razor_stream::error::{RpcErrCodec, RpcError, RpcIntErr};
use razor_stream::{client::task::*, proto::RpcAction};
use razor_stream_macros::{client_task, client_task_enum};
use std::marker::PhantomData;

#[client_task(1, debug)]
struct TaskA<E: RpcErrCodec> {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: String,
    #[field(resp)]
    resp: Option<String>,
    #[field(res)]
    res: Option<Result<(), RpcError<E>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTask>>>,
}

#[client_task("task_b", debug)]
struct TaskB<E: RpcErrCodec> {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: u32,
    #[field(resp)]
    resp: Option<u32>,
    #[field(res)]
    res: Option<Result<(), RpcError<E>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTask>>>,
    _phantom: PhantomData<E>,
}

#[client_task(3, debug)]
struct TaskC<E: RpcErrCodec> {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: (),
    #[field(resp)]
    resp: Option<()>,
    #[field(req_blob)]
    req_blob: Vec<u8>,
    #[field(resp_blob)]
    resp_blob: Option<Vec<u8>>,
    #[field(res)]
    res: Option<Result<(), RpcError<E>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTask>>>,
    _phantom: PhantomData<E>,
}

#[client_task_enum(error = Errno)]
#[derive(Debug)]
enum MyTask {
    A(TaskA<Errno>),
    B(TaskB<Errno>),
    C(TaskC<Errno>),
}

#[test]
fn test_client_task_enum_delegation() {
    let (tx, rx) = mpsc::unbounded_blocking();
    let task_a = TaskA::<Errno> {
        common: ClientTaskCommon::default(),
        req: "hello".to_string(),
        resp: None,
        res: None,
        noti: Some(tx.clone()),
    };

    let task_b = TaskB::<Errno> {
        common: ClientTaskCommon::default(),
        req: 123,
        resp: None,
        res: None,
        noti: Some(tx.clone()),
        _phantom: PhantomData,
    };

    let task_c = TaskC::<Errno> {
        common: ClientTaskCommon::default(),
        req: (),
        resp: None,
        req_blob: vec![1, 2, 3],
        resp_blob: Some(vec![]),
        res: None,
        noti: Some(tx.clone()),
        _phantom: PhantomData,
    };

    // Test From impls
    let mut enum_task_a: MyTask = task_a.into();

    // Test Deref
    enum_task_a.set_seq(10);

    // Test ClientTaskAction delegation
    assert_eq!(enum_task_a.get_action(), RpcAction::Num(1));

    let mut enum_task_b: MyTask = task_b.into();
    assert_eq!(enum_task_b.get_action(), RpcAction::Str("task_b"));
    let mut enum_task_c: MyTask = task_c.into();
    assert_eq!(enum_task_c.get_action(), RpcAction::Num(3));

    // Test ClientTask delegation
    assert_eq!(enum_task_a.get_result(), Err(&RpcIntErr::Internal.into()));
    enum_task_a.set_ok();
    enum_task_a.done();
    let received = rx.recv().unwrap();
    assert!(matches!(received, MyTask::A(_)));
    assert_eq!(received.get_result(), Ok(()));

    // Test ClientTaskEncode/Decode delegation
    let codec = MsgpCodec::default();
    let mut msg_buf = Vec::new();
    enum_task_b.encode_req(&codec, &mut msg_buf).expect("encode_req ok");

    if let MyTask::B(t) = &mut enum_task_b {
        t.resp = None;
    }
    assert!(enum_task_b.decode_resp(&codec, &msg_buf).is_ok());
    if let MyTask::B(t) = enum_task_b {
        assert!(t.resp.is_some());
    } else {
        panic!("Wrong enum variant");
    }

    // Test blob delegation
    assert_eq!(enum_task_c.get_req_blob(), Some(vec![1, 2, 3].as_slice()));
    assert!(enum_task_c.reserve_resp_blob(10).is_some());
}

#[client_task(999, debug)] // Dummy action
struct TaskActionOverwrite {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: String,
    #[field(resp)]
    resp: Option<String>,
    #[field(res)]
    res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTaskWithAction>>>,
}

#[client_task(999, debug)] // Dummy action
struct TaskActionDelegate {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: String,
    #[field(resp)]
    resp: Option<String>,
    #[field(res)]
    res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTaskWithAction>>>,
}

#[client_task("task_b", debug)]
struct TaskBWithAction {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: u32,
    #[field(resp)]
    resp: Option<u32>,
    #[field(res)]
    res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyTaskWithAction>>>,
}

#[client_task_enum(error = Errno)]
#[derive(Debug)]
enum MyTaskWithAction {
    #[action(100)]
    A(TaskActionOverwrite),
    #[action("action_b")]
    B(TaskBWithAction),
    C(TaskActionDelegate),
}

#[test]
fn test_client_task_enum_with_action_attribute() {
    let (tx, _rx) = mpsc::unbounded_blocking();
    let task_a = TaskActionOverwrite {
        common: ClientTaskCommon::default(),
        req: "hello".to_string(),
        resp: None,
        res: None,
        noti: Some(tx.clone()),
    };

    let task_b = TaskBWithAction {
        common: ClientTaskCommon::default(),
        req: 123,
        resp: None,
        res: None,
        noti: Some(tx.clone()),
    };

    let enum_task_a: MyTaskWithAction = task_a.into();
    assert_eq!(enum_task_a.get_action(), RpcAction::Num(100));

    let enum_task_b: MyTaskWithAction = task_b.into();
    assert_eq!(enum_task_b.get_action(), RpcAction::Str("action_b"));

    let task_c = TaskActionDelegate {
        common: ClientTaskCommon::default(),
        req: "hello".to_string(),
        resp: None,
        res: None,
        noti: Some(tx.clone()),
    };

    let enum_task_c: MyTaskWithAction = task_c.into();
    assert_eq!(enum_task_c.get_action(), RpcAction::Num(999));
}
