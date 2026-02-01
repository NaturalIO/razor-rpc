use crossfire::{MTx, mpsc};
use nix::errno::Errno;
use razor_stream::{
    client::task::{ClientTaskCommon, *},
    error::RpcError,
    proto::RpcAction,
};
use razor_stream_macros::{client_task, client_task_enum};
use serde_derive::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::{PhantomData, Send, Unpin};

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct MyTaskReq;

#[derive(Default, Deserialize, Serialize, Debug)]
pub struct MyTaskResp;

#[client_task(1)]
#[derive(Debug)]
pub struct GenericTask<T: Send + Debug + Unpin + 'static> {
    #[field(common)]
    common: ClientTaskCommon,
    #[field(req)]
    req: MyTaskReq,
    #[field(resp)]
    resp: Option<MyTaskResp>,
    #[field(res)]
    res: Option<Result<(), RpcError<Errno>>>,
    #[field(noti)]
    noti: Option<MTx<mpsc::List<MyEnumTask<T>>>>,
    _phantom: PhantomData<T>,
}

#[client_task_enum(error = Errno)]
#[derive(Debug)]
pub enum MyEnumTask<T: Send + Debug + Unpin + 'static> {
    VariantA(GenericTask<T>),
}

#[test]
fn test_client_task_enum_with_generic_struct() {
    let (tx, rx) = mpsc::unbounded_blocking();

    let task = GenericTask::<u32> {
        common: ClientTaskCommon::default(),
        req: MyTaskReq,
        resp: None,
        res: None,
        noti: Some(tx),
        _phantom: PhantomData,
    };

    let mut enum_task: MyEnumTask<u32> = task.into();

    assert_eq!(enum_task.get_action(), RpcAction::Num(1));

    enum_task.set_ok();
    enum_task.done();

    let received = rx.recv().unwrap();
    assert!(matches!(received, MyEnumTask::VariantA(_)));
}
