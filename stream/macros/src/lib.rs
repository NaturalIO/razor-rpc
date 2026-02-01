//! # razor-stream-macros
//!
//! This crate provides procedural macros to simplify the implementation of RPC tasks for the [`razor-rpc`](https://docs.rs/razor-rpc) framework.
//! These macros automatically generate boilerplate code for trait implementations, reducing manual effort and improving code clarity.
//!
//! # Provided Macros
//!
//! ### Client-Side
//! - [`#[client_task]`](macro@client_task): For defining a client-side RPC task on a struct. It will not generate ClientTask trait (it's optional to you to define ClientTaskAction with it)
//! - [`#[client_task_enum]`](macro@client_task_enum): For creating an enum that delegates to client task variants. It will generate ClientTask trait for the enum
//!
//! ### Server-Side
//! - [`#[server_task_enum]`](macro@server_task_enum): For defining a server-side RPC task enum.

mod client_task;
mod client_task_enum;
mod server_task_enum;

/// # `#[client_task]`
///
/// The `#[client_task]` attribute macro is used on a struct to designate it as a client-side RPC task.
/// It simplifies implementation by generating boilerplate code for several traits.
///
/// The macro always generates:
/// - `Deref` and `DerefMut` to the field marked `#[field(common)]`.
/// - `ClientTaskEncode` for the `#[field(req)]` and `#[field(req_blob)]` fields.
/// - `ClientTaskDecode` for the `#[field(resp)]` and `#[field(resp_blob)]` fields.
///
/// The macro can also conditionally generate:
/// - `ClientTaskAction`: Generated if a static action is provided (e.g., `#[client_task(1)]`) or if a field is marked `#[field(action)]`.
/// - `ClientTaskDone`: Generated if both `#[field(res)]` and `#[field(noti)]` are present. If not generated, you must implement this trait manually.
///
/// ### Field Attributes:
///
/// * `#[field(common)]`: **(Mandatory)** Marks a field that holds common task information (e.g., `ClientTaskCommon`).
///   Allows direct access to members like `seq` via `Deref`.
///
/// * `#[field(action)]`: Specifies a field that dynamically provides the RPC action. Mutually exclusive with a static action in `#[client_task(...)`.
///
/// * `#[field(req)]`: **(Mandatory)** Designates the field for the request payload.
///
/// * `#[field(resp)]`: **(Mandatory)** Designates the field for the response payload, which must be an `Option<T>`.
///
/// * `#[field(req_blob)]`: (Optional) Marks a field for an optional request blob. Must implement `AsRef<[u8]>`.
///
/// * `#[field(resp_blob)]`: (Optional) Marks a field for an optional response blob. Must be `Option<T>` where `T` implements `razor_stream::buffer::AllocateBuf`.
///
/// * `#[field(res)]`: (Optional) When used with `#[field(noti)]`, triggers automatic `ClientTaskDone` implementation.
///   Must be of type `Option<Result<(), RpcError<E>>>` where `E` implements `razor_stream::error::RpcErrCodec`. Stores the final result of the task.
///
/// * `#[field(noti)]`: (Optional) When used with `#[field(res)]`, triggers automatic `ClientTaskDone` implementation.
///   Must be an `Option` wrapping a crossfire list channel MTx sender (e.g., `Option<crossfire::MTx<crossfire::mpsc::List<ParentTask>>>`) to notify of task completion.
///   where the task has impl `Into` as `ParentTask` extracted with `crossfire::flavor::Queue::Item`
///
/// ### Example of Automatic `ClientTaskDone`
///
/// ```
/// use razor_stream::error::RpcError;
/// use nix::errno::Errno;
/// use razor_stream_macros::client_task;
/// use serde_derive::{Deserialize, Serialize};
/// use crossfire::{mpsc, MTx};
/// use razor_stream::client::task::*;
///
/// #[derive(Debug, Default, Deserialize, Serialize)]
/// pub struct FileReadReq {
///     pub path: String,
///     pub offset: u64,
///     pub len: u64,
/// }
///
/// #[derive(Debug, Default, Deserialize, Serialize)]
/// pub struct FileReadResp {
///     pub bytes_read: u64,
/// }
///
/// // A task with automatic `ClientTaskDone` implementation.
/// #[client_task(1, debug)]
/// pub struct FileReadTask {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: FileReadReq,
///     #[field(resp)]
///     resp: Option<FileReadResp>,
///     #[field(res)]
///     res: Option<Result<(), RpcError<Errno>>>,
///     #[field(noti)]
///     noti: Option<MTx<mpsc::List<Self>>>,
/// }
///
/// // Usage
/// let (tx, rx) = mpsc::unbounded_blocking::<FileReadTask>();
/// let mut task = FileReadTask {
///     common: ClientTaskCommon { seq: 1, ..Default::default() },
///     req: FileReadReq { path: "/path/to/file".to_string(), offset: 0, len: 1024 },
///     resp: None,
///     res: None,
///     noti: Some(tx),
/// };
///
/// task.set_ok();
/// task.done();
///
/// let completed_task = rx.recv().unwrap();
/// assert_eq!(completed_task.common.seq, 1);
/// assert!(completed_task.res.is_some() && completed_task.res.as_ref().unwrap().is_ok());
/// ```
#[proc_macro_attribute]
pub fn client_task(
    attrs: proc_macro::TokenStream, input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    client_task::client_task_impl(attrs, input)
}

/// # `#[client_task_enum]`
///
/// The `#[client_task_enum]` attribute is applied to an enum to delegate `ClientTask` related trait
/// implementations to its variants. Each variant must wrap a struct that is a valid client task
/// (often decorated with `#[client_task]`)
///
/// This macro generates `From` implementations for each variant, allowing for easy conversion
/// from a specific task struct to the enum. It also delegates methods from `ClientTask`,
/// `ClientTaskEncode`, and `ClientTaskDecode` to the inner task.
///
/// `client_task_enum` also require `error` flag, specified the custom error type for [RpcError<E: RpcErrCodec>](crate::error::RpcError)
///
/// ### `#[action]` on enum variants
///
/// As an alternative to defining the action inside the subtype, you can specify a static action
/// directly on an enum variant using the `#[action(...)]` attribute. Only one action (numeric, or
/// string literal, or numeric enum) is allowed per variant.
///
/// When `#[action(...)]` is used on a variant, the inner type does not need to define an action in this case,
/// but if it does, the enum's action will take precedence.
///
/// ### Example:
///
/// ```rust
/// use razor_stream::client::task::{ClientTask, ClientTaskCommon, ClientTaskAction, ClientTaskDone};
/// use razor_stream::error::RpcError;
/// use nix::errno::Errno;
/// use razor_stream_macros::{client_task, client_task_enum};
/// use serde_derive::{Deserialize, Serialize};
/// use crossfire::{mpsc, MTx};
///
/// #[derive(PartialEq, Debug)]
/// #[repr(u8)]
/// enum FileAction {
///     Open = 1,
///     Close = 2,
/// }
///
/// // Action can be specified in the FileTask enum
/// #[client_task(debug)]
/// pub struct FileOpenTask {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: String,
///     #[field(resp)]
///     resp: Option<()>,
///     #[field(res)]
///     res: Option<Result<(), RpcError<Errno>>>,
///     #[field(noti)]
///     noti: Option<MTx<mpsc::List<FileTask>>>,
/// }
///
/// // Action can be either with client_task
/// #[client_task(FileAction::Close, debug)]
/// pub struct FileCloseTask {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: (),
///     #[field(resp)]
///     resp: Option<()>,
///     #[field(res)]
///     res: Option<Result<(), RpcError<Errno>>>,
///     #[field(noti)]
///     noti: Option<MTx<mpsc::List<FileTask>>>,
/// }
///
/// #[client_task_enum(error=Errno)]
/// #[derive(Debug)]
/// pub enum FileTask {
///     #[action(FileAction::Open)]
///     Open(FileOpenTask),
///     Close(FileCloseTask),
/// }
///
/// // Usage
/// let (tx, rx) = mpsc::unbounded_blocking();
///
/// // Test Open Task
/// let open_task = FileOpenTask {
///     common: ClientTaskCommon::default(),
///     req: "/path/to/file".to_string(),
///     resp: None,
///     res: None,
///     noti: Some(tx.clone()),
/// };
///
/// let mut file_task: FileTask = open_task.into();
/// assert_eq!(file_task.get_action(), razor_stream::proto::RpcAction::Num(1));
/// file_task.set_ok();
/// file_task.done();
///
/// let received = rx.recv().unwrap();
/// assert!(matches!(received, FileTask::Open(_)));
///
/// // Test Close Task
/// let close_task = FileCloseTask {
///     common: ClientTaskCommon::default(),
///     req: (),
///     resp: None,
///     res: None,
///     noti: Some(tx),
/// };
///
/// let mut file_task: FileTask = close_task.into();
/// assert_eq!(file_task.get_action(), razor_stream::proto::RpcAction::Num(2));
/// file_task.set_ok();
/// file_task.done();
///
/// let received = rx.recv().unwrap();
/// assert!(matches!(received, FileTask::Close(_)));
/// ```
#[proc_macro_attribute]
pub fn client_task_enum(
    attrs: proc_macro::TokenStream, input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    client_task_enum::client_task_enum_impl(attrs, input)
}

/// # `#[server_task_enum]`
///
/// The `#[server_task_enum]` macro streamlines the creation of server-side task enums.
/// When applied to an enum, it automatically implements necessary traits for processing RPC requests,
/// reducing boilerplate and improving code maintainability.
///
/// ### Macro Arguments:
///
/// The `server_task_enum` can accept either or both of "req" and "resp" flags, with the following
/// rules:
/// - If `req` is specified, the enum is request task, the macro will impl [ServerTaskDecode](crate::server::task::ServerTaskDecode), [ServerTaskAction](crate::server::task::ServerTaskAction). each variant must have an `#[action(...)]` attribute.
/// - If "resp" is specified, the enum is response task. The macro will impl [ServerTaskEncode](crate::server::task::ServerTaskEncode), [ServerTaskDone](crate::server::task::ServerTaskDone)
/// - If both `req` and `resp` is specified, the response type for `ServerTaskDecode<R>` and `ServerTaskDone<R>` is implicitly `Self` (the enum itself). `resp_type` can be omitted.
/// - If only "req" is specified (and "resp" is not), then `resp_type` must be provided. This `resp_type` specifies the type `<R>` for parameters of `ServerTaskDecode<R>` and `ServerTaskDone<R>`.
///
/// `server_task_enum` also requires `error` flag, specified the custom error type for [RpcError<E: RpcErrCodec>](crate::error::RpcError)
///
/// ### Variant Attributes:
///
/// * `#[action(...)]`: Associates one or more RPC action (which can be numeric, string, or enum value) with an enum variant.
///   Multiple actions can be specified (e.g., `#[action(1, 2 )]`).
///   If there's more than one actions. The subtype should store its action and implement ServerTaskAction.
///
/// ### Example:
///
/// ```rust
/// use razor_stream::server::task::ServerTaskVariant;
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// use nix::errno::Errno;
///
/// #[derive(PartialEq)]
/// #[repr(u8)]
/// enum FileAction {
///     Open=1,
///     Read=2,
///     Write=3,
///     Truncate=4,
/// }
///
/// #[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
/// struct FileOpenReq { pub path: String, }
///
/// #[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
/// struct FileIOReq { pub path: String, pub offset: u64, pub len: u64, }
///
///
/// #[server_task_enum(req, resp, error=Errno)]
/// #[derive(Debug)]
/// pub enum FileTask {
///     #[action(FileAction::Open)]
///     Open(ServerTaskVariant<FileTask, FileOpenReq, Errno>),
///     #[action(FileAction::Read, FileAction::Write, FileAction::Truncate)]
///     Read(ServerTaskVariant<FileTask, FileIOReq, Errno>),
/// }
/// ```
#[proc_macro_attribute]
pub fn server_task_enum(
    attrs: proc_macro::TokenStream, input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    server_task_enum::server_task_enum_impl(attrs, input)
}
