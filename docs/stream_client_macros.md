# Rpc Client Task Macro Design

The `#[client_task_enum]` and `#[client_task]` procedural macro simplifies the implementation of client-side RPC tasks by automatically generating necessary trait implementations and boilerplate code.

`#[client_task]` attribute allows users to define a struct representing an RPC task and specify its common fields, request, response, and optional blob data using `#[field(...)]` attributes.
It will not generate ClientTask trait because the ClientTaskAction is optional for task variants.

`#[client_task_enum]` attribute allows users to wrap an enum and delegate the ClientTask trait requirements to its variants.
It will generate the ClientTask trait and the ClientTaskAction according to the #[action()] specified for enum variants.

### `#[action]` on enum variants

As an alternative to defining the action inside the subtype, you can specify a static action directly on an enum variant using the `#[action(...)]` attribute. Only one action (numeric, string literal, or a numeric `repr` enum variant) is allowed per variant.

When `#[action(...)]` is used on a variant, the `get_action()` method for that variant will return the specified static action, and the inner type does not need to provide an action.

## Usage Example

Here's an example of a struct using the `#[client_task]` macro:

```rust

#[client_task_enum]
pub enum FileTask {
    #[action(1)]
    Open(FileOpenTask),
    #[action("io_op")]
    IO(FileIOTask),
    // This variant delegates action to the inner type
    Close(FileCloseTask),
}

// This task does not need to specify an action, as it is handled by the enum variant.
#[client_task]
pub struct FileOpenTask {
    #[field(common)]
    common: TaskCommon,
    #[field(req)]
    req: String,
    #[field(resp)]
    resp: Option<()>
}

#[client_task]
pub struct FileIOTask {
    #[field(common)]
    common: TaskCommon,
    #[field(req)]
    req: FileIOReq,
    #[field(resp)]
    resp: Option<FileIOResp>,
}

// This task must specify its own action because the enum variant does not.
#[client_task(action = 3)]
pub struct FileCloseTask {
    #[field(common)]
    common: TaskCommon,
    #[field(req)]
    req: (),
    #[field(resp)]
    resp: Option<()>
}

```

## Field Attributes of `#[client_task]`

The macro processes specific fields within the task struct based on the `#[field(...)]` attributes:

*   `#[field(common)]`:
    *   **Purpose:** Designates a field that holds common task-related information.
    *   **Requirements:** This field is mandatory. The macro generates `AsMut<ClientTaskCommon>` implementations for the task struct, delegating to this `common` field. This allows direct access to the common fields of the task struct.

*   `#[field(action)]`
    *   Purpose: Implement the ClientTaskAction trait to return an RpcAction according to the field.
    *   Requirements: There is no `action=` specified within the `client_task` attribute. The field is a numeric or string type, or an enum type that is represented by a number.

*   attribute parameter `action` in `#[client_task(action=...)]`
    *   Purpose: Implement the ClientTaskAction trait with a static value.
    *   Requirements: There is no `#[field(action)]` specified within the struct. The `action()` parameter must be only one, and can be numeric, a string, or an enum represented by a number.

*   `#[field(req)]`:
    *   **Purpose:** Designates the field containing the request payload for the RPC call.
    *   **Requirements:** This field is mandatory. The macro uses this field to implement the `ClientTaskEncode::encode_req` method, serializing its content using the provided `Codec`.

*   `#[field(resp)]`:
    *   **Purpose:** Designates the field where the decoded response from the RPC call will be stored.
    *   **Requirements:** This field is mandatory and *must* be of type `Option<T>`. The macro implements `ClientTaskDecode::decode_resp`, which decodes the incoming buffer into `T` and sets it to `Some(decoded_value)` in this field.

*   `#[field(req_blob)]`:
    *   **Purpose:** Designates an optional field for sending additional binary data (a "request blob") with the RPC request.
    *   **Requirements:** This field is optional. If present, its type must implement `AsRef<u8>`. The macro implements `ClientTaskEncode::get_req_blob`, returning a reference to this blob.

*   `#[field(resp_blob)]`:
    *   **Purpose:** Designates an optional field for receiving additional binary data (a "response blob") with the RPC response.
    *   **Requirements:** This field is optional. If present, its type *must* be `Option<T>`, where `T` implements `razor_rpc::io::AllocateBuf`. The macro implements `ClientTaskDecode::get_resp_blob_mut`, returning a mutable reference to this `Option<T>`.

*  `#[field(res)]` and `#[field(noti)]`:
    *   **Purpose:** When both are present, the macro generates an implementation of the `ClientTaskDone` trait. This is used to signal the completion of a task and deliver its result.
    *   **Requirements:**
        *   The field marked with `#[field(res)]` must be of type `Option<Result<(), RpcError<E>>>` where `E` implements `RpcErrCodec`. This field will be populated with the task's outcome (`Ok(())` for success, `Err(RpcError<E>)` for failure).
        *   The field marked with `#[field(noti)]` must be an `Option` wrapping a channel sender (e.g., `Option<crossfire::mpsc::MTx<Self>>`). When the task is completed, the entire task struct (with the `res` field populated) is sent through this channel.


## Generated Trait Implementations

The `#[client_task]` macro automatically generates the following trait implementations for the decorated struct:

*   `AsMut<ClientTaskCommon>`:
    *   These traits are implemented to delegate to the field marked with `#[field(common)]`, providing convenient access to its members.

*   `ClientTaskEncode`:
    *   `encode_req<C: razor_rpc::codec::Codec>(&self, codec: &C) -> Result<Vec<u8>, ()>`: Encodes the content of the `#[field(req)]` field using the provided codec.
    *   `get_req_blob(&self) -> Option<&[u8]>`: If `#[field(req_blob)]` is present, returns a `Some` reference to the blob data; otherwise, it returns `None`.

*   `ClientTaskDecode`:
    *   `decode_resp<C: razor_rpc::codec::Codec>(&mut self, codec: &C, buffer: &[u8]) -> Result<(), ()>`: Decodes the response buffer using the provided codec and stores the result in the `#[field(resp)]` field.
    *   `get_resp_blob_mut(&mut self) -> Option<&mut impl razor_rpc::io::AllocateBuf>`: If `#[field(resp_blob)]` is present, returns a `Some` mutable reference to the response blob `Option<T>`; otherwise, it returns `None`.

*   `ClientTaskDone`:
    *   This trait is implemented when `#[field(res)]` and `#[field(noti)]` are present.
    *   The generated implementation provides the following methods:
        ```rust
        impl<E: razor_stream::error::RpcErrCodec> razor_stream::client::ClientTaskDone for T {
            fn set_custom_error<C: razor_rpc_core::Codec>(&mut self, codec: &C, res: razor_stream::EncodedErr) { /* ... */ }
            fn set_rpc_error(&mut self, e: razor_stream::RpcIntErr) { /* ... */ }
            fn set_ok(&mut self) { /* ... */ }
            fn done(self) { /* ... */ }
        }
        ```

*   when `#[client_task(debug)]` is specified, you will get a more specific Debug generated according to the `req` and `resp` fields
    ```rust
    impl std::fmt::Debug for T {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}(seq={} {:?}", std::any::type_name::<Self>(), self.seq, self.req)?;
            if let Some(resp) = self.resp.as_ref() {
                write!(f, " {:?})", self.resp)?;
            }
            Ok(())
        }
    }
    ```


The `#[client_task_enum]` will implement `From` to assist conversion from its variants to the parent enum, and delegate `ClientTaskEncode`, `ClientTaskDecode`, `ClientTaskAction`, and `RpcClientTask` to its variants.

## User Responsibilities

Users are responsible for implementing the `razor_stream::RpcErrCodec` trait for their custom error types, which are then used with `RpcError<E>`. The framework handles calling `set_ok()`, `set_rpc_error()`, `set_custom_error()`, and `done()` to signal task completion.

If the `#[field(res)]` and `#[field(noti)]` attributes are not used, the user must implement the `ClientTaskDone` trait manually. When these attributes are used, the macro generates this implementation automatically.

**Note:** The implementation of `ClientTaskAction` is optional for structs decorated with `#[client_task]`. If `ClientTaskAction` is not implemented by the struct, then the `#[client_task_enum]` variant wrapping it must provide an `#[action]` attribute.
