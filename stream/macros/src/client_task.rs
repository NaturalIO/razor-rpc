use proc_macro::TokenStream;
use quote::quote;
use syn::{AttributeArgs, DeriveInput, Fields, Ident, Meta, NestedMeta, Type, parse_macro_input};

fn check_option_inner_type(ty: &syn::Type) -> bool {
    if let syn::Type::Path(syn::TypePath { qself: None, path }) = ty
        && path.leading_colon.is_none()
        && path.segments.len() == 1
        && path.segments[0].ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &path.segments[0].arguments
        && args.args.len() == 1
        && let syn::GenericArgument::Type(_inner_ty) = &args.args[0]
    {
        return true;
    }
    false
}

pub fn client_task_impl(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as AttributeArgs);
    let mut ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    let has_generics = !ast.generics.params.is_empty();

    let (impl_generics_for_impl, ty_generics_for_impl, where_clause_for_impl) = if has_generics {
        (quote! { #impl_generics }, quote! { #ty_generics }, quote! { #where_clause })
    } else {
        (quote! {}, quote! {}, quote! {})
    };
    let mut common_field: Option<(Ident, Type)> = None;
    let mut req_field: Option<Ident> = None;
    let mut resp_field: Option<(Ident, Type)> = None;
    let mut req_blob_field: Option<Ident> = None;
    let mut resp_blob_field: Option<(Ident, Type)> = None;
    let mut field_action: Option<(Ident, Type)> = None; // For #[field(action)]
    let mut static_action: Option<NestedMeta> = None; // For #[client_task(action)]
    let mut res_field: Option<(Ident, Type)> = None;
    let mut noti_field: Option<(Ident, Type)> = None;
    let mut gen_debug = false;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::Path(path)) if path.is_ident("debug") => {
                if gen_debug {
                    panic!("Duplicate `debug` attribute argument");
                }
                gen_debug = true;
            }
            action_arg => {
                if static_action.is_some() {
                    panic!("Only one action can be specified.");
                }
                static_action = Some(action_arg);
            }
        }
    }

    if let syn::Data::Struct(syn::DataStruct { fields: Fields::Named(fields), .. }) = &mut ast.data
    {
        for field in fields.named.iter_mut() {
            let mut new_attrs = Vec::new();
            for attr in &field.attrs {
                if attr.path.is_ident("field") {
                    if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
                        for nested in meta_list.nested.iter() {
                            if let NestedMeta::Meta(Meta::Path(p)) = nested
                                && let Some(ident) = p.get_ident()
                            {
                                let f_name = field.ident.as_ref().unwrap().clone();
                                let f_type = field.ty.clone();
                                match ident.to_string().as_str() {
                                    "common" => common_field = Some((f_name, f_type)),
                                    "req" => req_field = Some(f_name),
                                    "resp" => resp_field = Some((f_name, f_type)),
                                    "req_blob" => req_blob_field = Some(f_name),
                                    "resp_blob" => resp_blob_field = Some((f_name, f_type)),
                                    "action" => {
                                        // Handle #[field(action)]
                                        if field_action.is_some() {
                                            panic!("Only one #[field(action)] is allowed.");
                                        }
                                        field_action = Some((f_name, f_type));
                                    }
                                    "res" => res_field = Some((f_name, f_type)),
                                    "noti" => noti_field = Some((f_name, f_type)),
                                    _ => {}
                                }
                            }
                        }
                    }
                } else {
                    new_attrs.push(attr.clone());
                }
            }
            field.attrs = new_attrs;
        }
    }

    // Check for mutual exclusivity
    if field_action.is_some() && static_action.is_some() {
        panic!("Cannot specify both #[field(action)] and #[client_task(action = ...)] attributes.");
    }

    if res_field.is_some() != noti_field.is_some() {
        panic!("#[field(res)] and #[field(noti)] must be specified together.");
    }

    let (common_field_name, common_field_type) =
        common_field.expect("common field must be tagged with #[field(common)]");
    let req_field_name = req_field.expect("req field must be tagged with #[field(req)]");
    let (resp_field_name, resp_field_type) =
        resp_field.expect("resp field must be tagged with #[field(resp)]");

    if !check_option_inner_type(&resp_field_type) {
        panic!("`{}::{}` resp field must be of type Option<T>", struct_name, resp_field_name);
    }

    let get_req_blob_body = if let Some(req_blob_field_name) = req_blob_field {
        quote! {
            #[inline]
            fn get_req_blob(&self) -> Option<&[u8]> {
                Some(self.#req_blob_field_name.as_ref())
            }
        }
    } else {
        quote! {}
    };

    let reserve_resp_blob_body = if let Some((resp_blob_field_name, _)) = &resp_blob_field {
        quote! {
            #[inline]
            fn reserve_resp_blob(&mut self, size: i32) -> Option<&mut [u8]> {
                razor_stream::buffer::AllocateBuf::reserve(&mut self.#resp_blob_field_name, size)
            }
        }
    } else {
        quote! {}
    };

    let client_task_action_impl = if let Some((f_action_name, f_action_type)) = field_action {
        let action_conversion = if let Type::Path(type_path) = &f_action_type {
            if let Some(segment) = type_path.path.segments.last() {
                if segment.ident == "String" {
                    quote! { razor_stream::proto::RpcAction::Str(&self.#f_action_name) }
                } else {
                    // Assume numeric or enum
                    quote! { razor_stream::proto::RpcAction::Num(self.#f_action_name as i32) }
                }
            } else {
                panic!("Unsupported type for #[field(action)]");
            }
        } else {
            panic!("Unsupported type for #[field(action)]");
        };

        quote! {
            impl #impl_generics_for_impl razor_stream::client::task::ClientTaskAction for #struct_name #ty_generics_for_impl #where_clause_for_impl {
                #[inline]
                fn get_action<'a>(&'a self) -> razor_stream::proto::RpcAction<'a> {
                    #action_conversion
                }
            }
        }
    } else if let Some(s_action) = static_action {
        let action_token_stream = match s_action {
            NestedMeta::Lit(syn::Lit::Str(s)) => {
                let action_str = s.value();
                quote! { razor_stream::proto::RpcAction::Str(#action_str) }
            }
            NestedMeta::Lit(syn::Lit::Int(i)) => {
                let action_int =
                    i.base10_parse::<i32>().expect("Invalid integer literal for action");
                quote! { razor_stream::proto::RpcAction::Num(#action_int) }
            }
            NestedMeta::Meta(syn::Meta::Path(p)) => {
                quote! { razor_stream::proto::RpcAction::Num(#p as i32) }
            }
            _ => panic!(
                "Unsupported action type in #[action(...)]. Only string/integer literals and enum variants are supported."
            ),
        };
        quote! {
            impl #impl_generics_for_impl razor_stream::client::task::ClientTaskAction for #struct_name #ty_generics_for_impl #where_clause_for_impl {
                #[inline]
                fn get_action<'a>(&'a self) -> razor_stream::proto::RpcAction<'a> {
                    #action_token_stream
                }
            }
        }
    } else {
        quote! {}
    };

    let client_task_done_impl = if let (
        Some((res_field_name, res_field_type)),
        Some((noti_field_name, noti_full_type)),
    ) = (&res_field, &noti_field)
    {
        let error_type = {
            let res_type_path = if let syn::Type::Path(p) = res_field_type {
                p
            } else {
                panic!("res field must be a path type")
            };
            let last_segment =
                res_type_path.path.segments.last().expect("res field type path must have segments");
            if last_segment.ident != "Option" {
                panic!("res field must be an Option")
            };

            let option_inner_type =
                if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                    if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                        ty
                    } else {
                        panic!("Option needs a type argument")
                    }
                } else {
                    panic!("Option needs angle bracketed arguments")
                };

            let result_type_path = if let syn::Type::Path(p) = option_inner_type {
                p
            } else {
                panic!("res field's inner type must be a Result")
            };
            let last_segment =
                result_type_path.path.segments.last().expect("Result type path must have segments");
            if last_segment.ident != "Result" {
                panic!("res field must be an Option<Result<...>>")
            };

            let result_args =
                if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                    &args.args
                } else {
                    panic!("Result needs angle bracketed arguments")
                };
            if result_args.len() != 2 {
                panic!("Result needs two type arguments")
            };

            if let syn::GenericArgument::Type(syn::Type::Tuple(ok_ty)) = &result_args[0] {
                if !ok_ty.elems.is_empty() {
                    panic!("Result's Ok type must be ()")
                };
            } else {
                panic!("Result's Ok type must be ()")
            };

            let rpc_error_type = if let syn::GenericArgument::Type(ty) = &result_args[1] {
                ty
            } else {
                panic!("Could not get error type from Result")
            };
            let rpc_error_path = if let syn::Type::Path(p) = rpc_error_type {
                p
            } else {
                panic!("Result's error type must be RpcError")
            };
            let last_segment =
                rpc_error_path.path.segments.last().expect("RpcError type path must have segments");
            if last_segment.ident != "RpcError" {
                panic!("Result's error type must be RpcError")
            };

            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                    ty
                } else {
                    panic!("RpcError needs a type argument")
                }
            } else {
                panic!("RpcError needs angle bracketed arguments")
            }
        };

        let noti_inner_type = match noti_full_type {
            Type::Path(type_path) => {
                let segment = type_path
                    .path
                    .segments
                    .last()
                    .expect("noti field type path must have segments");
                if segment.ident != "Option" {
                    panic!("noti field must be of type Option<MTx<T>>");
                }
                let args = match &segment.arguments {
                    syn::PathArguments::AngleBracketed(args) => args,
                    _ => panic!("Option needs angle bracketed arguments"),
                };
                let inner_option_type = match args.args.first() {
                    Some(syn::GenericArgument::Type(ty)) => ty,
                    _ => panic!("Option needs a type argument"),
                };
                let inner_type_path = match inner_option_type {
                    Type::Path(p) => p,
                    _ => panic!("Inner type of Option must be a path type"),
                };
                let inner_segment = inner_type_path
                    .path
                    .segments
                    .last()
                    .expect("Inner type path must have segments");
                if inner_segment.ident != "MTx" {
                    panic!("Inner type of Option must be MTx");
                }
                let inner_args = match &inner_segment.arguments {
                    syn::PathArguments::AngleBracketed(args) => args,
                    _ => panic!("MTx needs angle bracketed arguments"),
                };
                match inner_args.args.first() {
                    Some(syn::GenericArgument::Type(ty)) => ty.clone(),
                    _ => panic!("MTx needs a type argument"),
                }
            }
            _ => panic!("noti field must be a path type"),
        };

        let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

        let where_clause_with_into = if where_clause.is_some() {
            quote! { #where_clause, Self: Into<<#noti_inner_type as crossfire::flavor::Queue>::Item> }
        } else {
            quote! { where Self: Into<<#noti_inner_type as crossfire::flavor::Queue>::Item> }
        };

        quote! {
            impl #impl_generics razor_stream::client::task::ClientTaskGetResult<#error_type> for #struct_name #ty_generics #where_clause {
                #[inline]
                fn get_result(&self) -> Result<(), &razor_stream::error::RpcError<#error_type>> {
                    match self.#res_field_name.as_ref() {
                        Some(Ok(())) => Ok(()),
                        Some(Err(e)) => Err(e),
                        None => Err(&razor_stream::error::RpcError::Rpc(razor_stream::error::RpcIntErr::Internal)),
                    }
                }
            }

            impl #impl_generics razor_stream::client::task::ClientTaskDone for #struct_name #ty_generics #where_clause_with_into {
                #[inline]
                fn set_custom_error<C: razor_stream::Codec>(&mut self, codec: &C, res: razor_stream::error::EncodedErr, _last_index: usize, _conf_ver: u64) {
                    let rpc_error = match res {
                        razor_stream::error::EncodedErr::Rpc(e) => e.into(),
                        razor_stream::error::EncodedErr::Num(n) => {
                            if let Ok(e) = <#error_type as razor_stream::error::RpcErrCodec>::decode(codec, Ok(n as u32)) {
                                e.into()
                            } else {
                                razor_stream::error::RpcIntErr::Decode.into()
                            }
                        }
                        razor_stream::error::EncodedErr::Static(s) => {
                            if let Ok(e) = <#error_type as razor_stream::error::RpcErrCodec>::decode(codec, Err(s.as_bytes())) {
                                e.into()
                            } else {
                                razor_stream::error::RpcIntErr::Decode.into()
                            }
                        }
                        razor_stream::error::EncodedErr::Buf(b) => {
                            if let Ok(e) = <#error_type as razor_stream::error::RpcErrCodec>::decode(codec, Err(&b)) {
                                e.into()
                            } else {
                                razor_stream::error::RpcIntErr::Decode.into()
                            }
                        }
                    };
                    self.#res_field_name = Some(Err(rpc_error));
                }

                #[inline]
                fn set_rpc_error(&mut self, e: razor_stream::error::RpcIntErr) {
                    self.#res_field_name = Some(Err(e.into()));
                }

                #[inline]
                fn set_ok(&mut self) {
                    self.#res_field_name = Some(Ok(()));
                }

                #[inline]
                fn done(mut self) {
                    if let Some(noti) = self.#noti_field_name.take() {
                        let _ = noti.send(self.into());
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    let debug_impl = if gen_debug {
        quote! {
            impl #impl_generics_for_impl std::fmt::Debug for #struct_name #ty_generics_for_impl #where_clause_for_impl {
                fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    write!(f, "{}(seq={} req={:?}", stringify!(#struct_name), self.seq, &self.#req_field_name)?;
                    if let Some(resp) = &self.#resp_field_name {
                        write!(f, " resp={:?}", resp)?;
                    }
                    write!(f, ")")
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #ast

        #debug_impl

        #client_task_action_impl

        #client_task_done_impl

        impl #impl_generics_for_impl std::ops::Deref for #struct_name #ty_generics_for_impl #where_clause_for_impl {
            type Target = #common_field_type;
            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.#common_field_name
            }
        }

        impl #impl_generics_for_impl std::ops::DerefMut for #struct_name #ty_generics_for_impl #where_clause_for_impl {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.#common_field_name
            }
        }

        impl #impl_generics_for_impl razor_stream::client::task::ClientTaskEncode for #struct_name #ty_generics_for_impl #where_clause_for_impl {
            #[inline]
            fn encode_req<C: razor_stream::Codec>(&self, codec: &C, buf: &mut Vec<u8>) -> Result<usize, ()> {
                codec.encode_into(&self.#req_field_name, buf)
            }

            #get_req_blob_body
        }

        impl #impl_generics_for_impl razor_stream::client::task::ClientTaskDecode for #struct_name #ty_generics_for_impl #where_clause_for_impl {
            #[inline]
            fn decode_resp<C: razor_stream::Codec>(&mut self, codec: &C, buffer: &[u8]) -> Result<(), ()> {
                let resp = codec.decode(buffer)?;
                self.#resp_field_name = Some(resp);
                Ok(())
            }

            #reserve_resp_blob_body
        }
    };
    TokenStream::from(expanded)
}

/// ```compile_fail
/// use razor_stream_macros::*;
/// #[client_task]
/// pub struct FileTaskWrongResp {
///     #[field(common)]
///     common: razor_stream::client::task::ClientTaskCommon,
///     #[field(req)]
///     req: (),
///     #[field(resp)]
///     resp: (),
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_resp_not_option() {}

/// ```compile_fail
/// use razor_stream_macros::*;
/// #[client_task]
/// pub struct FileTaskNoReq {
///     #[field(common)]
///     common: razor_stream::client::task::ClientTaskCommon,
///     #[field(resp)]
///     resp: Option<()>,
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_req() {}

/// ```compile_fail
/// use razor_stream_macros::*;
/// #[client_task]
/// pub struct FileTaskNoResp {
///     #[field(common)]
///     common: razor_stream::client::task::ClientTaskCommon,
///     #[field(req)]
///     req: (),
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_resp() {}

/// ```compile_fail
/// use razor_stream_macros::*;
/// use razor_stream::client::task::ClientTaskCommon;
/// use razor_stream::error::RpcError;
/// use nix::errno::Errno;
///
/// #[client_task]
/// pub struct MissingNotiField {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: (),
///     #[field(resp)]
///     resp: Option<()>,
///     #[field(res)]
///     res: Option<Result<(), RpcError<Errno>>>,
/// }
/// ```#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_noti_field() {}

/// ```compile_fail
/// use razor_stream_macros::*;
/// use razor_stream::client::task::ClientTaskCommon;
/// use crossfire::{MTx, mpsc};
///
/// #[client_task]
/// pub struct MissingResField {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: (),
///     #[field(resp)]
///     resp: Option<()>,
///     #[field(noti)]
///     noti: Option<MTx<mpsc::List<Self>>>,
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_res_field() {}
