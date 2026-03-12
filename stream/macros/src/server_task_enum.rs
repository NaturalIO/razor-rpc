use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::{Data, DeriveInput, Fields, Meta, NestedMeta, Variant, parse_macro_input};

struct ServerTaskEnumAttrs {
    req: bool,
    resp: bool,
    resp_type: Option<syn::Type>,
    error: syn::Type,
}

impl syn::parse::Parse for ServerTaskEnumAttrs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut req = false;
        let mut resp = false;
        let mut resp_type = None;
        let mut error = None;

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                if ident == "req" {
                    req = true;
                } else if ident == "resp" {
                    resp = true;
                } else if ident == "resp_type" {
                    input.parse::<syn::Token![=]>()?;
                    resp_type = Some(input.parse()?);
                } else if ident == "error" {
                    input.parse::<syn::Token![=]>()?;
                    error = Some(input.parse()?);
                } else {
                    return Err(input.error(format!("unexpected attribute: {}", ident)));
                }
            } else if lookahead.peek(syn::Token![,]) {
                input.parse::<syn::Token![,]>()?;
            } else {
                return Err(input.error("unexpected token"));
            }
        }

        let error = error.ok_or_else(|| {
            input.error("#[server_task_enum] requires an `error = <Type>` attribute")
        })?;

        Ok(ServerTaskEnumAttrs { req, resp, resp_type, error })
    }
}

pub fn server_task_enum_impl(attrs: TokenStream, input: TokenStream) -> TokenStream {
    let mut ast = parse_macro_input!(input as DeriveInput);
    let enum_name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let variants = if let Data::Enum(ref mut data) = ast.data {
        &mut data.variants
    } else {
        panic!("#[server_task_enum] can only be applied to enums");
    };

    let mut from_impls = Vec::new();
    let mut decode_arms = Vec::new();
    let mut get_action_arms = Vec::new();
    let mut encode_arms = Vec::new();
    let mut set_result_arms = Vec::new();
    let mut where_clauses_for_decode = Vec::new();

    let mut inner_type_counts: HashMap<String, usize> = HashMap::new();
    for variant in variants.iter() {
        let inner_type = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => panic!("Enum variants must be tuple-style with a single field"),
        };
        let inner_type_str = quote! {#inner_type}.to_string();
        *inner_type_counts.entry(inner_type_str).or_insert(0) += 1;
    }

    let macro_attrs = parse_macro_input!(attrs as ServerTaskEnumAttrs);
    let has_req = macro_attrs.req;
    let has_resp = macro_attrs.resp;
    let error_type = macro_attrs.error.clone();

    let resp_type = if has_resp {
        if macro_attrs.resp_type.is_some() {
            panic!("Cannot specify 'resp_type' when 'resp' is present. Response type is Self.");
        }
        quote! { #enum_name }
    } else {
        let r_type =
            macro_attrs.resp_type.expect("resp_type must be specified when 'resp' is not present");
        quote! { #r_type }
    };

    for variant in variants.iter_mut() {
        let variant_name = &variant.ident;
        let inner_type = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            _ => panic!("Enum variants must be tuple-style with a single field"),
        };

        if has_req {
            let actions = get_action_attribute(variant);
            variant.attrs.retain(|attr| !attr.path.is_ident("action"));
            // Logic for decode_arms
            for action_meta in &actions {
                let action_token_stream = match action_meta {
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
                        quote! { razor_stream::proto::RpcAction::Num(val) if val == (#p as i32) }
                    }
                    _ => panic!(
                        "Unsupported action type for decode_arms. Only string/integer literals and enum variants are supported."
                    ),
                };
                decode_arms.push(quote! {
                            #action_token_stream => {
                                let task = <#inner_type as razor_stream::server::task::ServerTaskDecode<#resp_type>>::decode_req::<C>(codec, action, seq, req, blob, noti)?;
                                Ok(#enum_name::#variant_name(task))
                            }
                        });
            }

            // Logic for where_clauses_for_decode (conditional)
            let inner_type_exists =
                matches!(&variant.fields, Fields::Unnamed(fields) if fields.unnamed.len() == 1);

            if actions.len() > 1 || (actions.is_empty() && inner_type_exists) {
                where_clauses_for_decode.push(quote! {
                    #inner_type: razor_stream::server::task::ServerTaskDecode<#resp_type> + razor_stream::server::task::ServerTaskAction
                });
            } else {
                where_clauses_for_decode.push(quote! {
                    #inner_type: razor_stream::server::task::ServerTaskDecode<#resp_type>
                });
            }

            // Logic for get_action_arms (conditional and RpcAction return type)
            if actions.len() > 1 {
                get_action_arms.push(quote! {
                    #enum_name::#variant_name(inner) => inner.get_action(),
                });
            } else if actions.len() == 1 {
                let action_meta = &actions[0];
                let action_token_stream = match action_meta {
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
                        "Unsupported action type for get_action. Only string/integer literals and enum variants are supported."
                    ),
                };
                get_action_arms.push(quote! {
                    #enum_name::#variant_name(_) => #action_token_stream,
                });
            } else {
                panic!("Must specify #[action] attribute for req case");
            }
        }

        let inner_type_str = quote! {#inner_type}.to_string();
        let count = *inner_type_counts.get(&inner_type_str).unwrap_or(&0);
        if count == 1 {
            // Only generate if count is 1, prevent duplicate sub-types
            from_impls.push(quote! {
                impl #impl_generics From<#inner_type> for #enum_name #ty_generics #where_clause {
                    #[inline]
                    fn from(task: #inner_type) -> Self {
                        #enum_name::#variant_name(task)
                    }
                }
            });
        } else if count > 1 {
            // Explicitly panic if a duplicate sub-type is found
            panic!(
                "Duplicate sub-type `{}` found in enum `{}`. `From` implementation cannot be generated for duplicate sub-types.",
                inner_type_str, enum_name
            );
        }
        if has_resp {
            encode_arms.push(quote! {
                #enum_name::#variant_name(task) => task.encode_resp(codec, buf),
            });

            set_result_arms.push(quote! {
                #enum_name::#variant_name(task) => task._set_result(res),
            });
        }
    }

    let req_impl = if has_req {
        quote! {

            impl #impl_generics razor_stream::server::task::ServerTaskDecode<#resp_type> for #enum_name #ty_generics #where_clause
            where
                #(#where_clauses_for_decode),*
            {
                #[inline]
                fn decode_req<'a, C: razor_stream::Codec>(
                    codec: &'a C,
                    action: razor_stream::proto::RpcAction<'a>,
                    seq: u64,
                    req: &'a [u8],
                    blob: Option<io_buffer::Buffer>,
                    noti: razor_stream::server::task::RespNoti<#resp_type>,
                ) -> Result<Self, ()> {
                    match action {
                        #(#decode_arms)*
                        _ => {
                            log::error!("Unknown action: {:?}", action);
                            Err(())
                        }
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    let resp_impl = if has_resp {
        quote! {
            impl #impl_generics razor_stream::server::task::ServerTaskResp for #enum_name #ty_generics #where_clause {}

            impl #impl_generics razor_stream::server::task::ServerTaskEncode for #enum_name #ty_generics #where_clause {
                #[inline]
                fn encode_resp<'a, 'b, C: razor_stream::Codec>(
                    &'a mut self,
                    codec: &C,
                    buf: &'b mut Vec<u8>,
                ) -> (u64, Result<(usize, Option<&'a [u8]>), razor_stream::error::EncodedErr>) {
                    match self {
                        #(#encode_arms)*
                    }
                }
            }

            impl #impl_generics razor_stream::server::task::ServerTaskDone<#resp_type, #error_type> for #enum_name #ty_generics #where_clause {
                #[inline]
                fn _set_result(&mut self, res: Result<(), #error_type>) -> razor_stream::server::task::RespNoti<#resp_type> {
                    match self {
                        #(#set_result_arms)*
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    let get_action_impl = if has_req {
        quote! {
            impl #impl_generics razor_stream::server::task::ServerTaskAction for #enum_name #ty_generics #where_clause {
                #[inline]
                fn get_action<'a>(&'a self) -> razor_stream::proto::RpcAction<'a> {
                    match self {
                        #(#get_action_arms)*
                    }
                }
            }
        }
    } else {
        quote! {}
    };

    let expanded = quote! {
        #ast

        #(#from_impls)*

        #req_impl

        #resp_impl

        #get_action_impl
    };
    TokenStream::from(expanded)
}

fn get_action_attribute(variant: &Variant) -> Vec<NestedMeta> {
    for attr in &variant.attrs {
        if attr.path.is_ident("action")
            && let Ok(Meta::List(meta_list)) = attr.parse_meta()
        {
            let actions: Vec<NestedMeta> = meta_list.nested.into_iter().collect();
            if !actions.is_empty() {
                return actions;
            }
        }
    }
    // No action value needed if no req
    Vec::new()
}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// #[server_task_enum(req)]
/// pub struct NotAnEnum;
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_not_an_enum() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// struct MyMsg;
/// #[server_task_enum(req)]
/// pub enum EnumNoErrorType {
///     #[action(1)]
///     Task1, // Unit variant
///     #[action(2)]
///     Task2(MyMsg, MyMsg), // Multiple fields
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_no_error_type() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// struct MyMsg;
/// #[server_task_enum(req, error=())]
/// pub enum InvalidVariantFieldCount {
///     #[action(1)]
///     Task1, // Unit variant
///     #[action(2)]
///     Task2(MyMsg, MyMsg), // Multiple fields
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_invalid_variant_field_count() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// struct MyMsg;
/// #[server_task_enum(req, resp, resp_type=MyMsg, error=())]
/// pub enum RespAndRespType {
///     #[action(1)]
///     Task1(MyMsg),
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_resp_and_resp_type() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// struct MyMsg;
/// #[server_task_enum(req, error=())] // Missing resp_type
/// pub enum MissingRespType {
///     #[action(1)]
///     Task1(MyMsg),
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_resp_type() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use serde_derive::{Deserialize, Serialize};
/// #[derive(Serialize, Deserialize)]
/// struct MyMsg;
/// #[server_task_enum(req, error=())]
/// pub enum MissingActionAttribute {
///     Task1(MyMsg),
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_missing_action_attribute() {}

/// ```compile_fail
/// use razor_stream_macros::server_task_enum;
/// use razor_stream::server::task::ServerTaskVariant;
/// use razor_stream::server::task::RespNoti;
/// use razor_stream::proto::RpcActionOwned;
/// use serde_derive::{Deserialize, Serialize};
///
/// #[derive(Default, Deserialize, Serialize)]
/// pub struct MyServerReq;
///
/// #[server_task_enum(req, resp_type=MyServerResp, error=())]
/// pub enum MyServerEnumTask {
///     #[action(1)]
///     VariantA(ServerTaskVariant<MyServerResp, MyServerReq, ()>),
///     #[action(2)]
///     VariantB(ServerTaskVariant<MyServerResp, MyServerReq, ()>), // Duplicate sub-type MyServerReq
/// }
///
/// #[derive(Default, Debug)]
/// pub struct MyServerResp;
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_server_task_enum_duplicate_subtype() {}
