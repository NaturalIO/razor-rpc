use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::{Data, DeriveInput, Fields, Lit, Meta, NestedMeta, Variant, parse_macro_input};

fn get_action_attribute(variant: &Variant) -> Option<Meta> {
    for attr in &variant.attrs {
        if attr.path.is_ident("action")
            && let Ok(Meta::List(meta_list)) = attr.parse_meta()
        {
            let nested = meta_list.nested.first().cloned();
            if let Some(NestedMeta::Lit(lit)) = nested {
                if meta_list.nested.len() > 1 {
                    panic!("Only one action is allowed per variant");
                }
                return Some(Meta::NameValue(syn::MetaNameValue {
                    path: syn::Path::from(syn::Ident::new(
                        "action",
                        proc_macro2::Span::call_site(),
                    )),
                    eq_token: syn::token::Eq::default(),
                    lit,
                }));
            } else if let Some(NestedMeta::Meta(Meta::Path(path))) = nested {
                // Handle enum variant like Action::Open
                return Some(Meta::Path(path));
            }
        }
    }
    None
}

pub fn client_task_enum_impl(attr: TokenStream, input: TokenStream) -> TokenStream {
    let mut error_ty: Option<syn::Type> = None;
    let parser = |input: syn::parse::ParseStream| {
        let lookahead = input.lookahead1();
        if lookahead.peek(syn::Ident) {
            let ident: syn::Ident = input.parse()?;
            if ident == "error" {
                input.parse::<syn::Token![=]>()?;
                error_ty = Some(input.parse()?);
            }
        }
        Ok(())
    };
    parse_macro_input!(attr with parser);
    let error_type = error_ty.expect("#[client_task_enum] requires an `error = <Type>` attribute");

    let mut ast = parse_macro_input!(input as DeriveInput);

    let enum_name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();

    let variants = if let Data::Enum(data) = &mut ast.data {
        &mut data.variants
    } else {
        panic!("#[client_task_enum] can only be applied to enums");
    };

    let mut from_impls = Vec::new();
    let mut encode_req_arms = Vec::new();
    let mut get_req_blob_arms = Vec::new();
    let mut decode_resp_arms = Vec::new();
    let mut reserve_resp_blob_arms = Vec::new();
    let mut get_action_arms = Vec::new();
    let mut get_result_arms = Vec::new();
    let mut set_custom_error_arms = Vec::new();
    let mut set_rpc_error_arms = Vec::new();
    let mut set_ok_arms = Vec::new();
    let mut done_arms = Vec::new();
    let mut deref_arms = Vec::new();
    let mut deref_mut_arms = Vec::new();

    let mut inner_type_counts: HashMap<String, usize> = HashMap::new();

    for variant in variants.iter_mut() {
        let variant_name = &variant.ident;
        let inner_type_cloned = match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                fields.unnamed.first().unwrap().ty.clone()
            }
            _ => panic!("Enum variants must be tuple-style with a single field"),
        };

        let inner_type_str = quote! {#inner_type_cloned}.to_string();
        *inner_type_counts.entry(inner_type_str.clone()).or_insert(0) += 1;

        let count = *inner_type_counts.get(&inner_type_str).unwrap_or(&0);
        if count == 1 {
            from_impls.push(quote! {
                impl #impl_generics From<#inner_type_cloned> for #enum_name #ty_generics #where_clause {
                    #[inline]
                    fn from(task: #inner_type_cloned) -> Self {
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

        let action_meta = get_action_attribute(variant);
        variant.attrs.retain(|attr| !attr.path.is_ident("action"));

        let action_arm = if let Some(meta) = action_meta {
            match meta {
                Meta::NameValue(nv) => match nv.lit {
                    Lit::Int(val) => {
                        quote! { #enum_name::#variant_name(..) => razor_stream::proto::RpcAction::Num(#val), }
                    }
                    Lit::Str(val) => {
                        quote! { #enum_name::#variant_name(..) => razor_stream::proto::RpcAction::Str(#val), }
                    }
                    _ => panic!("Unsupported action type"),
                },
                Meta::Path(val) => {
                    quote! { #enum_name::#variant_name(..) => razor_stream::proto::RpcAction::Num(#val as i32), }
                }
                _ => panic!("Unsupported action type"),
            }
        } else {
            quote! { #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskAction::get_action(inner), }
        };

        get_action_arms.push(action_arm);

        encode_req_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskEncode::encode_req(inner, codec, buf),
        });

        get_req_blob_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskEncode::get_req_blob(inner),
        });

        decode_resp_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDecode::decode_resp(inner, codec, buffer),
        });

        reserve_resp_blob_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDecode::reserve_resp_blob(inner, size),
        });

        get_result_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskGetResult::get_result(inner),
        });
        set_custom_error_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDone::set_custom_error(inner, codec, res, last_index, conf_ver),
        });
        set_rpc_error_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDone::set_rpc_error(inner, e),
        });
        set_ok_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDone::set_ok(inner),
        });
        done_arms.push(quote! {
            #enum_name::#variant_name(inner) => razor_stream::client::task::ClientTaskDone::done(inner),
        });

        deref_arms.push(quote! {
            #enum_name::#variant_name(inner) => inner,
        });

        deref_mut_arms.push(quote! {
            #enum_name::#variant_name(inner) => inner,
        });
    }

    let expanded = quote! {
        #ast

        #(#from_impls)*

        impl #impl_generics std::ops::Deref for #enum_name #ty_generics #where_clause {
            type Target = razor_stream::client::task::ClientTaskCommon;
            #[inline]
            fn deref(&self) -> &Self::Target {
                match self {
                    #(#deref_arms)*
                }
            }
        }

        impl #impl_generics std::ops::DerefMut for #enum_name #ty_generics #where_clause {
            #[inline]
            fn deref_mut(&mut self) -> &mut Self::Target {
                match self {
                    #(#deref_mut_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTaskEncode for #enum_name #ty_generics #where_clause {
            #[inline]
            fn encode_req<C: razor_stream::Codec>(&self, codec: &C, buf: &mut Vec<u8>) -> Result<usize, ()> {
                match self {
                    #(#encode_req_arms)*
                }
            }

            #[inline]
            fn get_req_blob(&self) -> Option<&[u8]> {
                match self {
                    #(#get_req_blob_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTaskDecode for #enum_name #ty_generics #where_clause {
            #[inline]
            fn decode_resp<C: razor_stream::Codec>(&mut self, codec: &C, buffer: &[u8]) -> Result<(), ()> {
                match self {
                    #(#decode_resp_arms)*
                }
            }

            #[inline]
            fn reserve_resp_blob(&mut self, size: i32) -> Option<&mut [u8]> {
                match self {
                    #(#reserve_resp_blob_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTaskAction for #enum_name #ty_generics #where_clause {
            #[inline]
            fn get_action<'a>(&'a self) -> razor_stream::proto::RpcAction<'a> {
                match self {
                    #(#get_action_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTaskGetResult<#error_type> for #enum_name #ty_generics #where_clause {
            #[inline]
            fn get_result(&self) -> Result<(), &razor_stream::error::RpcError<#error_type>> {
                match self {
                    #(#get_result_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTaskDone for #enum_name #ty_generics #where_clause {
            #[inline]
            fn set_custom_error<C: razor_stream::Codec>(&mut self, codec: &C, res: razor_stream::error::EncodedErr, last_index: usize, conf_ver: u64) {
                match self {
                    #(#set_custom_error_arms)*
                }
            }

            #[inline]
            fn set_rpc_error(&mut self, e: razor_stream::error::RpcIntErr) {
                match self {
                    #(#set_rpc_error_arms)*
                }
            }

            #[inline]
            fn set_ok(&mut self) {
                match self {
                    #(#set_ok_arms)*
                }
            }

            #[inline]
            fn done(self) {
                match self {
                    #(#done_arms)*
                }
            }
        }

        impl #impl_generics razor_stream::client::task::ClientTask for #enum_name #ty_generics #where_clause {}
    };

    TokenStream::from(expanded)
}

/// ```compile_fail
/// use razor_stream_macros::{client_task, client_task_enum};
/// use razor_stream::client::task::ClientTaskCommon;
/// use razor_stream::error::{RpcErrCodec, RpcError};
/// use crossfire::{MTx, mpsc};
/// use serde_derive::{Deserialize, Serialize};
/// use nix::errno::Errno;
/// use std::marker::PhantomData;
///
/// #[derive(Default, Deserialize, Serialize)]
/// pub struct MyTaskReq;
///
/// #[derive(Default, Deserialize, Serialize)]
/// pub struct MyTaskResp;
///
/// #[client_task]
/// pub struct TaskA {
///     #[field(common)]
///     common: ClientTaskCommon,
///     #[field(req)]
///     req: MyTaskReq,
///     #[field(resp)]
///     resp: Option<MyTaskResp>,
///     #[field(res)]
///     res: Option<Result<(), RpcError<Errno>>>,
///     #[field(noti)]
///     noti: Option<MTx<mpsc::List<MyEnumTask>>>,
/// }
///
/// #[client_task_enum(error=nix::error::Errno)]
/// pub enum MyEnumTask {
///     VariantA(TaskA<Errno>),
///     VariantB(TaskA<Errno>), // Duplicate sub-type TaskA
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_client_task_enum_duplicate_subtype() {}
