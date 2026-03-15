use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, parse_macro_input};

/// Generate client struct
pub fn endpoint_client(attr: TokenStream) -> TokenStream {
    let client_name = parse_macro_input!(attr as Ident);

    let client_struct = quote! {
        pub struct #client_name<C>
        where
            C: razor_rpc::client::APIClientCaller,
        {
            caller: C,
        }
    };

    let new_method = quote! {
        impl<C> #client_name<C>
        where
            C: razor_rpc::client::APIClientCaller,
        {
            pub fn new(caller: C) -> Self {
                Self { caller }
            }
        }
    };

    let as_ref_impl = quote! {
        impl<C> std::convert::AsRef<C> for #client_name<C>
        where
            C: razor_rpc::client::APIClientCaller,
        {
            fn as_ref(&self) -> &C {
                &self.caller
            }
        }
    };

    let clone_impl = quote! {
        impl<C> Clone for #client_name<C>
        where
            C: razor_rpc::client::APIClientCaller + Clone,
        {
            fn clone(&self) -> Self {
                Self { caller: self.caller.clone() }
            }
        }
    };

    let expanded = quote! {
        #client_struct

        #new_method

        #as_ref_impl

        #clone_impl
    };

    TokenStream::from(expanded)
}
