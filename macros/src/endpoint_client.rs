use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, parse_macro_input};

/// Generate client struct with AsyncEndpoint implementation
pub fn endpoint_client(attr: TokenStream) -> TokenStream {
    let client_name = parse_macro_input!(attr as Ident);

    let client_struct = quote! {
        pub struct #client_name<C>
        where
            C: razor_rpc::client::ClientCaller,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            caller: C,
            codec: <C::Facts as razor_rpc::client::ClientFacts>::Codec,
        }
    };

    let new_method = quote! {
        impl<C> #client_name<C>
        where
            C: razor_rpc::client::ClientCaller,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            pub fn new(caller: C) -> Self {
                Self {
                    caller,
                    codec: Default::default(),
                }
            }
        }
    };

    let as_ref_impl = quote! {
        impl<C> std::convert::AsRef<C> for #client_name<C>
        where
            C: razor_rpc::client::ClientCaller + Sync,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            fn as_ref(&self) -> &C {
                &self.caller
            }
        }
    };

    let clone_impl = quote! {
        impl<C> Clone for #client_name<C>
        where
            C: razor_rpc::client::ClientCaller + Clone + Sync,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
            <C::Facts as razor_rpc::client::ClientFacts>::Codec: Clone,
        {
            fn clone(&self) -> Self {
                Self {
                    caller: self.caller.clone(),
                    codec: self.codec.clone(),
                }
            }
        }
    };

    let async_endpoint_impl = quote! {
        impl<C> razor_rpc::client::AsyncEndpoint<C> for #client_name<C>
        where
            C: razor_rpc::client::ClientCaller + Sync,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            fn codec(&self) -> &<C::Facts as razor_rpc::client::ClientFacts>::Codec {
                &self.codec
            }
        }
    };

    let expanded = quote! {
        #client_struct

        #new_method

        #as_ref_impl

        #clone_impl

        #async_endpoint_impl
    };

    TokenStream::from(expanded)
}
