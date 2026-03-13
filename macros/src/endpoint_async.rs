use proc_macro::TokenStream;
use quote::quote;
use syn::{
    FnArg, Ident, ItemTrait, Pat, PatType, Result, ReturnType, TraitItem,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// Parse macro arguments to get the struct name to generate
struct EndpointAsyncArgs {
    client_name: Ident,
}

impl Parse for EndpointAsyncArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let client_name = input.parse()?;
        Ok(EndpointAsyncArgs { client_name })
    }
}

pub fn endpoint_async(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as EndpointAsyncArgs);
    let input_trait = parse_macro_input!(input as ItemTrait);

    let client_name = args.client_name;

    // Check if the trait already has async_trait attribute
    let has_async_trait = input_trait
        .attrs
        .iter()
        .any(|attr| attr.path.segments.iter().any(|segment| segment.ident == "async_trait"));

    // Generate client struct
    let client_struct = generate_client_struct(&client_name);

    // Generate client implementation
    let client_impl = generate_client_impl(&client_name);

    // Generate trait implementation
    let trait_impl = generate_trait_impl(&client_name, &input_trait, has_async_trait);

    let expanded = quote! {
        #input_trait

        #client_struct

        #client_impl

        #trait_impl
    };
    TokenStream::from(expanded)
}

fn get_result_type_from_future(ty: &syn::Type) -> Option<&syn::Type> {
    if let syn::Type::ImplTrait(type_impl) = ty {
        for bound in &type_impl.bounds {
            if let syn::TypeParamBound::Trait(trait_bound) = bound
                && let Some(segment) = trait_bound.path.segments.last()
                && segment.ident == "Future"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
            {
                for arg in &args.args {
                    if let syn::GenericArgument::Binding(binding) = arg
                        && binding.ident == "Output"
                    {
                        return Some(&binding.ty);
                    }
                }
            }
        }
    }
    None
}

fn check_return_type(return_type: &syn::ReturnType, returns_impl_future: bool) -> bool {
    if returns_impl_future {
        // For impl Future, we need to check the Output type
        if let syn::ReturnType::Type(_, ty) = return_type
            && let Some(output_type) = get_result_type_from_future(ty)
        {
            return check_result_type(output_type);
        }
        false
    } else {
        // For direct return types, check directly
        if let syn::ReturnType::Type(_, ty) = return_type {
            return check_result_type(ty);
        }
        false
    }
}

fn check_result_type(ty: &syn::Type) -> bool {
    // Check if type is Result<_, RpcError<_>>
    if let syn::Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
        && segment.ident == "Result"
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
    {
        // Check if second generic argument is RpcError
        if args.args.len() == 2
            && let syn::GenericArgument::Type(syn::Type::Path(error_type)) = &args.args[1]
            && let Some(error_segment) = error_type.path.segments.last()
        {
            return error_segment.ident == "RpcError";
        }
    }
    false
}

fn generate_client_struct(client_name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        pub struct #client_name<C>
        where
            C: razor_rpc::client::ClientCaller,
            C: Clone,
            C: Sync,
            C: 'static,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            caller: C,
            codec: <C::Facts as razor_rpc::client::ClientFacts>::Codec,
        }
    }
}

fn generate_client_impl(client_name: &Ident) -> proc_macro2::TokenStream {
    let new_method = quote! {
        pub fn new(caller: C) -> Self {
            Self {
                caller,
                codec: Default::default(),
            }
        }
    };

    let as_ref_impl = quote! {
        impl<C> std::convert::AsRef<C> for #client_name<C>
        where
            C: razor_rpc::client::ClientCaller,
            C: Clone + Sync + 'static,
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
            C: razor_rpc::client::ClientCaller + Clone,
            C: Clone + Sync + 'static,
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
            C: razor_rpc::client::ClientCaller,
            C: Clone + Sync + 'static,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            fn codec(&self) -> &<C::Facts as razor_rpc::client::ClientFacts>::Codec {
                &self.codec
            }
        }
    };

    quote! {
        impl<C> #client_name<C>
        where
            C: razor_rpc::client::ClientCaller,
            C: Clone + Sync + 'static,
            C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
        {
            #new_method
        }

        #as_ref_impl

        #clone_impl

        #async_endpoint_impl
    }
}

fn generate_trait_impl(
    client_name: &Ident, input_trait: &ItemTrait, has_async_trait: bool,
) -> proc_macro2::TokenStream {
    let trait_name = &input_trait.ident;
    let trait_items = &input_trait.items;

    let mut impl_methods = Vec::new();

    // Generate implementation for each trait method
    for item in trait_items {
        if let TraitItem::Method(method) = item {
            let method_sig = &method.sig;
            let method_name = &method_sig.ident;

            // Parse method arguments
            let (arg_name, arg_type) = if method_sig.inputs.len() == 1 {
                // Only &self argument - this is not allowed
                panic!(
                    "Method `{}` has no parameters. Endpoint methods must have exactly one parameter besides &self.",
                    method_name
                );
            } else if method_sig.inputs.len() == 2 {
                // &self + one argument
                if let FnArg::Typed(PatType { pat, ty, .. }) = &method_sig.inputs[1] {
                    if let Pat::Ident(pat_ident) = pat.as_ref() {
                        (Some(pat_ident.ident.clone()), Some(ty.as_ref()))
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                }
            } else {
                panic!(
                    "Method `{}` methods must have exactly one parameter besides &self.",
                    method_name
                );
            };

            // Construct service method name "TraitName.method_name"
            let service_method = format!("{}.{}", trait_name, method_name);

            // Handle return type
            let return_type = &method_sig.output;

            // Check if this is an impl Future return type
            let returns_impl_future = if let ReturnType::Type(_, ty) = return_type {
                get_result_type_from_future(ty).is_some()
            } else {
                false
            };

            // Check if method is async or returns impl Future (which means it should be handled as async)
            let is_async_method = method_sig.asyncness.is_some() || returns_impl_future;

            // Check if return type is Result<_, RpcError<_>>
            if !check_return_type(return_type, returns_impl_future) {
                panic!(
                    "Method `{}` has invalid return type. Endpoint methods must return `Result<_, RpcError<_>>`.",
                    method_name
                );
            }

            // Generate method implementation
            let method_impl = if let Some(arg_type) = arg_type {
                // Method with arguments
                let arg_name = arg_name.unwrap();
                if is_async_method {
                    if returns_impl_future {
                        quote! {
                            fn #method_name(&self, #arg_name: #arg_type) #return_type {
                                async move {
                                    <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &#arg_name).await
                                }
                            }
                        }
                    } else {
                        quote! {
                            async fn #method_name(&self, #arg_name: #arg_type) #return_type {
                                <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &#arg_name).await
                            }
                        }
                    }
                } else {
                    // For non-async methods, we still need to return a future
                    quote! {
                        async fn #method_name(&self, #arg_name: #arg_type) #return_type {
                            <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &#arg_name).await
                        }
                    }
                }
            } else {
                // Method without arguments
                if is_async_method {
                    if returns_impl_future {
                        quote! {
                            fn #method_name(&self) #return_type {
                                async move {
                                    <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &()).await
                                }
                            }
                        }
                    } else {
                        quote! {
                            async fn #method_name(&self) #return_type {
                                <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &()).await
                            }
                        }
                    }
                } else {
                    // For non-async methods, we still need to return a future
                    quote! {
                        async fn #method_name(&self) #return_type {
                            <Self as razor_rpc::client::AsyncEndpoint<C>>::call(self, #service_method, &()).await
                        }
                    }
                }
            };

            impl_methods.push(method_impl);
        }
    }

    // Add async_trait attribute only if the original trait doesn't have it
    if has_async_trait {
        quote! {
            #[::async_trait::async_trait]
            impl<C> #trait_name for #client_name<C>
            where
                C: razor_rpc::client::ClientCaller,
                C: Clone,
                C: Sync,
                C: 'static,
                C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
            {
                #(#impl_methods)*
            }
        }
    } else {
        quote! {
            impl<C> #trait_name for #client_name<C>
            where
                C: razor_rpc::client::ClientCaller,
                C: Clone,
                C: Sync,
                C: 'static,
                C::Facts: razor_rpc::client::ClientFacts<Task = razor_rpc::client::task::APIClientReq>,
            {
                #(#impl_methods)*
            }
        }
    }
}

/// ```compile_fail
/// use razor_rpc_macros::endpoint_async;
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq)]
/// pub struct MyArg {
///     pub value: u32,
/// }
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq)]
/// pub struct MyResp {
///     pub result: u32,
/// }
///
/// // This should fail to compile because the method has too many arguments
/// // The endpoint_async macro only supports methods with &self and at most one additional argument
/// #[endpoint_async(DemoClient)]
/// pub trait DemoService {
///     async fn foo(&self, arg1: MyArg, arg2: MyArg) -> Result<MyResp, RpcError<()>>;
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_too_many_arguments_compile_fail() {}

/// ```compile_fail
/// use razor_rpc_macros::endpoint_async;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq)]
/// pub struct MyArg {
///     pub value: u32,
/// }
///
/// // This should fail to compile because the method has invalid return type
/// // The endpoint_async macro only supports methods that return Result<_, RpcError<_>>
/// #[endpoint_async(DemoClient)]
/// pub trait InvalidReturnService {
///     async fn compute(&self, args: MyArg) -> i32;
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_invalid_return_type_compile_fail() {}

/// ```compile_fail
/// use razor_rpc_macros::endpoint_async;
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq)]
/// pub struct MyArg {
///     pub value: u32,
/// }
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq, Default)]
/// pub struct MyResp {
///     pub result: u32,
/// }
///
/// // This should fail to compile because the method is not async
/// // The endpoint_async macro requires methods to be async or return impl Future
/// #[endpoint_async(DemoClient)]
/// pub trait BlockingService {
///     fn foo(&self, arg: MyArg) -> Result<MyResp, RpcError<()>>;
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_sync_method_compile_fail() {}

/// ```compile_fail
/// use razor_rpc_macros::endpoint_async;
/// use razor_rpc::error::RpcError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Deserialize, Serialize, PartialEq)]
/// pub struct MyResp {
///     pub result: u32,
/// }
///
/// // This should fail to compile because the method has no parameters (except &self)
/// // The endpoint_async macro requires methods to have &self and exactly one additional parameter
/// #[endpoint_async(DemoClient)]
/// pub trait ZeroParamService {
///     async fn get_data(&self) -> Result<MyResp, RpcError<()>>;
/// }
/// ```
#[doc(hidden)]
#[allow(dead_code)]
fn test_zero_param_compile_fail() {}
