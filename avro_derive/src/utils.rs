// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use proc_macro2::TokenStream;
use quote::quote;

/// Convert a list of errors into actual compiler errors.
///
/// Copied from `serde`, originally written by David Tolnay.
pub fn to_compile_errors(errors: impl AsRef<[syn::Error]>) -> TokenStream {
    let compile_errors = errors.as_ref().iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

pub fn preserve_optional(op: Option<impl quote::ToTokens>) -> TokenStream {
    if let Some(tt) = op {
        quote! {::std::option::Option::Some(#tt.into())}
    } else {
        quote! {::std::option::Option::None}
    }
}

/// Convert a list of strings to an expression that resolves to a `Option<Vec<Alias>>`.
pub fn aliases(op: &[impl AsRef<str>]) -> TokenStream {
    let items: Vec<TokenStream> = op
        .iter()
        .map(|alias| {
            let alias = alias.as_ref();
            quote! {
                ::apache_avro::schema::Alias::new(#alias).expect("Alias is invalid")
            }
        })
        .collect();
    if items.is_empty() {
        quote! {::std::option::Option::None}
    } else {
        quote! {::std::option::Option::Some(vec![#(#items),*])}
    }
}

/// Convert a list of strings to an expression that resolves to a `Vec<String>`.
pub fn field_aliases(op: &[impl AsRef<str>]) -> TokenStream {
    let items: Vec<TokenStream> = op
        .iter()
        .map(|string| {
            let string = string.as_ref();
            quote! {
                ::std::string::String::from(#string)
            }
        })
        .collect();
    quote! {vec![#(#items),*]}
}
