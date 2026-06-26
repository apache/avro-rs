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
use serde_json::Value;

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

/// Convert a `Value` into a `TokenStream`.
///
/// The `Value` is constructed, not parsed from a JSON string.
pub fn json_value_expr(value: Value) -> TokenStream {
    match value {
        Value::Null => quote! {
            ::serde_json::Value::Null
        },
        Value::Bool(bool) => quote! {
            ::serde_json::Value::Bool(#bool)
        },
        Value::Number(number) => {
            if let Some(n) = number.as_u64() {
                quote! {
                    ::serde_json::Value::Number(::serde_json::Number::from(#n))
                }
            } else if let Some(n) = number.as_i64() {
                quote! {
                    ::serde_json::Value::Number(::serde_json::Number::from(#n))
                }
            } else if let Some(n) = number.as_f64() {
                quote! {
                    ::serde_json::Value::Number(::serde_json::Number::from_f64(#n).expect("Unreachable, f64 is finite"))
                }
            } else {
                // This is needed when the arbitrary-precision feature flag is enabled on serde_json
                let s = number.to_string();
                quote! {
                    ::serde_json::Value::Number(#s.parse().expect(concat!("This was a valid number at compile time: ", #s)))
                }
            }
        }
        Value::String(string) => quote! {
            ::serde_json::Value::String(::std::string::String::from(#string))
        },
        Value::Array(array) => {
            let array = array.into_iter().map(json_value_expr);
            quote! {
                ::serde_json::Value::Array(vec![#(#array),*])
            }
        }
        Value::Object(object) => {
            let len = object.len();

            let mut keys = Vec::with_capacity(len);
            let mut values = Vec::with_capacity(len);
            for (key, value) in object {
                keys.push(key);
                values.push(json_value_expr(value));
            }

            quote! {
                ::serde_json::Value::Object({
                    let mut map = ::serde_json::Map::with_capacity(#len);
                    #(map.insert(::std::string::String::from(#keys), #values);)*
                    map
                })
            }
        }
    }
}
