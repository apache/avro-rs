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

#![cfg_attr(nightly, feature(proc_macro_diagnostic))]

//! This crate is the implementation of the `AvroSchema` derive macro.
//! Please use it via the [`apache-avro`](https://crates.io/crates/apache-avro) crate:
//!
//! ```no_run
//! use apache_avro::AvroSchema;
//!
//! #[derive(AvroSchema)]
//! ```
//! Please see the documentation of the [`AvroSchema`] trait for instructions on how to use it.
//!
//! [`AvroSchema`]: https://docs.rs/apache-avro/latest/apache_avro/serde/trait.AvroSchema.html

mod attributes;
mod case;
mod enums;
mod fields;
mod implementation;
mod structs;
mod utils;

use syn::{DeriveInput, parse_macro_input, spanned::Spanned};

use crate::attributes::NamedTypeOptions;
use crate::implementation::Implementation;
use crate::utils::to_compile_errors;

#[proc_macro_derive(AvroSchema, attributes(avro, serde))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(input)
        .map(Implementation::into_token_stream)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: DeriveInput) -> Result<Implementation, Vec<syn::Error>> {
    // It would be nice to parse the attributes before the `match`, but we first need to validate that `input` is not a union.
    // Otherwise a user could get errors related to the attributes and after fixing those get an error because the attributes were on a union.
    let input_span = input.span();
    match input.data {
        syn::Data::Struct(data_struct) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            structs::to_implementation(
                input_span,
                input.ident,
                input.generics,
                named_type_options,
                data_struct,
            )
        }
        syn::Data::Enum(data_enum) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            enums::to_implementation(
                input_span,
                input.ident,
                input.generics,
                named_type_options,
                data_enum,
            )
        }
        syn::Data::Union(_) => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive only works for structs and simple enums",
        )]),
    }
}

// #[cfg(test)]
// mod tests {
//     use quote::quote;
//     use syn::DeriveInput;
//     use crate::derive_avro_schema;
//
//     #[test]
//     fn debug() {
//         let struc = quote! {
//             #[avro(repr = "record_internally_tagged")]
//             #[serde(tag = "type")]
//             enum Foo {
//                 A {},
//                 B {
//                     #[avro(default = r#""spam""#)]
//                     spam: String,
//                 },
//                 #[serde(rename = "D")]
//                 C {
//                     #[avro(default = r#""bar""#)]
//                     bar: String,
//                     #[serde(rename = "is_it_true", alias = "is_it_false")]
//                     #[avro(default = "true")]
//                     other: bool,
//                 },
//             }
//         };
//         let input = syn::parse2::<DeriveInput>(struc).unwrap();
//         let stream = derive_avro_schema(input).unwrap().into_token_stream();
//         println!("{}", stream.to_string());
//         panic!();
//     }
// }
