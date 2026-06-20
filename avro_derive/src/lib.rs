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
mod structs;
mod utils;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Generics, Ident, parse_macro_input, spanned::Spanned};

use crate::attributes::NamedTypeOptions;
use crate::enums::get_data_enum_schema_def;
use crate::structs::{get_struct_schema_def, get_transparent_struct_schema_def};
use crate::utils::{aliases, preserve_optional, to_compile_errors};

#[proc_macro_derive(AvroSchema, attributes(avro, serde))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    // It would be nice to parse the attributes before the `match`, but we first need to validate that `input` is not a union.
    // Otherwise a user could get errors related to the attributes and after fixing those get an error because the attributes were on a union.
    let input_span = input.span();
    match input.data {
        syn::Data::Struct(data_struct) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            let (get_schema_impl, get_record_fields_impl) = if named_type_options.transparent {
                get_transparent_struct_schema_def(data_struct.fields, input_span)?
            } else {
                let (schema_def, record_fields) =
                    get_struct_schema_def(&named_type_options, data_struct, input.ident.span())?;
                (
                    handle_named_schemas(&named_type_options.name, &schema_def),
                    record_fields,
                )
            };
            Ok(create_trait_definition(
                &input.ident,
                &input.generics,
                &get_schema_impl,
                &get_record_fields_impl,
                &named_type_options.default,
            ))
        }
        syn::Data::Enum(data_enum) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            if named_type_options.transparent {
                return Err(vec![syn::Error::new(
                    input_span,
                    "AvroSchema: `#[serde(transparent)]` is only supported on structs",
                )]);
            }
            let schema_def =
                get_data_enum_schema_def(&named_type_options, &data_enum, input.ident.span())?;
            let inner = handle_named_schemas(&named_type_options.name, &schema_def);
            Ok(create_trait_definition(
                &input.ident,
                &input.generics,
                &inner,
                &quote! { ::std::option::Option::None },
                &named_type_options.default,
            ))
        }
        syn::Data::Union(_) => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive only works for structs and simple enums",
        )]),
    }
}

/// Generate the trait definition with the correct generics
fn create_trait_definition(
    ident: &Ident,
    generics: &Generics,
    get_schema_impl: &TokenStream,
    get_record_fields_impl: &TokenStream,
    field_default_impl: &TokenStream,
) -> TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::apache_avro::AvroSchemaComponent for #ident #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::apache_avro::schema::Schema {
                #get_schema_impl
            }

            fn get_record_fields_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
                #get_record_fields_impl
            }

            fn field_default() -> ::std::option::Option<::serde_json::Value> {
                ::std::option::Option::#field_default_impl
            }
        }
    }
}

/// Generate the code to check `named_schemas` if this schema already exist
fn handle_named_schemas(full_schema_name: &str, schema_def: &TokenStream) -> TokenStream {
    quote! {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(#full_schema_name, enclosing_namespace).expect(concat!("Unable to parse schema name ", #full_schema_name));
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref{name}
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            #schema_def
        }
    }
}
