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

use crate::utils::name_expr;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::Generics;

pub struct Implementation {
    ident: Ident,
    generics: Generics,
    /// An expression that resolves to a `Schema`
    ///
    /// The variables `named_schemas: &mut HashSet<Name>` and `enclosing_namespace: NamespaceRef` are
    /// defined.
    schema_expr: TokenStream,
    /// An expression that resolves to a `Option<Vec<RecordField>>`
    ///
    /// The variables `named_schemas: &mut HashSet<Name>` and `enclosing_namespace: NamespaceRef` are
    /// defined.
    record_fields_expr: TokenStream,
    /// An expression that resolves to a `Option<serde_json::Value>`
    field_default_expr: TokenStream,
}

impl Implementation {
    #[expect(
        clippy::needless_pass_by_value,
        reason = "Makes it match the `unnamed` variant"
    )]
    pub fn named(
        ident: Ident,
        generics: Generics,
        name: &str,
        schema_expr: TokenStream,
        record_fields_expr: Option<TokenStream>,
        field_default_expr: Option<TokenStream>,
    ) -> Implementation {
        let name_expr = name_expr(name);
        let schema_expr = quote! {
            let name = #name_expr;
            if named_schemas.contains(&name) {
                ::apache_avro::schema::Schema::Ref{name}
            } else {
                let enclosing_namespace = name.namespace();
                named_schemas.insert(name.clone());
                #schema_expr
            }
        };

        Self::unnamed(
            ident,
            generics,
            schema_expr,
            record_fields_expr,
            field_default_expr,
        )
    }

    pub fn unnamed(
        ident: Ident,
        generics: Generics,
        schema_expr: TokenStream,
        record_fields_expr: Option<TokenStream>,
        field_default_expr: Option<TokenStream>,
    ) -> Implementation {
        Self {
            ident,
            generics,
            schema_expr,
            record_fields_expr: record_fields_expr
                .unwrap_or_else(|| quote! { ::std::option::Option::None }),
            field_default_expr: field_default_expr
                .unwrap_or_else(|| quote! { ::std::option::Option::None }),
        }
    }

    pub fn into_token_stream(self) -> TokenStream {
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let ident = self.ident;
        let schema_expr = self.schema_expr;
        let record_fields_expr = self.record_fields_expr;
        let field_default_expr = self.field_default_expr;

        quote! {
            #[automatically_derived]
            impl #impl_generics ::apache_avro::AvroSchemaComponent for #ident #ty_generics #where_clause {
                fn get_schema_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::apache_avro::schema::Schema {
                    #schema_expr
                }

                fn get_record_fields_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
                    #record_fields_expr
                }

                fn field_default() -> ::std::option::Option<::serde_json::Value> {
                    #field_default_expr
                }
            }
        }
    }
}
