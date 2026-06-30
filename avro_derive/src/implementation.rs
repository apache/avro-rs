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
use quote::{format_ident, quote};
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
    /// Tests that will be run if the type has no generics and they haven't been disabled via an attribute.
    tests: Option<Vec<Test>>,
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
        tests: bool,
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
            tests,
        )
    }

    pub fn unnamed(
        ident: Ident,
        generics: Generics,
        schema_expr: TokenStream,
        record_fields_expr: Option<TokenStream>,
        field_default_expr: Option<TokenStream>,
        tests: bool,
    ) -> Implementation {
        let mut this = Self {
            ident,
            generics,
            schema_expr,
            record_fields_expr: record_fields_expr
                .unwrap_or_else(|| quote! { ::std::option::Option::None }),
            field_default_expr: field_default_expr
                .unwrap_or_else(|| quote! { ::std::option::Option::None }),
            tests: tests.then(Vec::new),
        };

        this.add_schema_test();

        this
    }

    pub fn add_test(&mut self, test: Test) {
        if let Some(tests) = &mut self.tests {
            tests.push(test);
        }
    }

    fn add_schema_test(&mut self) {
        // Only add the schema test if there are no generics to deal with. I think it should be possible
        // to be able to generate the test if there are only lifetimes, but that needs more experimentation.
        if self.generics.lifetimes().count() == 0
            && self.generics.type_params().count() == 0
            && self.generics.const_params().count() == 0
        {
            let ident = &self.ident;
            self.add_test(Test {
                name: "schema".to_string(),
                body: quote! {
                    eprintln!("Invalid schema, if this is because a different validator is used at runtime you can disable generating tests with `#[avro(tests = false)]`");
                    <#ident as ::apache_avro::serde::AvroSchema>::get_schema();
                },
                should_panic: None,
            });
        }
    }

    pub fn into_token_stream(self) -> TokenStream {
        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();
        let ident = self.ident;
        let schema_expr = self.schema_expr;
        let record_fields_expr = self.record_fields_expr;
        let field_default_expr = self.field_default_expr;

        let tests = if let Some(tests) = self.tests {
            let test_module_name = format!(
                "_apache_avro_derive_tests_for_{}",
                ident.to_string().to_lowercase()
            );
            let tests = tests
                .into_iter()
                .map(|t| t.into_token_stream(&test_module_name));
            quote! {
                #(
                    #[cfg(test)]
                    #tests
                )*
            }
        } else {
            TokenStream::new()
        };

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

            #tests
        }
    }
}

pub struct Test {
    /// The name of the test.
    pub name: String,
    /// The body of the test function, so without `fn some_name() {`.
    pub body: TokenStream,
    /// If this test should panic, this should be the message.
    pub should_panic: Option<String>,
}

impl Test {
    pub fn into_token_stream(self, prefix: &str) -> TokenStream {
        let name = format_ident!("{prefix}_{}", &self.name);
        let body = &self.body;
        let should_panic = if let Some(expected) = &self.should_panic {
            quote! { #[should_panic(expected = #expected)] }
        } else {
            TokenStream::new()
        };
        quote! {
            #[test]
            #should_panic
            fn #name() {
                #body
            }
        }
    }
}
