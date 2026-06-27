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

use crate::attributes::{FieldOptions, NamedTypeOptions, Repr, VariantOptions, With};
use crate::enums::newtype_extra_attribute_checks;
use crate::fields::{field_to_record_fields_expr, named_fields_to_record_fields};
use crate::implementation::Implementation;
use crate::utils::{aliases, json_value_expr, preserve_optional};
use proc_macro2::Ident;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Expr, Fields, Generics};

pub fn to_implementation(
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let Some(Repr::RecordInternallyTagged { tag }) = container_attrs.repr else {
        unreachable!()
    };
    let mut errors = Vec::new();
    let mut field_exprs = Vec::new();

    for variant in data.variants {
        let variant_span = variant.span();
        let variant_attrs = match VariantOptions::new(&variant.attrs, variant_span) {
            Ok(attrs) => attrs,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        if variant_attrs.skip {
            continue;
        }

        match variant_attrs.with {
            With::Serde(path) => field_exprs.push(quote! {
                fields.extend(
                    #path::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)
                        .expect("Expected record fields for internally tagged enum");
                )
            }),
            With::Expr(Expr::Closure(closure)) => {
                if closure.inputs.is_empty() {
                    field_exprs.push(quote! {
                        fields.extend(
                            ::apache_avro::serde::get_record_fields_in_ctxt(
                                named_schemas,
                                enclosing_namespace,
                                |_, _| (#closure)(),
                            ).expect("Expected record fields for internally tagged enum");
                        )
                    });
                } else {
                    errors.push(syn::Error::new(
                        variant.span(),
                        "Expected closure with 0 parameters",
                    ));
                }
            }
            With::Expr(Expr::Path(path)) => {
                field_exprs.push(quote! {
                    fields.extend(
                        ::apache_avro::serde::get_record_fields_in_ctxt(named_schemas, enclosing_namespace, #path)
                            .expect("Expected record fields for internally tagged enum");
                    )
                });
            }
            With::Expr(_expr) => errors.push(syn::Error::new(
                variant.span(),
                "Invalid expression, expected a function or a closure",
            )),
            With::Trait => {
                match variant.fields {
                    Fields::Named(fields) => {
                        let record_fields_expr = match named_fields_to_record_fields(
                            fields,
                            variant_attrs
                                .rename_all
                                .or(container_attrs.rename_all_fields),
                        ) {
                            Ok(expr) => expr,
                            Err(errs) => {
                                errors.extend(errs);
                                continue;
                            }
                        };
                        field_exprs.push(quote! {
                            fields.extend(#record_fields_expr)
                        });
                    }
                    Fields::Unnamed(mut fields) => {
                        // Serde only allows #[serde(tag = "...")] on tuple variants with exactly one field (newtype variant)
                        if fields.unnamed.len() == 1 {
                            let pair = fields
                                .unnamed
                                .pop()
                                .unwrap_or_else(|| unreachable!("Length is 1"));
                            let field = pair.into_value();
                            let field_attrs = match FieldOptions::new(&field.attrs, field.span())
                                .and_then(|o| newtype_extra_attribute_checks(o, field.span()))
                            {
                                Ok(attrs) => attrs,
                                Err(errs) => {
                                    errors.extend(errs);
                                    continue;
                                }
                            };
                            let record_fields_expr =
                                match field_to_record_fields_expr(&field, &field_attrs.with) {
                                    Ok(expr) => expr,
                                    Err(errs) => {
                                        errors.extend(errs);
                                        continue;
                                    }
                                };
                            field_exprs.push(quote! {
                        if let Some(record_fields) = #record_fields_expr {
                            fields.extend(record_fields)
                        } else {
                            panic!("Newtype variant type must implement `get_record_fields` for internally tagged enums")
                        }
                    });
                        } else {
                            errors.push(syn::Error::new(fields.span(), "AvroSchema: Internally tagged enums only support struct, unit, and newtype variants"));
                        }
                    }
                    Fields::Unit => {
                        // No fields to add to the struct
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let doc = preserve_optional(
        container_attrs
            .doc
            .as_ref()
            .map(|s| quote! { #s.to_string() }),
    );
    let enum_aliases = aliases(&container_attrs.aliases);

    let schema_expr = quote! {{
        let mut fields = ::std::vec::Vec::new();
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(::apache_avro::schema::Schema::String)
            .build()
        );
        #(
            #field_exprs
        )*
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(name)
            .aliases(#enum_aliases)
            .doc(#doc)
            .fields(fields)
            .build()
        )
    }};

    let record_fields_expr = quote! {{
        let mut fields = ::std::vec::Vec::new();
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(::apache_avro::schema::Schema::String)
            .build()
        );
        #(
            #field_exprs
        )*
        Some(fields)
    }};

    Ok(Implementation::named(
        ident,
        generics,
        &container_attrs.name,
        schema_expr,
        Some(record_fields_expr),
        container_attrs
            .default
            .map(json_value_expr)
            .map(|t| quote! { ::std::option::Option::Some(#t)}),
    ))
}
