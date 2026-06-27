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

mod bare_union;
mod plain;
mod record_internally_tagged;
mod record_tag_content;
mod union_of_records;

use crate::attributes::{FieldDefault, FieldOptions, NamedTypeOptions, Repr, VariantOptions, With};
use crate::case::RenameRule;
use crate::fields::{field_to_schema_expr, named_fields_to_schema, unnamed_fields_to_schema};
use crate::implementation::Implementation;
use crate::utils::{name_expr, rename_ident};
use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Expr, Fields, Generics, Variant};

/// Generate a schema definition for a enum.
pub fn to_implementation(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    if container_attrs.transparent {
        return Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: `#[serde(transparent)]` is only supported on structs",
        )]);
    }

    match &container_attrs.repr {
        None => {
            if data
                .variants
                .iter()
                .filter(|v| {
                    // Filter skipped variants, if the attributes fail to parse we'll also filter them out
                    // `plain` will throw proper errors for that.
                    VariantOptions::new(&v.attrs, v.span())
                        .map(|o| !o.skip)
                        .unwrap_or(false)
                })
                .all(|v| Fields::Unit == v.fields)
            {
                plain::to_implementation(input_span, ident, generics, container_attrs, data)
            } else {
                union_of_records::to_implementation(ident, generics, container_attrs, data)
            }
        }
        Some(Repr::Enum) => {
            plain::to_implementation(input_span, ident, generics, container_attrs, data)
        }
        Some(Repr::BareUnion) => {
            bare_union::to_implementation(ident, generics, container_attrs, data)
        }
        Some(Repr::UnionOfRecords) => {
            union_of_records::to_implementation(ident, generics, container_attrs, data)
        }
        Some(Repr::RecordTagContent { .. }) => {
            record_tag_content::to_implementation(ident, generics, container_attrs, data)
        }
        Some(Repr::RecordInternallyTagged { .. }) => {
            record_internally_tagged::to_implementation(ident, generics, container_attrs, data)
        }
    }
}

fn newtype_extra_attribute_checks(
    options: FieldOptions,
    span: Span,
) -> Result<FieldOptions, Vec<syn::Error>> {
    let mut errors = Vec::new();
    if options.doc.is_some() {
        errors.push(syn::Error::new(
            span,
            r#"AvroSchema: `#[avro(doc = "..")]` only works on newtype variants when the enum uses `#[avro(repr = "union_of_records")`"#
        ));
    }
    if !matches!(options.default, FieldDefault::Trait) {
        errors.push(syn::Error::new(
            span,
            r#"AvroSchema: `#[avro(default = ..)]` only works on newtype variants when the enum uses `#[avro(repr = "union_of_records")`"#
        ));
    }
    if !options.alias.is_empty() {
        errors.push(syn::Error::new(
            span,
            r#"AvroSchema: `#[avro(alias = "..")]` only works on newtype variants when the enum uses `#[avro(repr = "union_of_records")`"#
        ));
    }
    if options.rename.is_some() {
        errors.push(syn::Error::new(
            span,
            r#"AvroSchema: `#[avro(alias = "..")]` only works on newtype variants when the enum uses `#[avro(repr = "union_of_records")`"#
        ));
    }
    if options.flatten {
        errors.push(syn::Error::new(
            span,
            r#"AvroSchema: `#[avro(flatten)]` only works on newtype variants when the enum uses `#[avro(repr = "union_of_records")`"#,
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(options)
}

fn variant_to_schema_expr(
    variant: Variant,
    variant_attrs: VariantOptions,
    rename_all: RenameRule,
    rename_all_fields: RenameRule,
    transparent_newtype: bool,
    unit_is_null: bool,
) -> Result<TokenStream, Vec<syn::Error>> {
    match variant_attrs.with {
        With::Serde(path) => {
            Ok(quote! { #path::get_schema_in_ctxt(named_schemas, enclosing_namespace) })
        }
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(quote! { (#closure)() })
            } else {
                Err(vec![syn::Error::new(
                    variant.span(),
                    "Expected closure with 0 parameters",
                )])
            }
        }
        With::Expr(Expr::Path(path)) => Ok(quote! { #path(named_schemas, enclosing_namespace) }),
        With::Expr(_expr) => Err(vec![syn::Error::new(
            variant.span(),
            "Invalid expression, expected a function or a closure",
        )]),
        With::Trait => {
            let name = rename_ident(
                &variant.ident,
                variant_attrs.rename,
                rename_all,
                RenameRule::apply_to_variant,
            );

            match variant.fields {
                Fields::Named(fields) => named_fields_to_schema(
                    &name,
                    fields,
                    variant_attrs.rename_all.or(rename_all_fields),
                    variant_attrs.doc,
                    &variant_attrs.aliases,
                ),
                Fields::Unnamed(mut fields) if transparent_newtype && fields.unnamed.len() == 1 => {
                    let pair = fields.unnamed.pop().expect("There is one field");
                    let field = pair.into_value();
                    let field_attributes = FieldOptions::new(&field.attrs, field.span())?;
                    field_to_schema_expr(&field, &field_attributes.with)
                }
                Fields::Unnamed(fields) => unnamed_fields_to_schema(
                    &name,
                    fields,
                    variant_attrs.doc,
                    &variant_attrs.aliases,
                ),
                Fields::Unit if unit_is_null => Ok(quote! {
                    ::apache_avro::schema::Schema::Null
                }),
                Fields::Unit => {
                    let name_expr = name_expr(&name);
                    Ok(quote! {
                        ::apache_avro::schema::Schema::record(#name_expr).build()
                    })
                }
            }
        }
    }
}
