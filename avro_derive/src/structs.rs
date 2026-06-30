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

use crate::attributes::{FieldOptions, NamedTypeOptions};
use crate::fields::{
    field_to_field_default_expr, field_to_record_fields_expr, field_to_schema_expr,
    named_fields_to_record_fields, unnamed_fields_to_record_fields,
};
use crate::implementation::Implementation;
use crate::utils::{aliases, json_value_expr, preserve_optional};
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataStruct, Fields, Generics};

pub fn to_implementation(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataStruct,
) -> Result<Implementation, Vec<syn::Error>> {
    if container_attrs.transparent {
        transparent(input_span, ident, generics, &container_attrs, data)
    } else {
        normal(ident, generics, container_attrs, data)
    }
}

fn normal(
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataStruct,
) -> Result<Implementation, Vec<syn::Error>> {
    let record_fields_expr = match data.fields {
        Fields::Named(fields) => named_fields_to_record_fields(fields, container_attrs.rename_all)?,
        Fields::Unnamed(fields) => unnamed_fields_to_record_fields(fields)?,
        Fields::Unit => {
            quote! {
                ::std::vec::Vec::new()
            }
        }
    };

    let record_doc = preserve_optional(container_attrs.doc.map(|s| quote! { #s.to_string() }));
    let record_aliases = aliases(&container_attrs.aliases);

    let schema_expr = quote! {
        ::apache_avro::schema::Schema::Record(
            ::apache_avro::schema::RecordSchema::builder()
                .aliases(#record_aliases)
                .maybe_doc(#record_doc)
                .fields(#record_fields_expr)
                .name(name)
                .build()
        )
    };

    let record_fields_expr = quote! {
        ::std::option::Option::Some(#record_fields_expr)
    };

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
        container_attrs.tests,
    ))
}

fn transparent(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: &NamedTypeOptions,
    data: DataStruct,
) -> Result<Implementation, Vec<syn::Error>> {
    match data.fields {
        Fields::Named(fields_named) => {
            let mut found = None;
            for field in fields_named.named {
                let attrs = FieldOptions::new(&field.attrs, field.span())?;
                if attrs.skip {
                    continue;
                }
                if found.replace((field, attrs)).is_some() {
                    return Err(vec![syn::Error::new(
                        input_span,
                        "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                    )]);
                }
            }

            if let Some((field, attrs)) = found {
                let field_default_expr = field_to_field_default_expr(&field, attrs.default)?;

                Ok(Implementation::unnamed(
                    ident,
                    generics,
                    field_to_schema_expr(&field, &attrs.with)?,
                    Some(field_to_record_fields_expr(&field, &attrs.with)?),
                    Some(field_default_expr),
                    container_attrs.tests,
                ))
            } else {
                Err(vec![syn::Error::new(
                    input_span,
                    "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                )])
            }
        }
        Fields::Unnamed(_) => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for tuple structs",
        )]),
        Fields::Unit => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for unit structs",
        )]),
    }
}
