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

use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::case::RenameRule;
use crate::enums::default_enum_variant;
use crate::{aliases, preserve_optional};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    if data_enum.variants.iter().all(|v| Fields::Unit == v.fields) {
        let default_value = default_enum_variant(&data_enum, ident_span)?;
        let default = preserve_optional(default_value);
        let mut symbols = Vec::new();
        for variant in &data_enum.variants {
            let field_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
            let name = match (field_attrs.rename, container_attrs.rename_all) {
                (Some(rename), _) => rename,
                (None, rename_all) if !matches!(rename_all, RenameRule::None) => {
                    rename_all.apply_to_variant(&variant.ident.to_string())
                }
                _ => variant.ident.to_string(),
            };
            symbols.push(name);
        }
        let full_schema_name = &container_attrs.name;
        Ok(quote! {
            ::apache_avro::schema::Schema::Enum(apache_avro::schema::EnumSchema {
                name: ::apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse enum name for schema {}", #full_schema_name)[..]),
                aliases: #enum_aliases,
                doc: #doc,
                symbols: vec![#(#symbols.to_owned()),*],
                default: #default,
                attributes: Default::default(),
            })
        })
    } else {
        Err(vec![syn::Error::new(
            ident_span,
            "AvroSchema: derive does not work for enums with non unit structs",
        )])
    }
}
