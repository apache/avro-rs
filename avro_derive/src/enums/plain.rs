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
use crate::implementation::Implementation;
use crate::{aliases, preserve_optional};
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Attribute, DataEnum, Fields, Generics, Meta};

pub fn to_implementation(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    if data.variants.iter().all(|v| Fields::Unit == v.fields) {
        let default_value = default_enum_variant(&data, input_span)?;
        let default = preserve_optional(default_value);
        let mut symbols = Vec::new();
        for variant in data.variants {
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
        let schema_expr = quote! {
            ::apache_avro::schema::Schema::Enum(::apache_avro::schema::EnumSchema {
                name,
                aliases: #enum_aliases,
                doc: #doc,
                symbols: vec![#(#symbols.to_owned()),*],
                default: #default,
                attributes: ::std::collections::BTreeMap::new(),
            })
        };

        Ok(Implementation::named(
            ident,
            generics,
            &container_attrs.name,
            schema_expr,
            None,
            container_attrs.default,
        ))
    } else {
        Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for enums with non unit structs",
        )])
    }
}

fn default_enum_variant(
    data_enum: &DataEnum,
    error_span: Span,
) -> Result<Option<String>, Vec<syn::Error>> {
    match data_enum
        .variants
        .iter()
        .filter(|v| v.attrs.iter().any(is_default_attr))
        .collect::<Vec<_>>()
    {
        variants if variants.is_empty() => Ok(None),
        single if single.len() == 1 => Ok(Some(single[0].ident.to_string())),
        multiple => Err(vec![syn::Error::new(
            error_span,
            format!(
                "AvroSchema: Multiple defaults defined: {:?}",
                multiple
                    .iter()
                    .map(|v| v.ident.to_string())
                    .collect::<Vec<String>>()
            ),
        )]),
    }
}

fn is_default_attr(attr: &Attribute) -> bool {
    matches!(attr, Attribute { meta: Meta::Path(path), .. } if path.get_ident().map(Ident::to_string).as_deref() == Some("default"))
}
