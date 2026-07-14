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
use crate::utils::{aliases, json_value_expr, preserve_optional, rename_ident};
use proc_macro2::{Ident, Span};
use quote::quote;
use serde_json::Value;
use syn::spanned::Spanned;
use syn::{Attribute, DataEnum, Fields, Generics, Meta};

pub fn to_implementation(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let doc = preserve_optional(
        container_attrs
            .doc
            .as_ref()
            .map(|s| quote! { #s.to_string() }),
    );
    let enum_aliases = aliases(&container_attrs.aliases);

    let mut errors = Vec::new();
    let mut symbols = Vec::new();
    let mut default = match &container_attrs.default {
        None => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => {
            errors.push(syn::Error::new(
                input_span,
                "Expected a JSON string for an enum default",
            ));
            None
        }
    };

    for variant in data.variants {
        let variant_attrs = match VariantOptions::new(&variant.attrs, variant.span()) {
            Ok(attrs) => attrs,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        // Check the skip attribute before the check if it is a unit variant, because a skipped (un)named
        // variant is not a problem
        if variant_attrs.skip {
            continue;
        }

        if !matches!(variant.fields, Fields::Unit) {
            errors.push(syn::Error::new(
                variant.span(),
                "AvroSchema: derive does not work for enums with non unit structs",
            ));
            continue;
        } else if !variant_attrs.only_skip_rename_and_alias_can_be_set() {
            errors.push(syn::Error::new(
                variant.span(),
                "AvroSchema: On unit variants, only the `skip`, `rename` and `alias` attributes are allowed to be set",
            ));
            continue;
        }

        let name = rename_ident(
            &variant.ident,
            variant_attrs.rename,
            container_attrs.rename_all,
            RenameRule::apply_to_variant,
        );

        // When a variant has the `#[default]` attribute and we don't have a default yet, we assign
        // it as the default. If an enum has multiple variants with the default attribute, the compiler
        // throws an error anyway so we can ignore that situation. If the default attribute is on a
        // skipped variant, we ignore it as Serde will throw an error if we try to deserialize that
        // variant.
        if default.is_none() && has_default_attr(&variant.attrs) {
            default = Some(name.clone());
        }

        symbols.push(name);
    }

    if let Some(default) = &default
        && symbols.iter().all(|n| default != n)
    {
        errors.push(syn::Error::new(
            input_span,
            format!("The enum default {default} is not one of the variants: {symbols:?}"),
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let default = preserve_optional(default.map(|d| quote! { ::std::string::String::from(#d)}));
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
        container_attrs
            .default
            .map(json_value_expr)
            .map(|t| quote! { ::std::option::Option::Some(#t)}),
    ))
}

fn has_default_attr(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| {
        matches!(attr, Attribute { meta: Meta::Path(path), .. } if path.get_ident().map(Ident::to_string).as_deref() == Some("default"))
    })
}
