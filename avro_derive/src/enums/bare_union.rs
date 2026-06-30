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

use crate::attributes::{NamedTypeOptions, Repr, VariantOptions};
use crate::enums::{FieldVariants, SchemaType, variant_to_schema_expr};
use crate::implementation::Implementation;
use crate::utils::json_value_expr;
use proc_macro2::Ident;
use quote::quote;
use std::collections::{BTreeSet, HashSet};
use syn::spanned::Spanned;
use syn::{DataEnum, Generics};

pub fn to_implementation(
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let Some(Repr::BareUnion { untagged }) = container_attrs.repr else {
        unreachable!()
    };

    let mut errors = Vec::new();
    let mut variant_exprs = Vec::new();

    // Used for checking that the resulting schema is useful
    let mut have_null = false;
    let mut names = HashSet::new();
    // This is a BTreeSet so the error output is deterministic for our UI tests
    let mut tuple_sizes = BTreeSet::new();

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
        match variant_to_schema_expr(
            variant,
            variant_attrs,
            container_attrs.rename_all,
            container_attrs.rename_all_fields,
            true,
            true,
            |info| {
                match (info.schema_type, info.variant) {
                    (_, FieldVariants::Named(0)) if untagged => {
                        Err("AvroSchema: Empty struct variants are not allowed for `#[serde(untagged)]`".to_string())
                    }
                    (_, FieldVariants::Unnamed(len)) if untagged && len >= 2 && !tuple_sizes.insert(len) => {
                        // Tuples of size 2 or larger are represented as a Schema::Record with an "org.apache.avro.rust.tuple" attribute.
                        // As the only information Serde provides for untagged enums is the size of the tuple, the amount of fields in
                        // records with the tuple tag must be unique for it to find the correct variant.
                        // This check is not watertight as it will fail for `enum Abc { A((i32, i32)), B(i64, i64) }`
                        // as we don't know the schema for the newtype inside Abc::A.
                        Err(format!("AvroSchema: Duplicate tuple sizes detected which is incompatible with `#[serde(untagged)]`: new: {len}, already seen: {tuple_sizes:?}"))
                    }
                    (SchemaType::Null, _) if have_null => Err(r#"AvroSchema: Two variants resolve to Schema::Null, this is not supported for `#[avro(repr = "bare_union")]`"#.to_string()),
                    (SchemaType::Null, _) => { have_null = true; Ok(()) },
                    (SchemaType::Named(name), _) if !names.insert(name.to_string())=> Err(format!("AvroSchema: Duplicate variant names detected: {name}")),
                    _ => Ok(())
                }
            },
        ) {
            Ok(expr) => variant_exprs.push(expr),
            Err(errs) => errors.extend(errs),
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let schema_expr = quote! {{
        let mut builder = ::apache_avro::schema::UnionSchema::builder();
        #(builder.variant(#variant_exprs).expect("Duplicate Schema found");)*
        ::apache_avro::schema::Schema::Union(builder.build())
    }};

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
        container_attrs.tests,
    ))
}
