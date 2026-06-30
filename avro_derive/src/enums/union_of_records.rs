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
use crate::enums::{SchemaType, variant_to_schema_expr};
use crate::implementation::Implementation;
use crate::utils::json_value_expr;
use proc_macro2::Ident;
use quote::quote;
use std::collections::HashSet;
use syn::spanned::Spanned;
use syn::{DataEnum, Generics};

pub fn to_implementation(
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let mut errors = Vec::new();
    let mut variant_exprs = Vec::new();

    let mut names = HashSet::new();

    for variant in data.variants {
        let variant_attrs = match VariantOptions::new(&variant.attrs, variant.span()) {
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
            false,
            false,
            |info| {
                if let SchemaType::Named(name) = info.schema_type
                    && !names.insert(name.to_string())
                {
                    Err(format!(
                        "AvroSchema: Duplicate variant names detected: {name}"
                    ))
                } else {
                    Ok(())
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

        #(builder.variant(#variant_exprs).expect("Duplicate names found");)*

        ::apache_avro::schema::Schema::Union(builder.build())
    }};

    Ok(Implementation::unnamed(
        ident,
        generics,
        schema_expr,
        None,
        container_attrs
            .default
            .map(json_value_expr)
            .map(|t| quote! { ::std::option::Option::Some(#t)}),
        container_attrs.tests,
    ))
}
