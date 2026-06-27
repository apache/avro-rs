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
use crate::case::RenameRule;
use crate::enums::variant_to_schema_expr;
use crate::implementation::Implementation;
use crate::utils::{aliases, json_value_expr, name_expr, preserve_optional, rename_ident};
use proc_macro2::Ident;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Generics};

pub fn to_implementation(
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataEnum,
) -> Result<Implementation, Vec<syn::Error>> {
    let Some(Repr::RecordTagContent { tag, content }) = container_attrs.repr else {
        unreachable!()
    };
    let mut errors = Vec::new();
    let mut symbols = Vec::new();
    let mut variant_exprs = Vec::new();

    for variant in data.variants {
        let variant_attrs = VariantOptions::new(&variant.attrs, variant.span())?;

        if variant_attrs.skip {
            continue;
        }

        let name = rename_ident(
            &variant.ident,
            variant_attrs.rename.clone(),
            container_attrs.rename_all,
            RenameRule::apply_to_variant,
        );

        match variant_to_schema_expr(
            variant,
            variant_attrs,
            container_attrs.rename_all,
            container_attrs.rename_all_fields,
            true,
            true,
        ) {
            Ok(expr) => variant_exprs.push(expr),
            Err(errs) => errors.extend(errs),
        }

        symbols.push(name);
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
    let tag_name_expr = name_expr(&tag);

    let schema_expr = quote! {{
        let mut builder = ::apache_avro::schema::UnionSchema::builder();
        #(builder.variant_ignore_duplicates(#variant_exprs).expect("Got two map or arrays with different inner types");)*

        let content_contains_null = builder.contains(&::apache_avro::schema::Schema::Null);
        let content_schema = ::apache_avro::schema::Schema::Union(builder.build());

        let tag_schema = ::apache_avro::schema::Schema::r#enum(#tag_name_expr, vec![#(#symbols.to_owned()),*]).build();

        let mut fields = ::std::vec::Vec::with_capacity(2);
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(tag_schema)
            .build()
        );
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#content)
            .schema(content_schema)
            // This is needed for unit variants, for which Serde won't serialize content
            .maybe_default(if content_contains_null { Some(::serde_json::Value::Null) } else { None })
            .build()
        );
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(name)
            .aliases(#enum_aliases)
            .doc(#doc)
            .fields(fields)
            .build()
        )
    }};

    let record_fields_expr = quote! {{
        let mut builder = ::apache_avro::schema::UnionSchema::builder();
        #(builder.variant_ignore_duplicates(#variant_exprs).expect("Got two map or arrays with different inner types");)*

        let content_contains_null = builder.contains(&::apache_avro::schema::Schema::Null);
        let content_schema = ::apache_avro::schema::Schema::Union(builder.build());

        let tag_schema = ::apache_avro::schema::Schema::r#enum(#tag_name_expr, vec![#(#symbols.to_owned()),*]).build();

        let mut fields = ::std::vec::Vec::with_capacity(2);
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(tag_schema)
            .build()
        );
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#content)
            .schema(content_schema)
            // This is needed for unit variants, for which Serde won't serialize content
            .maybe_default(if content_contains_null { Some(::serde_json::Value::Null) } else { None })
            .build()
        );

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
