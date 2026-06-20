use crate::attributes::{FieldOptions, NamedTypeOptions};
use crate::case::RenameRule;
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

use crate::fields::{
    field_to_field_default_expr, field_to_record_fields_expr, field_to_schema_expr,
};
use crate::implementation::Implementation;
use crate::utils::{aliases, field_aliases, preserve_optional};
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
        transparent(input_span, ident, generics, container_attrs, data)
    } else {
        normal(input_span, ident, generics, container_attrs, data)
    }
}

fn normal(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
    data: DataStruct,
) -> Result<Implementation, Vec<syn::Error>> {
    let mut record_field_exprs = vec![];
    match data.fields {
        Fields::Named(a) => {
            for field in a.named {
                let mut name = field
                    .ident
                    .as_ref()
                    .expect("Field must have a name")
                    .to_string();
                if let Some(raw_name) = name.strip_prefix("r#") {
                    name = raw_name.to_string();
                }
                let field_attrs = FieldOptions::new(&field.attrs, field.span())?;
                let doc = preserve_optional(field_attrs.doc);
                match (field_attrs.rename, container_attrs.rename_all) {
                    (Some(rename), _) => {
                        name = rename;
                    }
                    (None, rename_all) if rename_all != RenameRule::None => {
                        name = rename_all.apply_to_field(&name);
                    }
                    _ => {}
                }
                if field_attrs.skip {
                    continue;
                } else if field_attrs.flatten {
                    // Inline the fields of the child record at runtime, as we don't have access to
                    // the schema here.
                    let get_record_fields = field_to_record_fields_expr(&field, &field_attrs.with)?;
                    record_field_exprs.push(quote! {
                        if let Some(flattened_fields) = #get_record_fields {
                            schema_fields.extend(flattened_fields);
                        } else {
                            panic!("{} does not have any fields to flatten to", stringify!(#field));
                        }
                    });

                    // Don't add this field as it's been replaced by the child record fields
                    continue;
                }
                let default_value = field_to_field_default_expr(&field, field_attrs.default)?;
                let aliases = field_aliases(&field_attrs.alias);
                let schema_expr = field_to_schema_expr(&field, &field_attrs.with)?;
                record_field_exprs.push(quote! {
                    schema_fields.push(::apache_avro::schema::RecordField {
                        name: #name.to_string(),
                        doc: #doc,
                        default: #default_value,
                        aliases: #aliases,
                        schema: #schema_expr,
                        custom_attributes: ::std::collections::BTreeMap::new(),
                    });
                });
            }
        }
        Fields::Unnamed(_) => {
            return Err(vec![syn::Error::new(
                input_span,
                "AvroSchema derive does not work for tuple structs",
            )]);
        }
        Fields::Unit => {
            return Err(vec![syn::Error::new(
                input_span,
                "AvroSchema derive does not work for unit structs",
            )]);
        }
    }

    let record_doc = preserve_optional(container_attrs.doc.as_ref());
    let record_aliases = aliases(&container_attrs.aliases);
    let full_schema_name = &container_attrs.name;

    // When flatten is involved, there will be more but we don't know how many. This optimises for
    // the most common case where there is no flatten.
    let minimum_fields = record_field_exprs.len();

    let schema_expr = quote! {
        {
            let mut schema_fields = ::std::vec::Vec::with_capacity(#minimum_fields);
            #(#record_field_exprs)*
            let schema_field_set: ::std::collections::HashSet<_> = schema_fields.iter().map(|rf| &rf.name).collect();
            assert_eq!(schema_fields.len(), schema_field_set.len(), "Duplicate field names found: {schema_fields:?}");
            let name = ::apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse struct name for schema {}", #full_schema_name)[..]);
            let lookup: ::std::collections::BTreeMap<String, usize> = schema_fields
                .iter()
                .enumerate()
                .map(|(position, field)| (field.name.to_owned(), position))
                .collect();
            ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema {
                name,
                aliases: #record_aliases,
                doc: #record_doc,
                fields: schema_fields,
                lookup,
                attributes: ::std::collections::BTreeMap::new(),
            })
        }
    };
    let record_fields_expr = quote! {
        let mut schema_fields = ::std::vec::Vec::with_capacity(#minimum_fields);
        #(#record_field_exprs)*
        ::std::option::Option::Some(schema_fields)
    };

    Ok(Implementation::named(
        ident,
        generics,
        &container_attrs.name,
        schema_expr,
        Some(record_fields_expr),
        container_attrs.default,
    ))
}

fn transparent(
    input_span: Span,
    ident: Ident,
    generics: Generics,
    container_attrs: NamedTypeOptions,
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
                let field_default_expr = if container_attrs.default.is_none() {
                    Some(field_to_field_default_expr(&field, attrs.default)?)
                } else {
                    container_attrs.default
                };

                Ok(Implementation::unnamed(
                    ident,
                    generics,
                    field_to_schema_expr(&field, &attrs.with)?,
                    Some(field_to_record_fields_expr(&field, &attrs.with)?),
                    field_default_expr,
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
