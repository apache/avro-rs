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

mod plain;

use crate::attributes::NamedTypeOptions;
use proc_macro2::{Ident, Span, TokenStream};
use syn::{Attribute, DataEnum, Fields, Meta};

/// Generate a schema definition for a enum.
pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    if data_enum.variants.iter().all(|v| Fields::Unit == v.fields) {
        plain::schema_def(container_attrs, data_enum, ident_span)
    } else {
        Err(vec![syn::Error::new(
            ident_span,
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
                "Multiple defaults defined: {:?}",
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
