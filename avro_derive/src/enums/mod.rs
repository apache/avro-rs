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
use crate::implementation::Implementation;
use proc_macro2::{Ident, Span};
use syn::{DataEnum, Fields, Generics};

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
    if data.variants.iter().all(|v| Fields::Unit == v.fields) {
        plain::to_implementation(input_span, ident, generics, container_attrs, data)
    } else {
        Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for enums with non unit structs",
        )])
    }
}
