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

//! Attribute parsing for Avro attributes
//!
//! # Avro attributes
//! Although a user will mostly use the Serde attributes, there are some Avro specific attributes
//! a user can use. These add extra metadata to the generated schema.

use crate::attributes::FieldDefault;
use crate::case::RenameRule;
use darling::FromMeta;
use proc_macro2::Span;
use syn::Expr;

/// All the Avro attributes a container can have.
#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct ContainerAttributes {
    /// Deprecated. Use [`serde::ContainerAttributes::rename`] instead.
    ///
    /// Change the name of this record/enum in the schema.
    ///
    /// [`serde::ContainerAttributes::rename`]: crate::attributes::serde::ContainerAttributes::rename
    #[darling(default)]
    pub name: Option<String>,
    /// Adds a `namespace` field to the schema.
    #[darling(default)]
    pub namespace: Option<String>,
    /// Adds a `doc` field to the schema.
    #[darling(default)]
    pub doc: Option<String>,
    /// Adds the `aliases` field to the schema.
    #[darling(multiple)]
    pub alias: Vec<String>,
    /// Deprecated. Use [`serde::ContainerAttributes::rename_all`] instead.
    ///
    /// Change the name of all fields in the schema according to the [`RenameRule`].
    ///
    /// [`serde::ContainerAttributes::rename_all`]: crate::attributes::serde::ContainerAttributes::rename_all
    #[darling(default)]
    pub rename_all: RenameRule,
    /// Set the default value if this schema is used as a field
    #[darling(default)]
    pub default: Option<String>,
}

impl ContainerAttributes {
    pub fn deprecated(&self, span: Span) {
        if self.name.is_some() {
            super::warn(
                span,
                r#"`#[avro(name = "...")]` is deprecated."#,
                r#"Use `#[serde(rename = "...")]` instead."#,
            )
        }
        if self.rename_all != RenameRule::None {
            super::warn(
                span,
                r#"`#[avro(rename_all = "..")]` is deprecated"#,
                r#"Use `#[serde(rename_all = "..")]` instead"#,
            )
        }
    }
}

/// All the Avro attributes a variant can have.
#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct VariantAttributes {
    /// Deprecated. Use [`serde::VariantAttributes::rename`] instead.
    ///
    /// Changes the name of the variant in the schema.
    ///
    /// [`serde::VariantAttributes::rename`]: crate::attributes::serde::VariantAttributes::rename
    #[darling(default)]
    pub rename: Option<String>,
}

impl VariantAttributes {
    pub fn deprecated(&self, span: Span) {
        if self.rename.is_some() {
            super::warn(
                span,
                r#"`#[avro(rename = "..")]` is deprecated"#,
                r#"Use `#[serde(rename = "..")]` instead"#,
            )
        }
    }
}

/// How to get the schema for a field.
#[derive(Debug, FromMeta, PartialEq, Default)]
#[darling(from_expr = |expr| Ok(With::Expr(expr.clone())))]
pub enum With {
    /// Use `<T as AvroSchemaComponent>::get_schema_in_ctxt`.
    #[default]
    #[darling(skip)]
    Trait,
    /// Use `module::get_schema_in_ctxt` where the module is defined by Serde's `with` attribute.
    #[darling(word, skip)]
    Serde,
    /// Call the function in this expression.
    Expr(Expr),
}

/// All the Avro attributes a field can have.
#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct FieldAttributes {
    /// Adds a `doc` field to the schema.
    #[darling(default)]
    pub doc: Option<String>,
    /// Adds a `default` field to the schema.
    ///
    /// This is also used as the default when `skip_serializing{_if}` is used.
    #[darling(default)]
    pub default: FieldDefault,
    /// Deprecated. Use [`serde::FieldAttributes::alias`] instead.
    ///
    /// Adds the `aliases` field to the schema.
    ///
    /// [`serde::FieldAttributes::alias`]: crate::attributes::serde::FieldAttributes::alias
    #[darling(multiple)]
    pub alias: Vec<String>,
    /// Deprecated. Use [`serde::FieldAttributes::rename`] instead.
    ///
    /// Changes the name of the field in the schema.
    ///
    /// [`serde::FieldAttributes::rename`]: crate::attributes::serde::FieldAttributes::rename
    #[darling(default)]
    pub rename: Option<String>,
    /// Deprecated. Use [`serde::FieldAttributes::skip`] instead.
    ///
    /// Don't include this field in the schema.
    ///
    /// [`serde::FieldAttributes::skip`]: crate::attributes::serde::FieldAttributes::skip
    #[darling(default)]
    pub skip: bool,
    /// Deprecated. Use [`serde::FieldAttributes::flatten`] instead.
    ///
    /// Replace this field by the fields of its schema.
    ///
    /// [`serde::FieldAttributes::flatten`]: crate::attributes::serde::FieldAttributes::flatten
    #[darling(default)]
    pub flatten: bool,
    /// How to get the schema for a field.
    ///
    /// By default uses `<T as AvroSchemaComponent>::get_schema_in_ctxt`.
    ///
    /// When it's provided without an argument (`#[avro(with)]`), it will use the function `get_schema_in_ctxt` defined
    /// in the same module as the `#[serde(with = "some_module")]` attribute.
    ///
    /// When it's provided with an argument (`#[avro(with = some_fn)]`), it will use that function.
    #[darling(default)]
    pub with: With,
}

impl FieldAttributes {
    pub fn deprecated(&self, span: Span) {
        if !self.alias.is_empty() {
            super::warn(
                span,
                r#"`#[avro(alias = "..")]` is deprecated"#,
                r#"Use `#[serde(alias = "..")]` instead"#,
            )
        }
        if self.rename.is_some() {
            super::warn(
                span,
                r#"`#[avro(rename = "..")]` is deprecated"#,
                r#"Use `#[serde(rename = "..")]` instead"#,
            )
        }
        if self.skip {
            super::warn(
                span,
                "`#[avro(skip)]` is deprecated",
                "Use `#[serde(skip)]` instead",
            )
        }
        if self.flatten {
            super::warn(
                span,
                "`#[avro(flatten)]` is deprecated",
                "Use `#[serde(flatten)]` instead",
            )
        }
    }
}
