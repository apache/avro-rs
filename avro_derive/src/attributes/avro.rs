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

use crate::case::RenameRule;
use darling::FromMeta;
use proc_macro2::Span;
use serde_json::Value;
use syn::Expr;

/// What `Schema` representation to generate for a type.
#[derive(Debug, FromMeta, PartialEq, Clone, Copy)]
pub enum Repr {
    /// Generate a `Schema::Enum` for a `enum`.
    ///
    /// Only works for unit variants.
    Enum,
    /// Generate a `Schema::Union` for a `enum`.
    ///
    /// Requires `#[serde(untagged)]`
    ///
    /// There can only be one unit variant and every newtype/struct/tuple variant must be unique.
    BareUnion,
    /// Generate a `Schema::Union` with a `Schema::Record`  for a `enum`.
    ///
    /// This works for every enum as the records will have unique names for every variant.
    UnionOfRecords,
    /// Generate a `Schema::Record` with a tag and content field for a `enum`.
    ///
    /// Requires `#[serde(tag = "..", content = "..")]`.
    RecordTagContent,
    /// Generate a `Schema::Record` with a tag field and flattened variant fields for a `enum`.
    ///
    /// Requires `#[serde(tag = "..")]`
    RecordInternallyTagged,
}

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
    /// Force the generator to use a certain schema representation
    #[darling(default)]
    pub repr: Option<Repr>,
}

impl ContainerAttributes {
    pub fn deprecated(&self, span: Span) {
        if self.name.is_some() {
            super::warn(
                span,
                r#"`#[avro(name = "...")]` is deprecated."#,
                r#"Use `#[serde(rename = "...")]` instead."#,
            );
        }
        if self.rename_all != RenameRule::None {
            super::warn(
                span,
                r#"`#[avro(rename_all = "..")]` is deprecated"#,
                r#"Use `#[serde(rename_all = "..")]` instead"#,
            );
        }
    }
}

/// How to get the schema for a variant.
#[derive(Debug, PartialEq, Default)]
pub enum With {
    /// Use `<T as AvroSchemaComponent>::get_schema_in_ctxt`.
    #[default]
    Trait,
    /// Use `module::get_schema_in_ctxt` where the module is defined by Serde's `with` attribute.
    Serde,
    /// Call the function in this expression.
    Expr(Expr),
}

impl FromMeta for With {
    fn from_word() -> darling::Result<Self> {
        Ok(Self::Serde)
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        Ok(Self::Expr(expr.clone()))
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
    /// How to get the schema for a variant.
    ///
    /// By default uses `<T as AvroSchemaComponent>::get_schema_in_ctxt`.
    ///
    /// When it's provided without an argument (`#[avro(with)]`), it will use the function `get_schema_in_ctxt` defined
    /// in the same module as the `#[serde(with = "some_module")]` attribute.
    ///
    /// When it's provided with an argument (`#[avro(with = some_fn)]`), it will use that function.
    #[darling(default)]
    pub with: With,
    /// Adds a `doc` field to the schema.
    #[darling(default)]
    pub doc: Option<String>,
}

impl VariantAttributes {
    pub fn deprecated(&self, span: Span) {
        if self.rename.is_some() {
            super::warn(
                span,
                r#"`#[avro(rename = "..")]` is deprecated"#,
                r#"Use `#[serde(rename = "..")]` instead"#,
            );
        }
    }
}

/// How to get the default value for a value.
#[derive(Debug, PartialEq, Default)]
pub enum FieldDefault {
    /// Use `<T as AvroSchemaComponent>::field_default`.
    #[default]
    Trait,
    /// Don't set a default.
    Disabled,
    /// Use `module::field_default` where the module is defined by Serde's `with` attribute.
    Serde,
    /// Use this JSON value.
    Value(Value),
    /// Call the function in this expression.
    Expr(Expr),
}

impl FromMeta for FieldDefault {
    fn from_word() -> darling::Result<Self> {
        Ok(Self::Serde)
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Lit(lit) => Self::from_value(&lit.lit),
            Expr::Group(group) => {
                // syn may generate this invisible group delimiter when the input to the darling
                // proc macro (specifically, the attributes) are generated by a
                // macro_rules! (e.g. propagating a macro_rules!'s expr)
                // Since we want to basically ignore these invisible group delimiters,
                // we just propagate the call to the inner expression.
                Self::from_expr(&group.expr)
            }
            expr => Ok(Self::Expr(expr.clone())),
        }
    }

    fn from_string(value: &str) -> darling::Result<Self> {
        serde_json::from_str(value)
            .map(Self::Value)
            .map_err(darling::Error::custom)
    }

    fn from_bool(value: bool) -> darling::Result<Self> {
        if value {
            Err(darling::Error::custom(
                "Expected `false` or a JSON string, got `true`",
            ))
        } else {
            Ok(Self::Disabled)
        }
    }
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
            );
        }
        if self.rename.is_some() {
            super::warn(
                span,
                r#"`#[avro(rename = "..")]` is deprecated"#,
                r#"Use `#[serde(rename = "..")]` instead"#,
            );
        }
        if self.skip {
            super::warn(
                span,
                "`#[avro(skip)]` is deprecated",
                "Use `#[serde(skip)]` instead",
            );
        }
        if self.flatten {
            super::warn(
                span,
                "`#[avro(flatten)]` is deprecated",
                "Use `#[serde(flatten)]` instead",
            );
        }
    }
}
