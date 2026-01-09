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

//! Attribute parsing for Serde attributes
//!
//! # Serde attributes
//! This module only parses the minimal amount to be able to read Serde attributes. This means
//! most attributes are decoded as an `Option<String>` as the actual content is not relevant.
//! Attributes which are not needed by the derive are prefixed with a `_` as we can't ignore them
//! (this would be a compile error).
//!
//! If Serde adds new attributes they need to be added here too.

use darling::{FromAttributes, FromMeta};
use syn::Expr;

use crate::case::RenameRule;

/// Represents `rename_all` and `rename_all_fields`.
///
/// These attributes can take the following forms:
///   1. `rename_all = ".."`
///   2. `rename_all(serialize = "..", deserialize = "..")`
///
/// To parse the first form, we use Darling's `from_expr`.
/// To parse the second form, we use Darling's `FromMeta` derive.
#[derive(Debug, Default, FromMeta, PartialEq, Eq)]
#[darling(from_expr = RenameAll::from_expr)]
pub struct RenameAll {
    #[darling(default)]
    pub serialize: RenameRule,
    #[darling(default)]
    pub deserialize: RenameRule,
}

impl From<RenameRule> for RenameAll {
    fn from(value: RenameRule) -> Self {
        Self {
            serialize: value,
            deserialize: value,
        }
    }
}

impl RenameAll {
    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        let Expr::Lit(lit) = expr else {
            return Err(darling::Error::custom("Expected a string literal!"));
        };
        let rule = RenameRule::from_value(&lit.lit)?;
        Ok(RenameAll::from(rule))
    }
}

/// Represents `default`.
///
/// This attribute can take the following forms:
///   1. `default`
///   2. `default = ".."`
///
/// To parse the first form, we use Darling's `FromMeta` derive with `word, skip`.
/// To parse the second form, we use Darling's `from_expr`.
#[derive(Debug, FromMeta, PartialEq)]
#[darling(from_expr = |expr| Ok(SerdeDefault::Expr(expr.clone())))]
pub enum SerdeDefault {
    #[darling(word, skip)]
    UseTrait,
    Expr(Expr),
}

/// All Serde attributes that a container can have.
#[derive(Debug, FromAttributes)]
#[darling(attributes(serde))]
pub struct ContainerAttributes {
    /// Rename this container.
    pub rename: Option<String>,
    /// Rename all the fields (if this is a struct) or variants (if this is an enum) according to the given case convention.
    #[darling(default)]
    pub rename_all: RenameAll,
    /// Rename all the fields of the struct variants in this enum.
    #[darling(default, rename = "rename_all_fields")]
    pub _rename_all_fields: RenameAll,
    /// Error when encountering unknown fields when deserialising.
    #[darling(default, rename = "deny_unknown_fields")]
    pub _deny_unknown_fields: bool,
    /// Add/expect a tag during serialisation.
    ///
    /// When used on a struct, this adds an extra field which is not in the schema definition.
    /// When used on an enum, serde transforms it into a struct which does not match the schema definition.
    pub tag: Option<String>,
    /// Put the content in a field with this name.
    pub content: Option<String>,
    /// This makes the enum transparent, (de)serializing based on the variant directly.
    ///
    /// This does not match the schema definition.
    #[darling(default)]
    pub untagged: bool,
    /// Add a bound to the Serialize/Deserialize trait.
    #[darling(default, rename = "bound")]
    pub _bound: Option<String>,
    /// When deserializing, any missing fields should be filled in from the struct's implementation of `Default`.
    #[darling(rename = "default")]
    pub _default: Option<SerdeDefault>,
    /// This type is the serde implementation for a "remote" type.
    ///
    /// This makes the (de)serialisation use/return a different type.
    pub remote: Option<String>,
    /// Directly use the inner type for (de)serialisation.
    #[darling(default)]
    pub transparent: bool,
    /// Deserialize using the given type and then convert to this type with `From`.
    #[darling(default, rename = "from")]
    pub _from: Option<String>,
    /// Deserialize using the given type and then convert to this type with `TryFrom`.
    #[darling(default, rename = "try_from")]
    pub _try_from: Option<String>,
    /// Convert this type to the given type using `Into` and then serialize using the given type.
    #[darling(default, rename = "into")]
    pub _into: Option<String>,
    /// Use the Serde API at this path.
    #[darling(default, rename = "crate")]
    pub _crate: Option<String>,
    /// Custom error text.
    #[darling(default, rename = "expecting")]
    pub _expecting: Option<String>,
    /// This does something with tags, which are incompatible.
    #[darling(default)]
    pub variant_identifier: bool,
    /// This does something with tags, which are incompatible.
    #[darling(default)]
    pub field_identifier: bool,
}

/// All Serde attributes that a enum variant can have.
#[derive(Debug, FromAttributes)]
#[darling(attributes(serde))]
pub struct VariantAttributes {
    /// Rename the variant.
    #[darling(default)]
    pub rename: Option<String>,
    /// Aliases for this variant, only used during deserialisation.
    #[darling(multiple, rename = "alias")]
    pub _alias: Vec<String>,
    #[darling(default, rename = "rename_all")]
    pub _rename_all: RenameAll,
    /// Do not serialize or deserialize this variant.
    #[darling(default, rename = "skip")]
    pub _skip: bool,
    /// Do not serialize this variant.
    #[darling(default, rename = "skip_serializing")]
    pub _skip_serializing: bool,
    /// Do not deserialize this variant.
    #[darling(default, rename = "skip_deserializing")]
    pub _skip_deserializing: bool,
    /// Use this function for serializing.
    #[darling(rename = "serialize_with")]
    pub _serialize_with: Option<String>,
    /// Use this function for deserializing.
    #[darling(rename = "deserialize_with")]
    pub _deserialize_with: Option<String>,
    /// Use this module for (de)serializing.
    #[darling(rename = "with")]
    pub _with: Option<String>,
    /// Put trait bounds on the implementations.
    #[darling(rename = "bound")]
    pub _bound: Option<String>,
    /// Put bounds on the lifetimes.
    #[darling(rename = "borrow")]
    pub _borrow: Option<SerdeBorrow>,
    /// This does something with tags, which are incompatible.
    #[darling(default)]
    pub other: bool,
    /// (De)serialize this variant as if it was not part of the enum.
    #[darling(default)]
    pub untagged: bool,
}

/// Represents `borrow`.
///
/// This attribute can take the following forms:
///   1. `borrow`
///   2. `borrow = ".."`
///
/// To parse the first form, we use Darling's `FromMeta` derive with `word, skip`.
/// To parse the second form, we use Darling's `from_expr`.
#[derive(Debug, FromMeta, PartialEq)]
#[darling(from_expr = |expr| Ok(SerdeBorrow::Expr(expr.clone())))]
pub enum SerdeBorrow {
    #[darling(word, skip)]
    Default,
    Expr(Expr),
}

/// All Serde attributes that a field can have.
#[derive(Debug, FromAttributes)]
#[darling(attributes(serde))]
pub struct FieldAttributes {
    /// Rename the field.
    #[darling(default)]
    pub rename: Option<String>,
    /// Aliases for this field, only used during deserialisation.
    #[darling(multiple)]
    pub alias: Vec<String>,
    /// When deserializing, if this field is missing use `Default` or the given function.
    #[darling(rename = "default")]
    pub _default: Option<SerdeDefault>,
    #[darling(default)]
    pub flatten: bool,
    /// Do not serialize or deserialize this field.
    #[darling(default)]
    pub skip: bool,
    /// Do not serialize this field.
    #[darling(default)]
    pub skip_serializing: bool,
    /// Do not deserialize this field.
    #[darling(default)]
    pub skip_deserializing: bool,
    /// Do not serialize this field if the function returns `false`.
    pub skip_serializing_if: Option<String>,
    /// Use this function for serializing.
    #[darling(rename = "serialize_with")]
    pub _serialize_with: Option<String>,
    /// Use this function for deserializing.
    #[darling(rename = "deserialize_with")]
    pub _deserialize_with: Option<String>,
    /// Use this module for (de)serializing.
    pub with: Option<String>,
    /// Put bounds on the lifetimes.
    #[darling(rename = "borrow")]
    pub _borrow: Option<SerdeBorrow>,
    /// Used for remote types.
    pub getter: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::{
        RenameRule,
        attributes::serde::{ContainerAttributes, RenameAll, SerdeDefault},
    };
    use darling::FromAttributes;
    use syn::DeriveInput;

    #[test]
    fn test_rename_all() {
        let derive: DeriveInput = syn::parse_quote! {
            #[serde(rename_all = "lowercase")]
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(input.rename_all, RenameAll::from(RenameRule::LowerCase));

        let derive: DeriveInput = syn::parse_quote! {
            #[serde(rename_all(serialize = "lowercase"))]
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(
            input.rename_all,
            RenameAll {
                serialize: RenameRule::LowerCase,
                deserialize: RenameRule::None
            }
        );

        let derive: DeriveInput = syn::parse_quote! {
            #[serde(rename_all(deserialize = "lowercase"))]
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(
            input.rename_all,
            RenameAll {
                serialize: RenameRule::None,
                deserialize: RenameRule::LowerCase
            }
        );

        let derive: DeriveInput = syn::parse_quote! {
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(input.rename_all, RenameAll::from(RenameRule::None));
    }

    #[test]
    fn test_default() {
        let derive: DeriveInput = syn::parse_quote! {
            #[serde(default)]
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(input._default, Some(SerdeDefault::UseTrait));

        let derive: DeriveInput = syn::parse_quote! {
            #[serde(default = "some::path")]
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert!(matches!(input._default, Some(SerdeDefault::Expr(_))));

        let derive: DeriveInput = syn::parse_quote! {
            struct Config {
                field: String,
            }
        };
        let input = ContainerAttributes::from_attributes(&derive.attrs).unwrap();
        assert_eq!(input._default, None);
    }
}
