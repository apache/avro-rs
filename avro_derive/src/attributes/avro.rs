//! Attribute parsing for Avro attributes
//!
//! # Avro attributes
//! Although a user will mostly use the Serde attributes, there are some Avro specific attributes
//! a user can use. These add extra metadata to the generated schema.

use crate::case::RenameRule;

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
    pub default: Option<String>,
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

/// All the Avro attributes a container can have.
#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct ContainerAttributes {
    /// Change the name of this record/enum in the schema.
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
}
