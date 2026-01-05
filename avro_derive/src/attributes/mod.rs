use darling::FromAttributes;
use proc_macro2::Span;
use syn::Attribute;

use crate::{case::RenameRule, darling_to_syn};

pub mod avro;
pub mod serde;

#[derive(Default)]
pub struct NamedTypeOptions {
    pub name: Option<String>,
    pub namespace: Option<String>,
    pub doc: Option<String>,
    pub alias: Vec<String>,
    pub rename_all: RenameRule,
}

impl NamedTypeOptions {
    pub fn new(attributes: &[Attribute], span: Span) -> Result<Self, Vec<syn::Error>> {
        let avro =
            avro::ContainerAttributes::from_attributes(attributes).map_err(darling_to_syn)?;
        let serde =
            serde::ContainerAttributes::from_attributes(attributes).map_err(darling_to_syn)?;

        // Check for deprecated attributes
        avro.deprecated(span);

        // Collect errors so user gets all feedback at once
        let mut errors = Vec::new();

        // Check for any Serde attributes that are hard errors
        if serde.tag.is_some()
            || serde.content.is_some()
            || serde.untagged
            || serde.variant_identifier
            || serde.field_identifier
        {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support changing the tagging Serde generates (`tag`, `content`, `untagged`, `variant_identifier`, `field_identifier`)",
            ));
        }
        if serde.remote.is_some() {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support the Serde `remote` attribute",
            ));
        }
        if serde.transparent {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support Serde `transparent` attribute",
            ))
        }
        if serde.rename_all.deserialize != serde.rename_all.serialize {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support different rename rules for serializing and deserializing (`rename_all(serialize = \"..\", deserialize = \"..\")`)"
            ))
        }

        // Check for conflicts between Serde and Avro
        if avro.rename_all != RenameRule::None && serde.rename_all.serialize != avro.rename_all {
            errors.push(syn::Error::new(
                span,
                "#[avro(rename_all = \"..\")] must match #[serde(rename_all = \"..\")], it's also deprecated. Please use only `#[serde(rename_all = \"..\")]`",
            ))
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(Self {
            name: avro.name,
            namespace: avro.namespace,
            doc: avro.doc,
            alias: avro.alias,
            rename_all: serde.rename_all.serialize,
        })
    }
}

pub struct VariantOptions {
    pub rename: Option<String>,
}

impl VariantOptions {
    pub fn new(attributes: &[Attribute], span: Span) -> Result<Self, Vec<syn::Error>> {
        let avro = avro::VariantAttributes::from_attributes(attributes).map_err(darling_to_syn)?;
        let serde =
            serde::VariantAttributes::from_attributes(attributes).map_err(darling_to_syn)?;

        // Check for deprecated attributes
        avro.deprecated(span);

        // Collect errors so user gets all feedback at once
        let mut errors = Vec::new();

        // Check for any Serde attributes that are hard errors
        if serde.other || serde.untagged {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support changing the tagging Serde generates (`other`, `untagged`)",
            ));
        }

        if avro.rename.is_some() && serde.rename != avro.rename {
            errors.push(syn::Error::new(
                span,
                "`#[avro(rename = \"..\")]` must match `#[serde(rename = \"..\")]`, it's also deprecated. Please use only `#[serde(rename  = \"..\")]`"
            ));
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(Self {
            rename: serde.rename,
        })
    }
}

pub struct FieldOptions {
    pub doc: Option<String>,
    pub default: Option<String>,
    pub alias: Vec<String>,
    pub rename: Option<String>,
    pub skip: bool,
    pub flatten: bool,
}

impl FieldOptions {
    pub fn new(attributes: &[Attribute], span: Span) -> Result<Self, Vec<syn::Error>> {
        let mut avro =
            avro::FieldAttributes::from_attributes(attributes).map_err(darling_to_syn)?;
        let mut serde =
            serde::FieldAttributes::from_attributes(attributes).map_err(darling_to_syn)?;
        // Sort the aliases, so our check for equality does not fail if they are provided in a different order
        avro.alias.sort();
        serde.alias.sort();

        // Check for deprecated attributes
        avro.deprecated(span);

        // Collect errors so user gets all feedback at once
        let mut errors = Vec::new();

        // Check for any Serde attributes that are hard errors
        if serde.getter.is_some() {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support the Serde `getter` attribute",
            ));
        }

        // Check for conflicts between Serde and Avro
        if avro.skip && !serde.skip {
            errors.push(syn::Error::new(
                span,
                "`#[avro(skip)]` requires `#[serde(skip)]`, it's also deprecated. Please use only `#[serde(skip)]`"
            ));
        }
        if avro.flatten && !serde.flatten {
            errors.push(syn::Error::new(
                span,
                "`#[avro(flatten)]` requires `#[serde(flatten)]`, it's also deprecated. Please use only `#[serde(flatten)]`"
            ));
        }
        // TODO: rename and alias checking can be relaxed with a more complex check, would require the field name
        if avro.rename.is_some() && serde.rename != avro.rename {
            errors.push(syn::Error::new(
                span,
                "`#[avro(rename = \"..\")]` must match `#[serde(rename = \"..\")]`, it's also deprecated. Please use only `#[serde(rename  = \"..\")]`"
            ));
        }
        if !avro.alias.is_empty() && serde.alias != avro.alias {
            errors.push(syn::Error::new(
                span,
                "`#[avro(alias = \"..\")]` must match `#[serde(alias = \"..\")]`, it's also deprecated. Please use only `#[serde(alias  = \"..\")]`"
            ));
        }
        if serde.skip_serializing && serde.skip_deserializing {
            errors.push(syn::Error::new(
                span,
                "Use `#[serde(skip)]` instead of `#[serde(skip_serializing, skip_deserializing)]`",
            ));

            // Don't want to suggest default for skip_serializing as it's not needed for skip
            return Err(errors);
        }
        if (serde.skip_serializing || serde.skip_serializing_if.is_some()) && avro.default.is_none()
        {
            errors.push(syn::Error::new(
                span,
                "`#[serde(skip_serializing)]` and `#[serde(skip_serializing_if)]` require `#[avro(default = \"..\")]`"
            ));
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(Self {
            doc: avro.doc,
            default: avro.default,
            alias: serde.alias,
            rename: serde.rename,
            skip: serde.skip,
            flatten: serde.flatten,
        })
    }
}

#[cfg(nightly)]
/// Emit a compiler warning.
///
/// This is a no-op when the `nightly` feature is not enabled.
fn warn(span: Span, message: &str, help: &str) {
    proc_macro::Diagnostic::spanned(span.unwrap(), proc_macro::Level::Warning, message)
        .help(help)
        .emit()
}

#[cfg(not(nightly))]
/// Emit a compiler warning.
///
/// This is a no-op when the `nightly` feature is not enabled.
fn warn(_span: Span, _message: &str, _help: &str) {}
