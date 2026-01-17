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

use crate::case::RenameRule;
use darling::{FromAttributes, FromMeta};
use proc_macro2::Span;
use syn::{AttrStyle, Attribute, Expr, Ident, Path, spanned::Spanned};

mod avro;
mod serde;

#[derive(Default)]
pub struct NamedTypeOptions {
    pub name: String,
    pub doc: Option<String>,
    pub aliases: Vec<String>,
    pub rename_all: RenameRule,
    pub transparent: bool,
}

impl NamedTypeOptions {
    pub fn new(
        ident: &Ident,
        attributes: &[Attribute],
        span: Span,
    ) -> Result<Self, Vec<syn::Error>> {
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
        if serde.rename_all.deserialize != serde.rename_all.serialize {
            errors.push(syn::Error::new(
                span,
                "AvroSchema derive does not support different rename rules for serializing and deserializing (`rename_all(serialize = \"..\", deserialize = \"..\")`)"
            ));
        }

        // Check for conflicts between Serde and Avro
        if avro.name.is_some() && avro.name != serde.rename {
            errors.push(syn::Error::new(
                span,
                "#[avro(name = \"..\")] must match #[serde(rename = \"..\")], it's also deprecated. Please use only `#[serde(rename = \"..\")]`",
            ));
        }
        if avro.rename_all != RenameRule::None && serde.rename_all.serialize != avro.rename_all {
            errors.push(syn::Error::new(
                span,
                "#[avro(rename_all = \"..\")] must match #[serde(rename_all = \"..\")], it's also deprecated. Please use only `#[serde(rename_all = \"..\")]`",
            ));
        }
        if serde.transparent
            && (serde.rename.is_some()
                || avro.name.is_some()
                || avro.namespace.is_some()
                || avro.doc.is_some()
                || !avro.alias.is_empty()
                || avro.rename_all != RenameRule::None
                || serde.rename_all.serialize != RenameRule::None
                || serde.rename_all.deserialize != RenameRule::None)
        {
            errors.push(syn::Error::new(
                span,
                "#[serde(transparent)] is incompatible with all other attributes",
            ));
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        let name = serde.rename.unwrap_or(ident.to_string());
        let full_schema_name = vec![avro.namespace, Some(name)]
            .into_iter()
            .flatten()
            .collect::<Vec<String>>()
            .join(".");

        let doc = avro.doc.or_else(|| extract_rustdoc(attributes));

        Ok(Self {
            name: full_schema_name,
            doc,
            aliases: avro.alias,
            rename_all: serde.rename_all.serialize,
            transparent: serde.transparent,
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

        // Check for conflicts between Serde and Avro
        if avro.rename.is_some() && serde.rename != avro.rename {
            errors.push(syn::Error::new(
                span,
                "`#[avro(rename = \"..\")]` must match `#[serde(rename = \"..\")]`, it's also deprecated. Please use only `#[serde(rename = \"..\")]`"
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

/// How to get the schema for this field or variant.
#[derive(Debug, PartialEq, Default)]
pub enum With {
    /// Use `<T as AvroSchemaComponent>::get_schema_in_ctxt`.
    #[default]
    Trait,
    /// Use `module::get_schema_in_ctxt` where the module is defined by Serde's `with` attribute.
    Serde(Path),
    /// Call the function in this expression.
    Expr(Expr),
}

impl With {
    fn from_avro_and_serde(
        avro: &avro::With,
        serde: &Option<String>,
        span: Span,
    ) -> Result<Self, syn::Error> {
        match &avro {
            avro::With::Trait => Ok(Self::Trait),
            avro::With::Serde => {
                if let Some(serde) = serde {
                    let path = Path::from_string(serde).map_err(|err| {
                        syn::Error::new(
                            span,
                            format!("Expected a path for `#[serde(with = \"..\")]`: {err:?}"),
                        )
                    })?;
                    Ok(Self::Serde(path))
                } else {
                    Err(syn::Error::new(
                        span,
                        "`#[avro(with)]` requires `#[serde(with = \"some_module\")]` or provide a function to call `#[avro(with = some_fn)]`",
                    ))
                }
            }
            avro::With::Expr(expr) => Ok(Self::Expr(expr.clone())),
        }
    }
}

pub struct FieldOptions {
    pub doc: Option<String>,
    pub default: Option<String>,
    pub alias: Vec<String>,
    pub rename: Option<String>,
    pub skip: bool,
    pub flatten: bool,
    pub with: With,
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
        if avro.skip && !(serde.skip || (serde.skip_serializing && serde.skip_deserializing)) {
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
                "`#[avro(rename = \"..\")]` must match `#[serde(rename = \"..\")]`, it's also deprecated. Please use only `#[serde(rename = \"..\")]`"
            ));
        }
        if !avro.alias.is_empty() && serde.alias != avro.alias {
            errors.push(syn::Error::new(
                span,
                "`#[avro(alias = \"..\")]` must match `#[serde(alias = \"..\")]`, it's also deprecated. Please use only `#[serde(alias = \"..\")]`"
            ));
        }
        if ((serde.skip_serializing && !serde.skip_deserializing)
            || serde.skip_serializing_if.is_some())
            && avro.default.is_none()
        {
            errors.push(syn::Error::new(
                span,
                "`#[serde(skip_serializing)]` and `#[serde(skip_serializing_if)]` require `#[avro(default = \"..\")]`"
            ));
        }
        let with = match With::from_avro_and_serde(&avro.with, &serde.with, span) {
            Ok(with) => with,
            Err(error) => {
                errors.push(error);
                // This won't actually be used, but it does simplify the code
                With::Trait
            }
        };

        if !errors.is_empty() {
            return Err(errors);
        }

        let doc = avro.doc.or_else(|| extract_rustdoc(attributes));

        Ok(Self {
            doc,
            default: avro.default,
            alias: serde.alias,
            rename: serde.rename,
            skip: serde.skip || (serde.skip_serializing && serde.skip_deserializing),
            flatten: serde.flatten,
            with,
        })
    }
}

fn extract_rustdoc(attributes: &[Attribute]) -> Option<String> {
    let doc = attributes
        .iter()
        .filter(|attr| attr.style == AttrStyle::Outer && attr.path().is_ident("doc"))
        .filter_map(|attr| {
            let name_value = attr.meta.require_name_value();
            match name_value {
                Ok(name_value) => match &name_value.value {
                    syn::Expr::Lit(expr_lit) => match expr_lit.lit {
                        syn::Lit::Str(ref lit_str) => Some(lit_str.value().trim().to_string()),
                        _ => None,
                    },
                    _ => None,
                },
                Err(_) => None,
            }
        })
        .collect::<Vec<String>>()
        .join("\n");
    if doc.is_empty() { None } else { Some(doc) }
}

fn darling_to_syn(e: darling::Error) -> Vec<syn::Error> {
    let msg = format!("{e}");
    let token_errors = e.write_errors();
    vec![syn::Error::new(token_errors.span(), msg)]
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
