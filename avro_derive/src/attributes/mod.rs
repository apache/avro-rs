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
use proc_macro2::{Span, TokenStream};
use quote::quote;
use serde_json::Value;
use std::cmp::PartialEq;
use syn::{AttrStyle, Attribute, Expr, Field, Ident, Path, Type, spanned::Spanned};

mod avro;
mod serde;

/// What `Schema` representation to generate for a type.
#[derive(Debug, PartialEq)]
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
    RecordTagContent { tag: String, content: String },
    /// Generate a `Schema::Record` with a tag field and flattened variant fields for a `enum`.
    ///
    /// Requires `#[serde(tag = "..")]`
    RecordInternallyTagged { tag: String },
}

impl Repr {
    fn from_avro_and_serde(
        avro: Option<avro::Repr>,
        tag: Option<String>,
        content: Option<String>,
        untagged: bool,
        span: Span,
    ) -> Result<Option<Self>, syn::Error> {
        match avro {
            Some(avro::Repr::Enum) => {
                if tag.is_some() || content.is_some() || untagged {
                    Err(syn::Error::new(
                        span,
                        r#"AvroSchema: `#[avro(repr = "enum")]` is incompatible with `#[serde(tag = "..")]`, `#[serde(content = "..")]`, and `#[serde(untagged)]`"#,
                    ))
                } else {
                    Ok(Some(Self::Enum))
                }
            }
            Some(avro::Repr::BareUnion) => {
                if tag.is_some() || content.is_some() {
                    Err(syn::Error::new(
                        span,
                        r#"AvroSchema: `#[avro(repr = "bare_union")]` is incompatible with `#[serde(tag = "..")]` and `#[serde(content = "..")]`"#,
                    ))
                } else {
                    Ok(Some(Self::BareUnion))
                }
            }
            Some(avro::Repr::UnionOfRecords) => {
                if tag.is_some() || content.is_some() || untagged {
                    Err(syn::Error::new(
                        span,
                        r#"AvroSchema: `#[avro(repr = "union_of_records")]` is incompatible with `#[serde(tag = "..")]`, `#[serde(content = "..")]`, and `#[serde(untagged)]`"#,
                    ))
                } else {
                    Ok(Some(Self::UnionOfRecords))
                }
            }
            Some(avro::Repr::RecordTagContent) => {
                if let Some(tag) = tag
                    && let Some(content) = content
                    && !untagged
                {
                    Ok(Some(Self::RecordTagContent { tag, content }))
                } else {
                    Err(syn::Error::new(
                        span,
                        r#"AvroSchema: `#[avro(repr = "record_tag_content")]` requires `#[serde(tag = "..", content = "..")]` and is incompatible with `#[serde(untagged)]`"#,
                    ))
                }
            }
            Some(avro::Repr::RecordInternallyTagged) => {
                if let Some(tag) = tag
                    && content.is_none()
                    && !untagged
                {
                    Ok(Some(Self::RecordInternallyTagged { tag }))
                } else {
                    Err(syn::Error::new(
                        span,
                        r#"AvroSchema: `#[avro(repr = "record_internally_tagged")]` requires `#[serde(tag = "..")]` and is incompatible with `#[serde(content = "..")]` and `#[serde(untagged)]`"#,
                    ))
                }
            }
            None if tag.is_some() && content.is_some() => Ok(Some(Self::RecordTagContent {
                tag: tag.unwrap(),
                content: content.unwrap(),
            })),
            None if tag.is_some() => Ok(Some(Self::RecordInternallyTagged { tag: tag.unwrap() })),
            None if untagged => Ok(Some(Self::BareUnion)),
            None => Ok(None),
        }
    }
}

pub struct NamedTypeOptions {
    pub name: String,
    pub doc: Option<String>,
    pub aliases: Vec<String>,
    pub rename_all: RenameRule,
    pub rename_all_fields: RenameRule,
    pub transparent: bool,
    pub default: TokenStream,
    pub repr: Option<Repr>,
    pub with: Option<Path>,
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
        if serde.variant_identifier || serde.field_identifier {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: `#[serde(variant_identifier)]` and `#[serde(field_identifier)]` are not supported"#,
            ));
        }
        if serde.rename_all.deserialize != serde.rename_all.serialize {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: rename rules for serializing and deserializing must match (`rename_all(serialize = "..", deserialize = "..")`)"#
            ));
        }
        if serde.rename_all_fields.deserialize != serde.rename_all_fields.serialize {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: rename rules for serializing and deserializing must match (`rename_all_fields(serialize = "..", deserialize = "..")`)"#
            ));
        }
        if serde.from != serde.into && serde.try_from != serde.into {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: `#[serde({try_,}from = "..")]` must match `#[serde(into = "..")]`"#,
            ));
        }

        // Check for conflicts between Serde and Avro
        if avro.name.is_some() && avro.name != serde.rename {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: #[avro(name = "..")] must match #[serde(rename = "..")] and it's deprecated. Please use only `#[serde(rename = "..")]`"#,
            ));
        }
        if avro.rename_all != RenameRule::None && serde.rename_all.serialize != avro.rename_all {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: #[avro(rename_all = "..")] must match #[serde(rename_all = "..")] and it's deprecated. Please use only `#[serde(rename_all = "..")]`"#,
            ));
        }
        if serde.transparent
            && (serde.rename.is_some()
                || avro.name.is_some()
                || avro.namespace.is_some()
                || avro.doc.is_some()
                || !avro.alias.is_empty()
                || avro.rename_all != RenameRule::None
                || avro.repr.is_some()
                || serde.rename_all.serialize != RenameRule::None
                || serde.rename_all.deserialize != RenameRule::None
                || serde.rename_all_fields.serialize != RenameRule::None
                || serde.rename_all_fields.deserialize != RenameRule::None
                || serde.untagged
                || serde.tag.is_some()
                || serde.content.is_some()
                || serde.into.is_some()
                || serde.from.is_some()
                || serde.try_from.is_some())
        {
            errors.push(syn::Error::new(
                span,
                "AvroSchema: `#[serde(transparent)]` is incompatible with all other attributes",
            ));
        }
        if serde.into.is_some()
            && (serde.rename.is_some()
                || avro.name.is_some()
                || avro.namespace.is_some()
                || avro.doc.is_some()
                || !avro.alias.is_empty()
                || avro.rename_all != RenameRule::None
                || avro.repr.is_some()
                || serde.rename_all.serialize != RenameRule::None
                || serde.rename_all.deserialize != RenameRule::None
                || serde.rename_all_fields.serialize != RenameRule::None
                || serde.rename_all_fields.deserialize != RenameRule::None
                || serde.untagged
                || serde.tag.is_some()
                || serde.content.is_some())
        {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: `#[serde({,try_}from = "..", into = "..")]` are incompatible with all other attributes"#,
            ));
        }
        let repr = match Repr::from_avro_and_serde(
            avro.repr,
            serde.tag,
            serde.content,
            serde.untagged,
            span,
        ) {
            Ok(repr) => repr,
            Err(err) => {
                errors.push(err);
                None
            }
        };

        let default = match avro.default {
            None => quote! { None },
            Some(default_value) => {
                if let Err(err) = serde_json::from_str::<serde_json::Value>(&default_value[..]) {
                    errors.push(syn::Error::new(
                        span,
                        format!("Invalid Avro `default` JSON: \n{err}"),
                    ));
                    quote! { ::std::option::Option::None }
                } else {
                    quote! {
                        ::std::option::Option::Some(::serde_json::from_str(#default_value).expect(format!("Invalid JSON: {:?}", #default_value).as_str()))
                    }
                }
            }
        };

        let with = match serde.into.as_deref().map(Path::from_string) {
            Some(Ok(path)) => Some(path),
            Some(Err(err)) => {
                errors.push(syn::Error::new(
                    span,
                    format!(r#"AvroSchema: Expected a path for `#[serde(into = "..")]`: {err:?}"#),
                ));
                None
            }
            None => None,
        };

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
            rename_all_fields: serde.rename_all_fields.serialize,
            transparent: serde.transparent,
            default,
            repr,
            with,
        })
    }
}

/// How to get the schema for this field or variant.
#[derive(Debug, PartialEq, Default, Clone)]
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
                            format!(
                                r#"AvroSchema: Expected a path for `#[serde(with = "..")]`: {err:?}"#
                            ),
                        )
                    })?;
                    Ok(Self::Serde(path))
                } else {
                    Err(syn::Error::new(
                        span,
                        r#"`#[avro(with)]` requires `#[serde(with = "some_module")]` or provide a function to call `#[avro(with = some_fn)]`"#,
                    ))
                }
            }
            avro::With::Expr(expr) => Ok(Self::Expr(expr.clone())),
        }
    }
}

pub struct VariantOptions {
    pub doc: Option<String>,
    pub rename: Option<String>,
    pub rename_all: RenameRule,
    pub aliases: Vec<String>,
    pub skip: bool,
    pub with: With,
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
                r#"AvroSchema: `#[serde(other)]` and `#[serde(untagged)]` are not supported"#,
            ));
        }
        if serde.rename_all.deserialize != serde.rename_all.serialize {
            errors.push(syn::Error::new(
                span,
                r#"AvroSchema: rename rules for serializing and deserializing must match (`rename_all(serialize = "..", deserialize = "..")`)"#
            ));
        }
        if serde.skip_serializing != serde.skip_deserializing {
            errors.push(syn::Error::new(
                span,
                "`#[serde(skip_serializing)]` must match `#[serde(skip_deserializing)]`",
            ));
        }

        // Check for conflicts between Serde and Avro
        if avro.rename.is_some() && serde.rename != avro.rename {
            errors.push(syn::Error::new(
                span,
                r#"`#[avro(rename = "..")]` must match `#[serde(rename = "..")]`, it's also deprecated. Please use only `#[serde(rename = "..")]`"#
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
            rename: serde.rename,
            rename_all: serde.rename_all.serialize,
            aliases: serde.alias,
            doc,
            skip: serde.skip || serde.skip_serializing,
            with,
        })
    }
}

/// How to get the default value for a value.
#[derive(PartialEq)]
pub enum FieldDefault {
    /// Use `<T as AvroSchemaComponent>::field_default`.
    Trait,
    /// Don't set a default.
    Disabled,
    /// Use `module::field_default` where the module is defined by Serde's `with` attribute.
    Serde(Path),
    /// Use this JSON value.
    Value(Value),
    /// Call the function in this expression.
    Expr(Expr),
}

impl FieldDefault {
    fn from_avro_and_serde(
        avro: avro::FieldDefault,
        with: &With,
        span: Span,
    ) -> Result<Self, syn::Error> {
        match avro {
            // Only get the default from the type if the schema is also retrieved from the type
            avro::FieldDefault::Trait if with == &With::Trait => Ok(Self::Trait),
            avro::FieldDefault::Trait | avro::FieldDefault::Disabled => Ok(Self::Disabled),
            avro::FieldDefault::Serde => {
                if let With::Serde(path) = with {
                    Ok(Self::Serde(path.clone()))
                } else {
                    Err(syn::Error::new(
                        span,
                        r##"`#[avro(default)]` requires `#[serde(with = "some_module")]` or provide a function to call `#[avro(default = some_fn)]` or a JSON value `#[avro(default = r#""string""#)]`"##,
                    ))
                }
            }
            avro::FieldDefault::Value(value) => Ok(Self::Value(value)),
            avro::FieldDefault::Expr(expr) => Ok(Self::Expr(expr)),
        }
    }

    pub fn to_expr(&self, field: &Field) -> Result<TokenStream, syn::Error> {
        match self {
            FieldDefault::Trait => {
                let ty = &field.ty;
                match &field.ty {
                    Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
                        Ok(quote! {<#ty as :: apache_avro::AvroSchemaComponent>::field_default()})
                    }
                    Type::Ptr(_) => Err(syn::Error::new(
                        ty.span(),
                        "AvroSchema: Raw pointers are not supported",
                    )),
                    _ => Err(syn::Error::new(
                        ty.span(),
                        format!("AvroSchema: Unexpected type encountered: {ty:?}"),
                    )),
                }
            }
            FieldDefault::Disabled => Ok(quote! { ::std::option::Option::None }),
            FieldDefault::Serde(path) => Ok(quote! { #path::field_default() }),
            FieldDefault::Value(value) => {
                let value = serde_json::to_string(&value)
                    .expect("serde_json::Value will always successfully serialize");
                Ok(quote! {
                    ::std::option::Option::Some(::serde_json::from_str(#value).expect("Unreachable! This is checked at compile time!"))
                })
            }
            FieldDefault::Expr(Expr::Closure(closure)) => {
                if closure.inputs.is_empty() {
                    Ok(quote! { (#closure)() })
                } else {
                    Err(syn::Error::new(
                        field.span(),
                        "AvroSchema: Expected closure with 0 parameters for `#[avro(default = ..)]`",
                    ))
                }
            }
            FieldDefault::Expr(Expr::Path(path)) => {
                Ok(quote! { #path(named_schemas, enclosing_namespace) })
            }
            FieldDefault::Expr(_expr) => Err(syn::Error::new(
                field.span(),
                "AvroSchema: Invalid expression, expected function or closure for `#[avro(default = ..)]`",
            )),
        }
    }
}

pub struct FieldOptions {
    pub doc: Option<String>,
    pub default: FieldDefault,
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
                r#"`#[avro(rename = "..")]` must match `#[serde(rename = "..")]`, it's also deprecated. Please use only `#[serde(rename = "..")]`"#
            ));
        }
        if !avro.alias.is_empty() && serde.alias != avro.alias {
            errors.push(syn::Error::new(
                span,
                r#"`#[avro(alias = "..")]` must match `#[serde(alias = "..")]`, it's also deprecated. Please use only `#[serde(alias = "..")]`"#
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
        let default = match FieldDefault::from_avro_and_serde(avro.default, &with, span) {
            Ok(with) => with,
            Err(error) => {
                errors.push(error);
                // This won't actually be used, but it does simplify the code
                FieldDefault::Disabled
            }
        };
        if ((serde.skip_serializing && !serde.skip_deserializing)
            || serde.skip_serializing_if.is_some())
            && default == FieldDefault::Disabled
        {
            errors.push(syn::Error::new(
                span,
                "`#[serde(skip_serializing)]` and `#[serde(skip_serializing_if)]` are incompatible with `#[avro(default = false)]` and `#[avro(with)]` without `#[avro(default)]`"
            ));
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        let doc = avro.doc.or_else(|| extract_rustdoc(attributes));

        Ok(Self {
            doc,
            default,
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
