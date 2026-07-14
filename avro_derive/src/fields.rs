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

use crate::attributes::{FieldDefault, FieldOptions, With};
use crate::case::RenameRule;
use crate::utils::{
    aliases, field_aliases, json_value_expr, name_expr, preserve_optional, rename_ident,
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Expr, Field, FieldsNamed, FieldsUnnamed, Type};

pub fn field_to_schema_expr(field: &Field, with: &With) -> Result<TokenStream, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_schema_expr(&field.ty)?),
        With::Serde(path) => {
            Ok(quote! { #path::get_schema_in_ctxt(named_schemas, enclosing_namespace) })
        }
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(quote! { (#closure)() })
            } else {
                Err(vec![syn::Error::new(
                    field.span(),
                    "Expected closure with 0 parameters",
                )])
            }
        }
        With::Expr(Expr::Path(path)) => Ok(quote! { #path(named_schemas, enclosing_namespace) }),
        With::Expr(_expr) => Err(vec![syn::Error::new(
            field.span(),
            "Invalid expression, expected a function or a closure",
        )]),
    }
}

pub fn field_to_record_fields_expr(
    field: &Field,
    with: &With,
) -> Result<TokenStream, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_record_fields_expr(&field.ty)?),
        With::Serde(path) => {
            Ok(quote! { #path::get_record_fields_in_ctxt(named_schemas, enclosing_namespace) })
        }
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(quote! {
                    ::apache_avro::serde::get_record_fields_in_ctxt(
                        named_schemas,
                        enclosing_namespace,
                        |_, _| (#closure)(),
                    )
                })
            } else {
                Err(vec![syn::Error::new(
                    field.span(),
                    "Expected closure with 0 parameters",
                )])
            }
        }
        With::Expr(Expr::Path(path)) => Ok(quote! {
            ::apache_avro::serde::get_record_fields_in_ctxt(named_schemas, enclosing_namespace, #path)
        }),
        With::Expr(_expr) => Err(vec![syn::Error::new(
            field.span(),
            "Invalid expression, expected a function or a closure",
        )]),
    }
}

pub fn field_to_field_default_expr(
    field: &Field,
    default: FieldDefault,
) -> Result<TokenStream, Vec<syn::Error>> {
    match default {
        FieldDefault::Disabled => Ok(quote! { ::std::option::Option::None }),
        FieldDefault::Trait => type_to_field_default_expr(&field.ty),
        FieldDefault::Value(default_value) => {
            let value = json_value_expr(default_value);
            Ok(quote! {
                ::std::option::Option::Some(#value)
            })
        }
    }
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)},
        ),
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        Type::Tuple(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support tuples",
        )]),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

fn type_to_record_fields_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)},
        ),
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        Type::Tuple(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support tuples",
        )]),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

fn type_to_field_default_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
            Ok(quote! {<#ty as :: apache_avro::AvroSchemaComponent>::field_default()})
        }
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        Type::Tuple(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support tuples",
        )]),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

pub fn named_fields_to_schema(
    name: &str,
    fields: FieldsNamed,
    rename_all: RenameRule,
    doc: Option<String>,
    the_aliases: &[String],
) -> Result<TokenStream, Vec<syn::Error>> {
    let record_fields_expr = named_fields_to_record_fields(fields, rename_all)?;

    let record_doc = preserve_optional(doc.map(|s| quote! { #s.to_string() }));
    let record_aliases = aliases(the_aliases);
    let name_expr = name_expr(name);

    Ok(quote! {
        ::apache_avro::schema::Schema::Record(
            ::apache_avro::schema::RecordSchema::builder()
                .aliases(#record_aliases)
                .maybe_doc(#record_doc)
                .fields(#record_fields_expr)
                .name(#name_expr)
                .build()
        )
    })
}

pub fn unnamed_fields_to_schema(
    name: &str,
    fields: FieldsUnnamed,
    doc: Option<String>,
    the_aliases: &[String],
) -> Result<TokenStream, Vec<syn::Error>> {
    let attributes_expr = if fields.unnamed.len() == 1 {
        quote! {
            [
                ("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true)),
                ("org.apache.avro.rust.union_of_records".to_string(), ::serde_json::value::Value::Bool(true)),
            ].into()
        }
    } else {
        quote! {
            [("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true))].into()
        }
    };
    let record_fields_expr = unnamed_fields_to_record_fields(fields)?;

    let record_doc = preserve_optional(doc.map(|s| quote! { #s.to_string() }));
    let record_aliases = aliases(the_aliases);
    let name_expr = name_expr(name);

    Ok(quote! {
        ::apache_avro::schema::Schema::Record(
            ::apache_avro::schema::RecordSchema::builder()
                .aliases(#record_aliases)
                .maybe_doc(#record_doc)
                .fields(#record_fields_expr)
                .attributes(#attributes_expr)
                .name(#name_expr)
                .build()
        )
    })
}

/// Returns an expression that resolves to a `Vec<RecordField>`.
pub fn named_fields_to_record_fields(
    fields: FieldsNamed,
    rename_all: RenameRule,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut errors = Vec::new();
    let mut field_exprs = Vec::with_capacity(fields.named.len());

    for field in fields.named {
        let field_attrs = match FieldOptions::new(&field.attrs, field.span()) {
            Ok(attrs) => attrs,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        if field_attrs.skip {
            continue;
        } else if field_attrs.flatten {
            // Inline the fields of the child record at runtime, as we don't have access to
            // the schema here.
            let get_record_fields = match field_to_record_fields_expr(&field, &field_attrs.with) {
                Ok(fields) => fields,
                Err(errs) => {
                    errors.extend(errs);
                    continue;
                }
            };
            field_exprs.push(quote! {
                if let Some(flattened_fields) = #get_record_fields {
                    fields.extend(flattened_fields);
                } else {
                    panic!(concat!(stringify!(#field), " does not have any fields to flatten to"));
                }
            });

            // Don't add this field as it's been replaced by the child record fields
            continue;
        }

        let default_value = match field_to_field_default_expr(&field, field_attrs.default) {
            Ok(default) => default,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        let name = rename_ident(
            field.ident.as_ref().unwrap(),
            field_attrs.rename,
            rename_all,
            RenameRule::apply_to_field,
        );

        let doc = preserve_optional(field_attrs.doc.map(|s| quote! { #s.to_string() }));
        let aliases = field_aliases(&field_attrs.alias);
        let schema_expr = match field_to_schema_expr(&field, &field_attrs.with) {
            Ok(expr) => expr,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };
        field_exprs.push(quote! {
            fields.push(::apache_avro::schema::RecordField {
                name: #name.to_string(),
                doc: #doc,
                default: #default_value,
                aliases: #aliases,
                schema: #schema_expr,
                custom_attributes: ::std::collections::BTreeMap::new(),
            });
        });
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    // When fields are flattened there might be more fields, but this is good for the regular case
    let minimum_length = field_exprs.len();
    Ok(quote! {
        {
            let mut fields = ::std::vec::Vec::with_capacity(#minimum_length);
            #(#field_exprs;)*
            fields
        }
    })
}

/// Returns an expression that resolves to a `Vec<RecordField>`.
pub fn unnamed_fields_to_record_fields(
    fields: FieldsUnnamed,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut errors = Vec::new();
    let mut field_exprs = Vec::with_capacity(fields.unnamed.len());

    let mut index = 0;
    for field in fields.unnamed {
        let field_attrs = match FieldOptions::new(&field.attrs, field.span()) {
            Ok(attrs) => attrs,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        if field_attrs.skip {
            continue;
        } else if field_attrs.flatten {
            errors.push(syn::Error::new(
                field.span(),
                "AvroSchema: `#[serde(flatten)]` is not supported on tuple fields",
            ));
            continue;
        }

        let default_value = match field_to_field_default_expr(&field, field_attrs.default) {
            Ok(default) => default,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };

        let name = if let Some(rename) = field_attrs.rename {
            rename
        } else {
            format!("field_{index}")
        };

        let doc = preserve_optional(field_attrs.doc.map(|s| quote! { #s.to_string() }));
        let aliases = field_aliases(&field_attrs.alias);
        let schema_expr = match field_to_schema_expr(&field, &field_attrs.with) {
            Ok(expr) => expr,
            Err(errs) => {
                errors.extend(errs);
                continue;
            }
        };
        field_exprs.push(quote! {
            ::apache_avro::schema::RecordField {
                name: #name.to_string(),
                doc: #doc,
                default: #default_value,
                aliases: #aliases,
                schema: #schema_expr,
                custom_attributes: ::std::collections::BTreeMap::new(),
            }
        });
        index += 1;
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(quote! {
        vec![#(#field_exprs,)*]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_trait_cast() {
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{i32}).unwrap()).unwrap().to_string(), quote!{<i32 as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{Vec<T>}).unwrap()).unwrap().to_string(), quote!{<Vec<T> as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{AnyType}).unwrap()).unwrap().to_string(), quote!{<AnyType as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
    }
}
