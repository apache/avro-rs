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

#![cfg_attr(nightly, feature(proc_macro_diagnostic))]

//! This crate is the implementation of the `AvroSchema` derive macro.
//! Please use it via the [`apache-avro`](https://crates.io/crates/apache-avro) crate:
//!
//! ```no_run
//! use apache_avro::AvroSchema;
//!
//! #[derive(AvroSchema)]
//! ```
//! Please see the documentation of the [`AvroSchema`] trait for instructions on how to use it.
//!
//! [`AvroSchema`]: https://docs.rs/apache-avro/latest/apache_avro/serde/trait.AvroSchema.html

mod attributes;
mod case;
mod enums;
mod tuple;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    DataStruct, DeriveInput, Expr, Field, Fields, FieldsNamed, Generics, Ident, Type,
    parse_macro_input, spanned::Spanned,
};

use crate::tuple::tuple_to_schema;
use crate::{
    attributes::{FieldOptions, NamedTypeOptions, With},
    case::RenameRule,
    tuple::unnamed_to_record_fields,
};

#[proc_macro_derive(AvroSchema, attributes(avro, serde))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    // It would be nice to parse the attributes before the `match`, but we first need to validate that `input` is not a union.
    // Otherwise a user could get errors related to the attributes and after fixing those get an error because the attributes were on a union.
    let input_span = input.span();
    let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
    if let Some(path) = named_type_options.with {
        // `#[serde(into = "..", {try_,}from = ".."]` was specified. This means Serde will use the `Serialize`/`Deserialize`
        // implementation of that type, so we also use the `AvroSchema` of that type.
        Ok(create_trait_definition(
            input.ident,
            &input.generics,
            quote! { <#path as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace) },
            quote! { <#path as ::apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(named_schemas, enclosing_namespace) },
            quote! { <#path as ::apache_avro::AvroSchemaComponent>::field_default() },
        ))
    } else {
        match input.data {
            syn::Data::Struct(data_struct) => {
                if named_type_options.repr.is_some() {
                    return Err(vec![syn::Error::new(
                        input_span,
                        r#"AvroSchema: `#[avro(repr = "..")]`, `#[serde(tag = "..")]`, `#[serde(content = "..")]`, and `#[serde(untagged)]` are only supported on enums"#,
                    )]);
                }
                let (get_schema_impl, get_record_fields_impl) = if named_type_options.transparent {
                    get_transparent_struct_schema_def(data_struct.fields, input_span)?
                } else {
                    let (schema_def, record_fields) =
                        get_struct_schema_def(&named_type_options, data_struct)?;
                    (
                        handle_named_schemas(named_type_options.name, schema_def),
                        record_fields,
                    )
                };
                Ok(create_trait_definition(
                    input.ident,
                    &input.generics,
                    get_schema_impl,
                    get_record_fields_impl,
                    named_type_options.default,
                ))
            }
            syn::Data::Enum(data_enum) => {
                if named_type_options.transparent {
                    return Err(vec![syn::Error::new(
                        input_span,
                        "AvroSchema: `#[serde(transparent)]` is only supported on structs",
                    )]);
                }
                let schema_def = enums::get_data_enum_schema_def(
                    &named_type_options,
                    data_enum,
                    input.ident.span(),
                )?;
                let inner = handle_named_schemas(named_type_options.name, schema_def);
                Ok(create_trait_definition(
                    input.ident,
                    &input.generics,
                    inner,
                    quote! { None },
                    named_type_options.default,
                ))
            }
            syn::Data::Union(_) => Err(vec![syn::Error::new(
                input_span,
                "AvroSchema: derive only works for structs and enums",
            )]),
        }
    }
}

/// Generate the trait definition with the correct generics
fn create_trait_definition(
    ident: Ident,
    generics: &Generics,
    get_schema_impl: TokenStream,
    get_record_fields_impl: TokenStream,
    field_default_impl: TokenStream,
) -> TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::apache_avro::AvroSchemaComponent for #ident #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::apache_avro::schema::Schema {
                #get_schema_impl
            }

            fn get_record_fields_in_ctxt(named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>, enclosing_namespace: ::apache_avro::schema::NamespaceRef) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
                #get_record_fields_impl
            }

            fn field_default() -> ::std::option::Option<::serde_json::Value> {
                #field_default_impl
            }
        }
    }
}

/// Generate the code to check `named_schemas` if this schema already exist
fn handle_named_schemas(full_schema_name: String, schema_def: TokenStream) -> TokenStream {
    quote! {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(#full_schema_name, enclosing_namespace).expect(concat!("Unable to parse schema name ", #full_schema_name));
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref{name}
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            #schema_def
        }
    }
}

/// Generate a schema definition for a struct.
fn get_struct_schema_def(
    container_attrs: &NamedTypeOptions,
    data_struct: DataStruct,
) -> Result<(TokenStream, TokenStream), Vec<syn::Error>> {
    let record_fields = match data_struct.fields {
        Fields::Named(a) => named_to_record_fields(a, container_attrs.rename_all)?,
        Fields::Unnamed(unnamed) => unnamed_to_record_fields(unnamed)?,
        Fields::Unit => quote! { std::vec::Vec::<::apache_avro::schema::RecordField>::new() },
    };

    let record_doc = preserve_optional(container_attrs.doc.as_ref());
    let record_aliases = aliases(&container_attrs.aliases);
    let full_schema_name = &container_attrs.name;

    let schema_def = quote! {
        {
            let schema_fields = #record_fields;
            let schema_field_set: ::std::collections::HashSet<_> = schema_fields.iter().map(|rf| &rf.name).collect();
            assert_eq!(schema_fields.len(), schema_field_set.len(), "Duplicate field names found: {schema_fields:?}");
            let name = ::apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse struct name for schema {}", #full_schema_name)[..]);
            let lookup: ::std::collections::BTreeMap<String, usize> = schema_fields
                .iter()
                .enumerate()
                .map(|(position, field)| (field.name.to_owned(), position))
                .collect();
            ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema {
                name,
                aliases: #record_aliases,
                doc: #record_doc,
                fields: schema_fields,
                lookup,
                attributes: ::std::collections::BTreeMap::new(),
            })
        }
    };
    let record_fields = quote! {
        ::std::option::Option::Some(#record_fields)
    };

    Ok((schema_def, record_fields))
}

/// Use the schema definition of the only field in the struct as the schema
fn get_transparent_struct_schema_def(
    fields: Fields,
    input_span: Span,
) -> Result<(TokenStream, TokenStream), Vec<syn::Error>> {
    match fields {
        Fields::Named(fields_named) => {
            let mut found = None;
            for field in fields_named.named {
                let attrs = FieldOptions::new(&field.attrs, field.span())?;
                if attrs.skip {
                    continue;
                }
                if found.replace((field, attrs)).is_some() {
                    return Err(vec![syn::Error::new(
                        input_span,
                        "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                    )]);
                }
            }

            if let Some((field, attrs)) = found {
                Ok((
                    get_field_schema_expr(&field, attrs.with.clone())?,
                    get_field_get_record_fields_expr(&field, attrs.with)?,
                ))
            } else {
                Err(vec![syn::Error::new(
                    input_span,
                    "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                )])
            }
        }
        Fields::Unnamed(unnamed) => {
            let mut found = None;
            for field in unnamed.unnamed {
                let attrs = FieldOptions::new(&field.attrs, field.span())?;
                if attrs.skip {
                    continue;
                }
                if found.replace((field, attrs)).is_some() {
                    return Err(vec![syn::Error::new(
                        input_span,
                        "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                    )]);
                }
            }

            if let Some((field, attrs)) = found {
                Ok((
                    get_field_schema_expr(&field, attrs.with.clone())?,
                    get_field_get_record_fields_expr(&field, attrs.with)?,
                ))
            } else {
                Err(vec![syn::Error::new(
                    input_span,
                    "AvroSchema: #[serde(transparent)] is only allowed on structs with one unskipped field",
                )])
            }
        }
        Fields::Unit => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: `#[serde(transparent)` does not work for unit structs",
        )]),
    }
}

fn get_field_schema_expr(field: &Field, with: With) -> Result<TokenStream, Vec<syn::Error>> {
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
            "Invalid expression, expected function or closure",
        )]),
    }
}

/// Call `get_record_fields_in_ctxt` for this field.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// A call to a `get_record_fields_in_ctxt(named_schemas, enclosing_namespace) -> Option<Vec<RecordField>>`
fn get_field_get_record_fields_expr(
    field: &Field,
    with: With,
) -> Result<TokenStream, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_get_record_fields_expr(&field.ty)?),
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
            "Invalid expression, expected function or closure",
        )]),
    }
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// A call to a `get_schema_in_ctxt(named_schemas, enclosing_namespace) -> Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)},
        ),
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        Type::Tuple(tuple) => tuple_to_schema(tuple),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

fn type_to_get_record_fields_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
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

/// Create a vector of `RecordField`s.
fn named_to_record_fields(
    named: FieldsNamed,
    rename_all: RenameRule,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut fields = Vec::with_capacity(named.named.len());
    for field in named.named {
        let field_attrs = FieldOptions::new(&field.attrs, field.span())?;
        if field_attrs.skip {
            continue;
        } else if field_attrs.flatten {
            // Inline the fields of the child record at runtime, as we don't have access to
            // the schema here.
            let get_record_fields = get_field_get_record_fields_expr(&field, field_attrs.with)?;
            fields.push(quote! {
                if let Some(flattened_fields) = #get_record_fields {
                    fields.extend(flattened_fields);
                } else {
                    panic!("{} does not have any fields to flatten to", stringify!(#field));
                }
            });

            // Don't add this field as it's been replaced by the child record fields
            continue;
        }
        let mut name = field
            .ident
            .as_ref()
            .expect("Field must have a name")
            .to_string();
        if let Some(raw_name) = name.strip_prefix("r#") {
            name = raw_name.to_string();
        }
        match (field_attrs.rename, rename_all) {
            (Some(rename), _) => {
                name = rename;
            }
            (None, rename_all) if rename_all != RenameRule::None => {
                name = rename_all.apply_to_field(&name);
            }
            _ => {}
        }
        let default_value = field_attrs
            .default
            .into_tokenstream(field.ident.span(), &field.ty)?;
        let aliases = field_aliases(&field_attrs.alias);
        let doc = doc_into_tokenstream(field_attrs.doc);
        let field_schema_expr = get_field_schema_expr(&field, field_attrs.with)?;
        fields.push(quote! {
            fields.push(::apache_avro::schema::RecordField::builder()
                .name(#name.to_string())
                .doc(#doc)
                .maybe_default(#default_value)
                .aliases(#aliases)
                .schema(#field_schema_expr)
                .build());
        });
    }

    // When fields are flattened there might be more fields, but this is good for the regular case
    let minimum_length = fields.len();
    Ok(quote! {
        {
            let mut fields = ::std::vec::Vec::with_capacity(#minimum_length);
            #(
                #fields
            )*
            fields
        }
    })
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

fn preserve_optional(op: Option<impl quote::ToTokens>) -> TokenStream {
    match op {
        Some(tt) => quote! {::std::option::Option::Some(#tt.into())},
        None => quote! {::std::option::Option::None},
    }
}

fn doc_into_tokenstream(doc: Option<String>) -> TokenStream {
    match doc {
        Some(doc) => quote! {::std::option::Option::Some(#doc.to_string())},
        None => quote! {::std::option::Option::None},
    }
}

fn aliases(op: &[impl quote::ToTokens]) -> TokenStream {
    let items: Vec<TokenStream> = op
        .iter()
        .map(|tt| quote! {#tt.try_into().expect("Alias is invalid")})
        .collect();
    if items.is_empty() {
        quote! {::std::option::Option::None}
    } else {
        quote! {::std::option::Option::Some(vec![#(#items),*])}
    }
}

fn field_aliases(op: &[impl quote::ToTokens]) -> TokenStream {
    let items: Vec<TokenStream> = op
        .iter()
        .map(|tt| quote! {#tt.try_into().expect("Alias is invalid")})
        .collect();
    if items.is_empty() {
        quote! {::std::vec::Vec::new()}
    } else {
        quote! {vec![#(#items),*]}
    }
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
