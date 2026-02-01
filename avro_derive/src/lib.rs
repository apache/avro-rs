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

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Attribute, DataEnum, DataStruct, DeriveInput, Expr, Field, Fields, Generics, Ident, Meta, Type,
    parse_macro_input, spanned::Spanned,
};

use crate::{
    attributes::{FieldOptions, NamedTypeOptions, VariantOptions, With},
    case::RenameRule,
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
    match input.data {
        syn::Data::Struct(data_struct) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            let (get_schema_impl, get_record_fields_impl) = if named_type_options.transparent {
                get_transparent_struct_schema_def(data_struct.fields, input_span)?
            } else {
                let (schema_def, record_fields) =
                    get_struct_schema_def(&named_type_options, data_struct, input.ident.span())?;
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
            ))
        }
        syn::Data::Enum(data_enum) => {
            let named_type_options = NamedTypeOptions::new(&input.ident, &input.attrs, input_span)?;
            if named_type_options.transparent {
                return Err(vec![syn::Error::new(
                    input_span,
                    "AvroSchema: `#[serde(transparent)]` is only supported on structs",
                )]);
            }
            let schema_def =
                get_data_enum_schema_def(&named_type_options, data_enum, input.ident.span())?;
            let inner = handle_named_schemas(named_type_options.name, schema_def);
            Ok(create_trait_definition(
                input.ident,
                &input.generics,
                inner,
                quote! { None },
            ))
        }
        syn::Data::Union(_) => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive only works for structs and simple enums",
        )]),
    }
}

/// Generate the trait definition with the correct generics
fn create_trait_definition(
    ident: Ident,
    generics: &Generics,
    get_schema_impl: TokenStream,
    get_record_fields_impl: TokenStream,
) -> TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[automatically_derived]
        impl #impl_generics ::apache_avro::AvroSchemaComponent for #ident #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut ::apache_avro::schema::Names, enclosing_namespace: &::std::option::Option<::std::string::String>) -> ::apache_avro::schema::Schema {
                #get_schema_impl
            }

            fn get_record_fields_in_ctxt(mut field_position: usize, named_schemas: &mut ::apache_avro::schema::Names, enclosing_namespace: &::std::option::Option<::std::string::String>) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
                #get_record_fields_impl
            }
        }
    }
}

/// Generate the code to check `named_schemas` if this schema already exist
fn handle_named_schemas(full_schema_name: String, schema_def: TokenStream) -> TokenStream {
    quote! {
        let name = apache_avro::schema::Name::new(#full_schema_name).expect(concat!("Unable to parse schema name ", #full_schema_name)).fully_qualified_name(enclosing_namespace);
        if named_schemas.contains_key(&name) {
            apache_avro::schema::Schema::Ref{name}
        } else {
            let enclosing_namespace = &name.namespace;
            // This is needed because otherwise recursive types will recurse forever and cause a stack overflow
            // TODO: Breaking change to AvroSchemaComponent, have named_schemas be a set instead
            named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
            let schema = #schema_def;
            named_schemas.insert(name, schema.clone());
            schema
        }
    }
}

/// Generate a schema definition for a struct.
fn get_struct_schema_def(
    container_attrs: &NamedTypeOptions,
    data_struct: DataStruct,
    ident_span: Span,
) -> Result<(TokenStream, TokenStream), Vec<syn::Error>> {
    let mut record_field_exprs = vec![];
    match data_struct.fields {
        Fields::Named(a) => {
            for field in a.named {
                let mut name = field
                    .ident
                    .as_ref()
                    .expect("Field must have a name")
                    .to_string();
                if let Some(raw_name) = name.strip_prefix("r#") {
                    name = raw_name.to_string();
                }
                let field_attrs = FieldOptions::new(&field.attrs, field.span())?;
                let doc = preserve_optional(field_attrs.doc);
                match (field_attrs.rename, container_attrs.rename_all) {
                    (Some(rename), _) => {
                        name = rename;
                    }
                    (None, rename_all) if rename_all != RenameRule::None => {
                        name = rename_all.apply_to_field(&name);
                    }
                    _ => {}
                }
                if field_attrs.skip {
                    continue;
                } else if field_attrs.flatten {
                    // Inline the fields of the child record at runtime, as we don't have access to
                    // the schema here.
                    let get_record_fields =
                        get_field_get_record_fields_expr(&field, field_attrs.with)?;
                    record_field_exprs.push(quote! {
                        if let Some(flattened_fields) = #get_record_fields {
                            field_position += flattened_fields.len();
                            schema_fields.extend(flattened_fields);
                        } else {
                            panic!("{} does not have any fields to flatten to", stringify!(#field));
                        }
                    });

                    // Don't add this field as it's been replaced by the child record fields
                    continue;
                }
                let default_value = match field_attrs.default {
                    Some(default_value) => {
                        let _: serde_json::Value = serde_json::from_str(&default_value[..])
                            .map_err(|e| {
                                vec![syn::Error::new(
                                    field.ident.span(),
                                    format!("Invalid avro default json: \n{e}"),
                                )]
                            })?;
                        quote! {
                            Some(serde_json::from_str(#default_value).expect(format!("Invalid JSON: {:?}", #default_value).as_str()))
                        }
                    }
                    None => quote! { None },
                };
                let aliases = aliases(&field_attrs.alias);
                let schema_expr = get_field_schema_expr(&field, field_attrs.with)?;
                record_field_exprs.push(quote! {
                    schema_fields.push(::apache_avro::schema::RecordField {
                        name: #name.to_string(),
                        doc: #doc,
                        default: #default_value,
                        aliases: #aliases,
                        schema: #schema_expr,
                        order: ::apache_avro::schema::RecordFieldOrder::Ascending,
                        position: field_position,
                        custom_attributes: Default::default(),
                    });
                    field_position += 1;
                });
            }
        }
        Fields::Unnamed(_) => {
            return Err(vec![syn::Error::new(
                ident_span,
                "AvroSchema derive does not work for tuple structs",
            )]);
        }
        Fields::Unit => {
            return Err(vec![syn::Error::new(
                ident_span,
                "AvroSchema derive does not work for unit structs",
            )]);
        }
    }

    let record_doc = preserve_optional(container_attrs.doc.as_ref());
    let record_aliases = aliases(&container_attrs.aliases);
    let full_schema_name = &container_attrs.name;

    // When flatten is involved, there will be more but we don't know how many. This optimises for
    // the most common case where there is no flatten.
    let minimum_fields = record_field_exprs.len();

    let schema_def = quote! {
        {
            let mut schema_fields = Vec::with_capacity(#minimum_fields);
            let mut field_position = 0;
            #(#record_field_exprs)*
            let schema_field_set: ::std::collections::HashSet<_> = schema_fields.iter().map(|rf| &rf.name).collect();
            assert_eq!(schema_fields.len(), schema_field_set.len(), "Duplicate field names found: {schema_fields:?}");
            let name = apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse struct name for schema {}", #full_schema_name)[..]);
            let lookup: std::collections::BTreeMap<String, usize> = schema_fields
                .iter()
                .map(|field| (field.name.to_owned(), field.position))
                .collect();
            apache_avro::schema::Schema::Record(apache_avro::schema::RecordSchema {
                name,
                aliases: #record_aliases,
                doc: #record_doc,
                fields: schema_fields,
                lookup,
                attributes: Default::default(),
            })
        }
    };
    let record_fields = quote! {
        let mut schema_fields = Vec::with_capacity(#minimum_fields);
        #(#record_field_exprs)*
        Some(schema_fields)
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
        Fields::Unnamed(_) => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for tuple structs",
        )]),
        Fields::Unit => Err(vec![syn::Error::new(
            input_span,
            "AvroSchema: derive does not work for unit structs",
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

fn get_field_get_record_fields_expr(
    field: &Field,
    with: With,
) -> Result<TokenStream, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_get_record_fields_expr(&field.ty)?),
        With::Serde(path) => Ok(
            quote! { #path::get_record_fields_in_ctxt(field_position, named_schemas, enclosing_namespace) },
        ),
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(quote! {
                    ::apache_avro::serde::get_record_fields_in_ctxt(
                        field_position,
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
            ::apache_avro::serde::get_record_fields_in_ctxt(field_position, named_schemas, enclosing_namespace, #path)
        }),
        With::Expr(_expr) => Err(vec![syn::Error::new(
            field.span(),
            "Invalid expression, expected function or closure",
        )]),
    }
}

/// Generate a schema definition for a enum.
fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    if data_enum.variants.iter().all(|v| Fields::Unit == v.fields) {
        let default_value = default_enum_variant(&data_enum, ident_span)?;
        let default = preserve_optional(default_value);
        let mut symbols = Vec::new();
        for variant in &data_enum.variants {
            let field_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
            let name = match (field_attrs.rename, container_attrs.rename_all) {
                (Some(rename), _) => rename,
                (None, rename_all) if !matches!(rename_all, RenameRule::None) => {
                    rename_all.apply_to_variant(&variant.ident.to_string())
                }
                _ => variant.ident.to_string(),
            };
            symbols.push(name);
        }
        let full_schema_name = &container_attrs.name;
        Ok(quote! {
            apache_avro::schema::Schema::Enum(apache_avro::schema::EnumSchema {
                name: apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse enum name for schema {}", #full_schema_name)[..]),
                aliases: #enum_aliases,
                doc: #doc,
                symbols: vec![#(#symbols.to_owned()),*],
                default: #default,
                attributes: Default::default(),
            })
        })
    } else {
        Err(vec![syn::Error::new(
            ident_span,
            "AvroSchema: derive does not work for enums with non unit structs",
        )])
    }
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)},
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

fn type_to_get_record_fields_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(field_position, named_schemas, enclosing_namespace)},
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

fn default_enum_variant(
    data_enum: &syn::DataEnum,
    error_span: Span,
) -> Result<Option<String>, Vec<syn::Error>> {
    match data_enum
        .variants
        .iter()
        .filter(|v| v.attrs.iter().any(is_default_attr))
        .collect::<Vec<_>>()
    {
        variants if variants.is_empty() => Ok(None),
        single if single.len() == 1 => Ok(Some(single[0].ident.to_string())),
        multiple => Err(vec![syn::Error::new(
            error_span,
            format!(
                "Multiple defaults defined: {:?}",
                multiple
                    .iter()
                    .map(|v| v.ident.to_string())
                    .collect::<Vec<String>>()
            ),
        )]),
    }
}

fn is_default_attr(attr: &Attribute) -> bool {
    matches!(attr, Attribute { meta: Meta::Path(path), .. } if path.get_ident().map(Ident::to_string).as_deref() == Some("default"))
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

fn preserve_optional(op: Option<impl quote::ToTokens>) -> TokenStream {
    match op {
        Some(tt) => quote! {Some(#tt.into())},
        None => quote! {None},
    }
}

fn aliases(op: &[impl quote::ToTokens]) -> TokenStream {
    let items: Vec<TokenStream> = op
        .iter()
        .map(|tt| quote! {#tt.try_into().expect("Alias is invalid")})
        .collect();
    if items.is_empty() {
        quote! {None}
    } else {
        quote! {Some(vec![#(#items),*])}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn basic_case() {
        let test_struct = quote! {
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn tuple_struct_unsupported() {
        let test_tuple_struct = quote! {
            struct B (i32, String);
        };

        match syn::parse2::<DeriveInput>(test_tuple_struct) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_err())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn unit_struct_unsupported() {
        let test_tuple_struct = quote! {
            struct AbsoluteUnit;
        };

        match syn::parse2::<DeriveInput>(test_tuple_struct) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_err())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn struct_with_optional() {
        let struct_with_optional = quote! {
            struct Test4 {
                a : Option<i32>
            }
        };
        match syn::parse2::<DeriveInput>(struct_with_optional) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_basic_enum() {
        let basic_enum = quote! {
            enum Basic {
                A,
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(basic_enum) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn avro_3687_basic_enum_with_default() {
        let basic_enum = quote! {
            enum Basic {
                #[default]
                A,
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(basic_enum) {
            Ok(input) => {
                let derived = derive_avro_schema(input);
                assert!(derived.is_ok());
                assert_eq!(derived.unwrap().to_string(), quote! {
                    #[automatically_derived]
                    impl ::apache_avro::AvroSchemaComponent for Basic {
                        fn get_schema_in_ctxt(
                            named_schemas: &mut ::apache_avro::schema::Names,
                            enclosing_namespace: &::std::option::Option<::std::string::String>
                        ) -> ::apache_avro::schema::Schema {
                            let name = apache_avro::schema::Name::new("Basic")
                                .expect(concat!("Unable to parse schema name ", "Basic"))
                                .fully_qualified_name(enclosing_namespace);
                            if named_schemas.contains_key(&name) {
                                apache_avro::schema::Schema::Ref { name }
                            } else {
                                let enclosing_namespace = &name.namespace;
                                named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
                                let schema =
                                apache_avro::schema::Schema::Enum(apache_avro::schema::EnumSchema {
                                    name: apache_avro::schema::Name::new("Basic").expect(
                                        &format!("Unable to parse enum name for schema {}", "Basic")[..]
                                    ),
                                    aliases: None,
                                    doc: None,
                                    symbols: vec![
                                        "A".to_owned(),
                                        "B".to_owned(),
                                        "C".to_owned(),
                                        "D".to_owned()
                                    ],
                                    default: Some("A".into()),
                                    attributes: Default::default(),
                                });
                                named_schemas.insert(name, schema.clone());
                                schema
                            }
                        }

                        fn get_record_fields_in_ctxt(
                            mut field_position: usize,
                            named_schemas: &mut ::apache_avro::schema::Names,
                            enclosing_namespace: &::std::option::Option<::std::string::String>
                        ) -> ::std::option::Option <::std::vec::Vec<::apache_avro::schema::RecordField>> {
                            None
                        }
                    }
                }.to_string());
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn avro_3687_basic_enum_with_default_twice() {
        let non_basic_enum = quote! {
            enum Basic {
                #[default]
                A,
                B,
                #[default]
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(non_basic_enum) {
            Ok(input) => match derive_avro_schema(input) {
                Ok(_) => {
                    panic!("Should not be able to derive schema for enum with multiple defaults")
                }
                Err(errors) => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors[0].to_string(),
                        r#"Multiple defaults defined: ["A", "C"]"#
                    );
                }
            },
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_non_basic_enum() {
        let non_basic_enum = quote! {
            enum Basic {
                A(i32),
                B,
                C,
                D
            }
        };
        match syn::parse2::<DeriveInput>(non_basic_enum) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_err())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_namespace() {
        let test_struct = quote! {
            #[avro(namespace = "namespace.testing")]
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                let schema_token_stream = derive_avro_schema(input);
                assert!(&schema_token_stream.is_ok());
                assert!(
                    schema_token_stream
                        .unwrap()
                        .to_string()
                        .contains("namespace.testing")
                )
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_reference() {
        let test_reference_struct = quote! {
            struct A<'a> {
                a: &'a Vec<i32>,
                b: &'static str
            }
        };

        match syn::parse2::<DeriveInput>(test_reference_struct) {
            Ok(input) => {
                assert!(derive_avro_schema(input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_trait_cast() {
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{i32}).unwrap()).unwrap().to_string(), quote!{<i32 as apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{Vec<T>}).unwrap()).unwrap().to_string(), quote!{<Vec<T> as apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_to_schema_expr(&syn::parse2::<Type>(quote!{AnyType}).unwrap()).unwrap().to_string(), quote!{<AnyType as apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
    }

    #[test]
    fn test_avro_3709_record_field_attributes() {
        let test_struct = quote! {
            struct A {
                #[serde(alias = "a1", alias = "a2", rename = "a3")]
                #[avro(doc = "a doc", default = "123")]
                a: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                let schema_res = derive_avro_schema(input);
                let expected_token_stream = r#"# [automatically_derived] impl :: apache_avro :: AvroSchemaComponent for A { fn get_schema_in_ctxt (named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: apache_avro :: schema :: Schema { let name = apache_avro :: schema :: Name :: new ("A") . expect (concat ! ("Unable to parse schema name " , "A")) . fully_qualified_name (enclosing_namespace) ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name } } else { let enclosing_namespace = & name . namespace ; named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema = { let mut schema_fields = Vec :: with_capacity (1usize) ; let mut field_position = 0 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "a3" . to_string () , doc : Some ("a doc" . into ()) , default : Some (serde_json :: from_str ("123") . expect (format ! ("Invalid JSON: {:?}" , "123") . as_str ())) , aliases : Some (vec ! ["a1" . try_into () . expect ("Alias is invalid") , "a2" . try_into () . expect ("Alias is invalid")]) , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; let schema_field_set : :: std :: collections :: HashSet < _ > = schema_fields . iter () . map (| rf | & rf . name) . collect () ; assert_eq ! (schema_fields . len () , schema_field_set . len () , "Duplicate field names found: {schema_fields:?}") ; let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse struct name for schema {}" , "A") [..]) ; let lookup : std :: collections :: BTreeMap < String , usize > = schema_fields . iter () . map (| field | (field . name . to_owned () , field . position)) . collect () ; apache_avro :: schema :: Schema :: Record (apache_avro :: schema :: RecordSchema { name , aliases : None , doc : None , fields : schema_fields , lookup , attributes : Default :: default () , }) } ; named_schemas . insert (name , schema . clone ()) ; schema } } fn get_record_fields_in_ctxt (mut field_position : usize , named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: std :: option :: Option < :: std :: vec :: Vec < :: apache_avro :: schema :: RecordField >> { let mut schema_fields = Vec :: with_capacity (1usize) ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "a3" . to_string () , doc : Some ("a doc" . into ()) , default : Some (serde_json :: from_str ("123") . expect (format ! ("Invalid JSON: {:?}" , "123") . as_str ())) , aliases : Some (vec ! ["a1" . try_into () . expect ("Alias is invalid") , "a2" . try_into () . expect ("Alias is invalid")]) , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; Some (schema_fields) } }"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert_eq!(schema_token_stream, expected_token_stream);
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };

        let test_enum = quote! {
            enum A {
                #[serde(rename = "A3")]
                Item1,
            }
        };

        match syn::parse2::<DeriveInput>(test_enum) {
            Ok(input) => {
                let schema_res = derive_avro_schema(input);
                let expected_token_stream = r#"# [automatically_derived] impl :: apache_avro :: AvroSchemaComponent for A { fn get_schema_in_ctxt (named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: apache_avro :: schema :: Schema { let name = apache_avro :: schema :: Name :: new ("A") . expect (concat ! ("Unable to parse schema name " , "A")) . fully_qualified_name (enclosing_namespace) ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name } } else { let enclosing_namespace = & name . namespace ; named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema = apache_avro :: schema :: Schema :: Enum (apache_avro :: schema :: EnumSchema { name : apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse enum name for schema {}" , "A") [..]) , aliases : None , doc : None , symbols : vec ! ["A3" . to_owned ()] , default : None , attributes : Default :: default () , }) ; named_schemas . insert (name , schema . clone ()) ; schema } } fn get_record_fields_in_ctxt (mut field_position : usize , named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: std :: option :: Option < :: std :: vec :: Vec < :: apache_avro :: schema :: RecordField >> { None } }"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert_eq!(schema_token_stream, expected_token_stream);
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_avro_rs_207_rename_all_attribute() {
        let test_struct = quote! {
            #[serde(rename_all="SCREAMING_SNAKE_CASE")]
            struct A {
                item: i32,
                double_item: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                let schema_res = derive_avro_schema(input);
                let expected_token_stream = r#"# [automatically_derived] impl :: apache_avro :: AvroSchemaComponent for A { fn get_schema_in_ctxt (named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: apache_avro :: schema :: Schema { let name = apache_avro :: schema :: Name :: new ("A") . expect (concat ! ("Unable to parse schema name " , "A")) . fully_qualified_name (enclosing_namespace) ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name } } else { let enclosing_namespace = & name . namespace ; named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema = { let mut schema_fields = Vec :: with_capacity (2usize) ; let mut field_position = 0 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "DOUBLE_ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; let schema_field_set : :: std :: collections :: HashSet < _ > = schema_fields . iter () . map (| rf | & rf . name) . collect () ; assert_eq ! (schema_fields . len () , schema_field_set . len () , "Duplicate field names found: {schema_fields:?}") ; let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse struct name for schema {}" , "A") [..]) ; let lookup : std :: collections :: BTreeMap < String , usize > = schema_fields . iter () . map (| field | (field . name . to_owned () , field . position)) . collect () ; apache_avro :: schema :: Schema :: Record (apache_avro :: schema :: RecordSchema { name , aliases : None , doc : None , fields : schema_fields , lookup , attributes : Default :: default () , }) } ; named_schemas . insert (name , schema . clone ()) ; schema } } fn get_record_fields_in_ctxt (mut field_position : usize , named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: std :: option :: Option < :: std :: vec :: Vec < :: apache_avro :: schema :: RecordField >> { let mut schema_fields = Vec :: with_capacity (2usize) ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "DOUBLE_ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; Some (schema_fields) } }"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert_eq!(schema_token_stream, expected_token_stream);
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };

        let test_enum = quote! {
            #[serde(rename_all="SCREAMING_SNAKE_CASE")]
            enum B {
                Item,
                DoubleItem,
            }
        };

        match syn::parse2::<DeriveInput>(test_enum) {
            Ok(input) => {
                let schema_res = derive_avro_schema(input);
                let expected_token_stream = r#"# [automatically_derived] impl :: apache_avro :: AvroSchemaComponent for B { fn get_schema_in_ctxt (named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: apache_avro :: schema :: Schema { let name = apache_avro :: schema :: Name :: new ("B") . expect (concat ! ("Unable to parse schema name " , "B")) . fully_qualified_name (enclosing_namespace) ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name } } else { let enclosing_namespace = & name . namespace ; named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema = apache_avro :: schema :: Schema :: Enum (apache_avro :: schema :: EnumSchema { name : apache_avro :: schema :: Name :: new ("B") . expect (& format ! ("Unable to parse enum name for schema {}" , "B") [..]) , aliases : None , doc : None , symbols : vec ! ["ITEM" . to_owned () , "DOUBLE_ITEM" . to_owned ()] , default : None , attributes : Default :: default () , }) ; named_schemas . insert (name , schema . clone ()) ; schema } } fn get_record_fields_in_ctxt (mut field_position : usize , named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: std :: option :: Option < :: std :: vec :: Vec < :: apache_avro :: schema :: RecordField >> { None } }"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert_eq!(schema_token_stream, expected_token_stream);
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_avro_rs_207_rename_attr_has_priority_over_rename_all_attribute() {
        let test_struct = quote! {
            #[serde(rename_all="SCREAMING_SNAKE_CASE")]
            struct A {
                item: i32,
                #[serde(rename="DoubleItem")]
                double_item: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(input) => {
                let schema_res = derive_avro_schema(input);
                let expected_token_stream = r#"# [automatically_derived] impl :: apache_avro :: AvroSchemaComponent for A { fn get_schema_in_ctxt (named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: apache_avro :: schema :: Schema { let name = apache_avro :: schema :: Name :: new ("A") . expect (concat ! ("Unable to parse schema name " , "A")) . fully_qualified_name (enclosing_namespace) ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name } } else { let enclosing_namespace = & name . namespace ; named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema = { let mut schema_fields = Vec :: with_capacity (2usize) ; let mut field_position = 0 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "DoubleItem" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; let schema_field_set : :: std :: collections :: HashSet < _ > = schema_fields . iter () . map (| rf | & rf . name) . collect () ; assert_eq ! (schema_fields . len () , schema_field_set . len () , "Duplicate field names found: {schema_fields:?}") ; let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse struct name for schema {}" , "A") [..]) ; let lookup : std :: collections :: BTreeMap < String , usize > = schema_fields . iter () . map (| field | (field . name . to_owned () , field . position)) . collect () ; apache_avro :: schema :: Schema :: Record (apache_avro :: schema :: RecordSchema { name , aliases : None , doc : None , fields : schema_fields , lookup , attributes : Default :: default () , }) } ; named_schemas . insert (name , schema . clone ()) ; schema } } fn get_record_fields_in_ctxt (mut field_position : usize , named_schemas : & mut :: apache_avro :: schema :: Names , enclosing_namespace : & :: std :: option :: Option < :: std :: string :: String >) -> :: std :: option :: Option < :: std :: vec :: Vec < :: apache_avro :: schema :: RecordField >> { let mut schema_fields = Vec :: with_capacity (2usize) ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; schema_fields . push (:: apache_avro :: schema :: RecordField { name : "DoubleItem" . to_string () , doc : None , default : None , aliases : None , schema : < i32 as apache_avro :: AvroSchemaComponent > :: get_schema_in_ctxt (named_schemas , enclosing_namespace) , order : :: apache_avro :: schema :: RecordFieldOrder :: Ascending , position : field_position , custom_attributes : Default :: default () , }) ; field_position += 1 ; Some (schema_fields) } }"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert_eq!(schema_token_stream, expected_token_stream);
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }
}
