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

mod case;
use case::RenameRule;
use darling::FromAttributes;
use proc_macro2::{Span, TokenStream};
use quote::quote;

use syn::{
    AttrStyle, Attribute, DeriveInput, Ident, Meta, Type, TypePath, parse_macro_input,
    spanned::Spanned,
};

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
struct FieldOptions {
    #[darling(default)]
    doc: Option<String>,
    #[darling(default)]
    default: Option<String>,
    #[darling(multiple)]
    alias: Vec<String>,
    #[darling(default)]
    rename: Option<String>,
    #[darling(default)]
    skip: Option<bool>,
}

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
struct VariantOptions {
    #[darling(default)]
    rename: Option<String>,
}

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
struct NamedTypeOptions {
    #[darling(default)]
    namespace: Option<String>,
    #[darling(default)]
    doc: Option<String>,
    #[darling(multiple)]
    alias: Vec<String>,
    #[darling(default)]
    rename_all: Option<String>,
}

#[proc_macro_derive(AvroSchema, attributes(avro))]
// Templated from Serde
pub fn proc_macro_derive_avro_schema(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    derive_avro_schema(&mut input)
        .unwrap_or_else(to_compile_errors)
        .into()
}

fn derive_avro_schema(input: &mut DeriveInput) -> Result<TokenStream, Vec<syn::Error>> {
    let named_type_options =
        NamedTypeOptions::from_attributes(&input.attrs[..]).map_err(darling_to_syn)?;

    let rename_all = parse_case(named_type_options.rename_all.as_deref(), input.span())?;

    let full_schema_name = vec![named_type_options.namespace, Some(input.ident.to_string())]
        .into_iter()
        .flatten()
        .collect::<Vec<String>>()
        .join(".");
    let schema_def = match &input.data {
        syn::Data::Struct(s) => get_data_struct_schema_def(
            &full_schema_name,
            named_type_options
                .doc
                .or_else(|| extract_outer_doc(&input.attrs)),
            named_type_options.alias,
            rename_all,
            s,
            input.ident.span(),
        )?,
        syn::Data::Enum(e) => get_data_enum_schema_def(
            &full_schema_name,
            named_type_options
                .doc
                .or_else(|| extract_outer_doc(&input.attrs)),
            named_type_options.alias,
            rename_all,
            e,
            input.ident.span(),
        )?,
        _ => {
            return Err(vec![syn::Error::new(
                input.ident.span(),
                "AvroSchema derive only works for structs and simple enums ",
            )]);
        }
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics apache_avro::schema::derive::AvroSchemaComponent for #ident #ty_generics #where_clause {
            fn get_schema_in_ctxt(named_schemas: &mut std::collections::HashMap<apache_avro::schema::Name, apache_avro::schema::Schema>, enclosing_namespace: &Option<String>) -> apache_avro::schema::Schema {
                let name =  apache_avro::schema::Name::new(#full_schema_name).expect(&format!("Unable to parse schema name {}", #full_schema_name)[..]).fully_qualified_name(enclosing_namespace);
                let enclosing_namespace = &name.namespace;
                if named_schemas.contains_key(&name) {
                    apache_avro::schema::Schema::Ref{name: name.clone()}
                } else {
                    named_schemas.insert(name.clone(), apache_avro::schema::Schema::Ref{name: name.clone()});
                    #schema_def
                }
            }
        }
    })
}

fn get_data_struct_schema_def(
    full_schema_name: &str,
    record_doc: Option<String>,
    aliases: Vec<String>,
    rename_all: RenameRule,
    s: &syn::DataStruct,
    error_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut record_field_exprs = vec![];
    match s.fields {
        syn::Fields::Named(ref a) => {
            let mut index: usize = 0;
            for field in a.named.iter() {
                let mut name = field.ident.as_ref().unwrap().to_string(); // we know everything has a name
                if let Some(raw_name) = name.strip_prefix("r#") {
                    name = raw_name.to_string();
                }
                let field_attrs =
                    FieldOptions::from_attributes(&field.attrs[..]).map_err(darling_to_syn)?;
                let doc =
                    preserve_optional(field_attrs.doc.or_else(|| extract_outer_doc(&field.attrs)));
                match (field_attrs.rename, rename_all) {
                    (Some(rename), _) => {
                        name = rename;
                    }
                    (None, rename_all) if !matches!(rename_all, RenameRule::None) => {
                        name = rename_all.apply_to_field(&name);
                    }
                    _ => {}
                }
                if let Some(true) = field_attrs.skip {
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
                let aliases = preserve_vec(field_attrs.alias);
                let schema_expr = type_to_schema_expr(&field.ty)?;
                let position = index;
                record_field_exprs.push(quote! {
                    apache_avro::schema::RecordField {
                            name: #name.to_string(),
                            doc: #doc,
                            default: #default_value,
                            aliases: #aliases,
                            schema: #schema_expr,
                            order: apache_avro::schema::RecordFieldOrder::Ascending,
                            position: #position,
                            custom_attributes: Default::default(),
                        }
                });
                index += 1;
            }
        }
        syn::Fields::Unnamed(_) => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroSchema derive does not work for tuple structs",
            )]);
        }
        syn::Fields::Unit => {
            return Err(vec![syn::Error::new(
                error_span,
                "AvroSchema derive does not work for unit structs",
            )]);
        }
    }
    let record_doc = preserve_optional(record_doc);
    let record_aliases = preserve_vec(aliases);
    Ok(quote! {
        let schema_fields = vec![#(#record_field_exprs),*];
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
    })
}

fn get_data_enum_schema_def(
    full_schema_name: &str,
    doc: Option<String>,
    aliases: Vec<String>,
    rename_all: RenameRule,
    e: &syn::DataEnum,
    error_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(doc);
    let enum_aliases = preserve_vec(aliases);
    if e.variants.iter().all(|v| syn::Fields::Unit == v.fields) {
        let default_value = default_enum_variant(e, error_span)?;
        let default = preserve_optional(default_value);
        let mut symbols = Vec::new();
        for variant in &e.variants {
            let field_attrs =
                VariantOptions::from_attributes(&variant.attrs[..]).map_err(darling_to_syn)?;
            let name = match (field_attrs.rename, rename_all) {
                (Some(rename), _) => rename,
                (None, rename_all) if !matches!(rename_all, RenameRule::None) => {
                    rename_all.apply_to_variant(&variant.ident.to_string())
                }
                _ => variant.ident.to_string(),
            };
            symbols.push(name);
        }
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
            error_span,
            "AvroSchema derive does not work for enums with non unit structs",
        )])
    }
}

/// Takes in the Tokens of a type and returns the tokens of an expression with return type `Schema`
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    if let Type::Path(p) = ty {
        let type_string = p.path.segments.last().unwrap().ident.to_string();

        let schema = match &type_string[..] {
            "bool" => quote! {apache_avro::schema::Schema::Boolean},
            "i8" | "i16" | "i32" | "u8" | "u16" => quote! {apache_avro::schema::Schema::Int},
            "u32" | "i64" => quote! {apache_avro::schema::Schema::Long},
            "f32" => quote! {apache_avro::schema::Schema::Float},
            "f64" => quote! {apache_avro::schema::Schema::Double},
            "String" | "str" => quote! {apache_avro::schema::Schema::String},
            "char" => {
                return Err(vec![syn::Error::new_spanned(
                    ty,
                    "AvroSchema: Cannot guarantee successful deserialization of this type",
                )]);
            }
            "u64" => {
                return Err(vec![syn::Error::new_spanned(
                    ty,
                    "Cannot guarantee successful serialization of this type due to overflow concerns",
                )]);
            } // Can't guarantee serialization type
            _ => {
                // Fails when the type does not implement AvroSchemaComponent directly
                // TODO check and error report with something like https://docs.rs/quote/1.0.15/quote/macro.quote_spanned.html#example
                type_path_schema_expr(p)
            }
        };
        Ok(schema)
    } else if let Type::Array(ta) = ty {
        let inner_schema_expr = type_to_schema_expr(&ta.elem)?;
        Ok(quote! {apache_avro::schema::Schema::array(#inner_schema_expr)})
    } else if let Type::Reference(tr) = ty {
        type_to_schema_expr(&tr.elem)
    } else {
        Err(vec![syn::Error::new_spanned(
            ty,
            format!("Unable to generate schema for type: {ty:?}"),
        )])
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

/// Generates the schema def expression for fully qualified type paths using the associated function
/// - `A -> <A as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt()`
/// - `A<T> -> <A<T> as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt()`
fn type_path_schema_expr(p: &TypePath) -> TokenStream {
    quote! {<#p as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}
}

/// Stolen from serde
fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

fn extract_outer_doc(attributes: &[Attribute]) -> Option<String> {
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

fn preserve_optional(op: Option<impl quote::ToTokens>) -> TokenStream {
    match op {
        Some(tt) => quote! {Some(#tt.into())},
        None => quote! {None},
    }
}

fn preserve_vec(op: Vec<impl quote::ToTokens>) -> TokenStream {
    let items: Vec<TokenStream> = op.iter().map(|tt| quote! {#tt.into()}).collect();
    if items.is_empty() {
        quote! {None}
    } else {
        quote! {Some(vec![#(#items),*])}
    }
}

fn darling_to_syn(e: darling::Error) -> Vec<syn::Error> {
    let msg = format!("{e}");
    let token_errors = e.write_errors();
    vec![syn::Error::new(token_errors.span(), msg)]
}

fn parse_case(case: Option<&str>, span: Span) -> Result<RenameRule, Vec<syn::Error>> {
    match case {
        None => Ok(RenameRule::None),
        Some(case) => {
            Ok(RenameRule::from_str(case)
                .map_err(|e| vec![syn::Error::new(span, e.to_string())])?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn basic_case() {
        let test_struct = quote! {
            struct A {
                a: i32,
                b: String
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
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
            Ok(mut input) => {
                let derived = derive_avro_schema(&mut input);
                assert!(derived.is_ok());
                assert_eq!(derived.unwrap().to_string(), quote! {
                    #[automatically_derived]
                    impl apache_avro::schema::derive::AvroSchemaComponent for Basic {
                        fn get_schema_in_ctxt(
                            named_schemas: &mut std::collections::HashMap<
                                apache_avro::schema::Name,
                                apache_avro::schema::Schema
                            >,
                            enclosing_namespace: &Option<String>
                        ) -> apache_avro::schema::Schema {
                            let name = apache_avro::schema::Name::new("Basic")
                                .expect(&format!("Unable to parse schema name {}", "Basic")[..])
                                .fully_qualified_name(enclosing_namespace);
                            let enclosing_namespace = &name.namespace;
                            if named_schemas.contains_key(&name) {
                                apache_avro::schema::Schema::Ref { name: name.clone() }
                            } else {
                                named_schemas.insert(
                                    name.clone(),
                                    apache_avro::schema::Schema::Ref { name: name.clone() }
                                );
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
                                })
                            }
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
            Ok(mut input) => match derive_avro_schema(&mut input) {
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_err())
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
            Ok(mut input) => {
                let schema_token_stream = derive_avro_schema(&mut input);
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
            Ok(mut input) => {
                assert!(derive_avro_schema(&mut input).is_ok())
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_trait_cast() {
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{i32}).unwrap()).to_string(), quote!{<i32 as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{Vec<T>}).unwrap()).to_string(), quote!{<Vec<T> as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
        assert_eq!(type_path_schema_expr(&syn::parse2::<TypePath>(quote!{AnyType}).unwrap()).to_string(), quote!{<AnyType as apache_avro::schema::derive::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)}.to_string());
    }

    #[test]
    fn test_avro_3709_record_field_attributes() {
        let test_struct = quote! {
            struct A {
                #[avro(alias = "a1", alias = "a2", doc = "a doc", default = "123", rename = "a3")]
                a: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                let schema_res = derive_avro_schema(&mut input);
                let expected_token_stream = r#"let schema_fields = vec ! [apache_avro :: schema :: RecordField { name : "a3" . to_string () , doc : Some ("a doc" . into ()) , default : Some (serde_json :: from_str ("123") . expect (format ! ("Invalid JSON: {:?}" , "123") . as_str ())) , aliases : Some (vec ! ["a1" . into () , "a2" . into ()]) , schema : apache_avro :: schema :: Schema :: Int , order : apache_avro :: schema :: RecordFieldOrder :: Ascending , position : 0usize , custom_attributes : Default :: default () , }] ;"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert!(schema_token_stream.contains(expected_token_stream));
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };

        let test_enum = quote! {
            enum A {
                #[avro(rename = "A3")]
                Item1,
            }
        };

        match syn::parse2::<DeriveInput>(test_enum) {
            Ok(mut input) => {
                let schema_res = derive_avro_schema(&mut input);
                let expected_token_stream = r#"let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse schema name {}" , "A") [..]) . fully_qualified_name (enclosing_namespace) ; let enclosing_namespace = & name . namespace ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name : name . clone () } } else { named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; apache_avro :: schema :: Schema :: Enum (apache_avro :: schema :: EnumSchema { name : apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse enum name for schema {}" , "A") [..]) , aliases : None , doc : None , symbols : vec ! ["A3" . to_owned ()] , default : None , attributes : Default :: default () , })"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert!(schema_token_stream.contains(expected_token_stream));
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_avro_rs_207_rename_all_attribute() {
        let test_struct = quote! {
            #[avro(rename_all="SCREAMING_SNAKE_CASE")]
            struct A {
                item: i32,
                double_item: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                let schema_res = derive_avro_schema(&mut input);
                let expected_token_stream = r#"let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse schema name {}" , "A") [..]) . fully_qualified_name (enclosing_namespace) ; let enclosing_namespace = & name . namespace ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name : name . clone () } } else { named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema_fields = vec ! [apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : apache_avro :: schema :: Schema :: Int , order : apache_avro :: schema :: RecordFieldOrder :: Ascending , position : 0usize , custom_attributes : Default :: default () , } , apache_avro :: schema :: RecordField { name : "DOUBLE_ITEM" . to_string () , doc : None , default : None , aliases : None , schema : apache_avro :: schema :: Schema :: Int , order : apache_avro :: schema :: RecordFieldOrder :: Ascending , position : 1usize , custom_attributes : Default :: default () , }] ;"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert!(schema_token_stream.contains(expected_token_stream));
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };

        let test_enum = quote! {
            #[avro(rename_all="SCREAMING_SNAKE_CASE")]
            enum B {
                Item,
                DoubleItem,
            }
        };

        match syn::parse2::<DeriveInput>(test_enum) {
            Ok(mut input) => {
                let schema_res = derive_avro_schema(&mut input);
                let expected_token_stream = r#"let name = apache_avro :: schema :: Name :: new ("B") . expect (& format ! ("Unable to parse schema name {}" , "B") [..]) . fully_qualified_name (enclosing_namespace) ; let enclosing_namespace = & name . namespace ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name : name . clone () } } else { named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; apache_avro :: schema :: Schema :: Enum (apache_avro :: schema :: EnumSchema { name : apache_avro :: schema :: Name :: new ("B") . expect (& format ! ("Unable to parse enum name for schema {}" , "B") [..]) , aliases : None , doc : None , symbols : vec ! ["ITEM" . to_owned () , "DOUBLE_ITEM" . to_owned ()] , default : None , attributes : Default :: default () , })"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert!(schema_token_stream.contains(expected_token_stream));
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }

    #[test]
    fn test_avro_rs_207_rename_attr_has_priority_over_rename_all_attribute() {
        let test_struct = quote! {
            #[avro(rename_all="SCREAMING_SNAKE_CASE")]
            struct A {
                item: i32,
                #[avro(rename="DoubleItem")]
                double_item: i32
            }
        };

        match syn::parse2::<DeriveInput>(test_struct) {
            Ok(mut input) => {
                let schema_res = derive_avro_schema(&mut input);
                let expected_token_stream = r#"let name = apache_avro :: schema :: Name :: new ("A") . expect (& format ! ("Unable to parse schema name {}" , "A") [..]) . fully_qualified_name (enclosing_namespace) ; let enclosing_namespace = & name . namespace ; if named_schemas . contains_key (& name) { apache_avro :: schema :: Schema :: Ref { name : name . clone () } } else { named_schemas . insert (name . clone () , apache_avro :: schema :: Schema :: Ref { name : name . clone () }) ; let schema_fields = vec ! [apache_avro :: schema :: RecordField { name : "ITEM" . to_string () , doc : None , default : None , aliases : None , schema : apache_avro :: schema :: Schema :: Int , order : apache_avro :: schema :: RecordFieldOrder :: Ascending , position : 0usize , custom_attributes : Default :: default () , } , apache_avro :: schema :: RecordField { name : "DoubleItem" . to_string () , doc : None , default : None , aliases : None , schema : apache_avro :: schema :: Schema :: Int , order : apache_avro :: schema :: RecordFieldOrder :: Ascending , position : 1usize , custom_attributes : Default :: default () , }] ;"#;
                let schema_token_stream = schema_res.unwrap().to_string();
                assert!(schema_token_stream.contains(expected_token_stream));
            }
            Err(error) => panic!(
                "Failed to parse as derive input when it should be able to. Error: {error:?}"
            ),
        };
    }
}
