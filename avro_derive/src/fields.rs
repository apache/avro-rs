use crate::attributes::{FieldDefault, With};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Field, Lit, Type, TypeArray, TypeTuple};

pub fn to_schema(field: &Field, with: With) -> Result<TokenStream, Vec<syn::Error>> {
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
/// An `Expr` that resolves to an instance of `Option<Vec<RecordField>>`.
pub fn to_record_fields(field: &Field, with: With) -> Result<TokenStream, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_record_fields(&field.ty)?),
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

pub fn to_default(field: &Field, default: FieldDefault) -> Result<TokenStream, Vec<syn::Error>> {
    match default {
        FieldDefault::Disabled => Ok(quote! { ::std::option::Option::None }),
        FieldDefault::Trait => type_to_field_default(&field.ty),
        FieldDefault::Value(default_value) => {
            let _: serde_json::Value = serde_json::from_str(&default_value[..]).map_err(|e| {
                vec![syn::Error::new(
                    field.span(),
                    format!("Invalid avro default json: \n{e}"),
                )]
            })?;
            Ok(quote! {
                ::std::option::Option::Some(::serde_json::from_str(#default_value).expect("Unreachable! Checked at compile time"))
            })
        }
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
/// An `Expr` that resolves to an instance of `Schema`.
fn type_to_schema_expr(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)},
        ),
        Type::Tuple(tuple) => tuple_to_schema(tuple),
        Type::Array(array) => array_to_schema(array),
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

/// Create a schema definition for a tuple.
///
/// # Mapping
/// - `0-tuple` => `Schema::Null`,
/// - `1-tuple` => Schema of the only element,
/// - `n-tuple` => `Schema::Record`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Schema`.
fn tuple_to_schema(tuple: &TypeTuple) -> Result<TokenStream, Vec<syn::Error>> {
    if tuple.elems.is_empty() {
        Ok(quote! {::apache_avro::schema::Schema::Null})
    } else if tuple.elems.len() == 1 {
        type_to_schema_expr(&tuple.elems.iter().next().unwrap())
    } else {
        let mut fields = Vec::with_capacity(tuple.elems.len());

        for (index, elem) in tuple.elems.iter().enumerate() {
            let name = format!("field_{index}");
            let field_schema_expr = type_to_schema_expr(elem)?;
            fields.push(quote! {
                ::apache_avro::schema::RecordField::builder()
                    .name(#name.to_string())
                    .schema(#field_schema_expr)
                    .build()
            });
        }

        // Try to create a unique name for this record, this is done in a best effort way and the
        // name is NOT recorded in `names`.
        // This will always start and end with a `_` as `(` and `)` are not valid characters
        let tuple_as_valid_name = tuple
            .to_token_stream()
            .to_string()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>();

        let name = format!("tuple_{}{tuple_as_valid_name}", tuple.elems.len());

        Ok(quote! {
            ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
                .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                .fields(vec![
                    #(#fields, )*
                ])
                .attributes(
                    [
                        ("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true)),
                    ].into()
                )
                .build()
            )
        })
    }
}

/// Create a schema definition for an array.
///
/// # Mapping
/// - `[T; 0]` => `Schema::Null`,
/// - `[T; 1]` => Schema of `T`,
/// - `[T; N]` => `Schema::Record`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Schema`.
fn array_to_schema(array: &TypeArray) -> Result<TokenStream, Vec<syn::Error>> {
    let Expr::Lit(ExprLit {
        lit: Lit::Int(lit), ..
    }) = &array.len
    else {
        return Err(vec![syn::Error::new(
            array.span(),
            "AvroSchema: Expected a integer literal for the array length",
        )]);
    };
    // This should always work as the length always needs to fit in a usize
    let len: usize = lit.base10_parse().map_err(|e| vec![e])?;

    if len == 0 {
        Ok(quote! {::apache_avro::schema::Schema::Null})
    } else if len == 1 {
        type_to_schema_expr(&array.elem)
    } else {
        let t_schema_expr = type_to_schema_expr(&array.elem)?;
        let fields = (0..len).map(|index| {
            let name = format!("field_{index}");
            quote! {
                ::apache_avro::schema::RecordField::builder()
                    .name(#name.to_string())
                    .schema(#t_schema_expr)
                    .build()
            }
        });

        // Try to create a unique name for this record, this is done as best effort and the
        // name is NOT recorded in `names`.
        let array_elem_as_valid_name = array
            .elem
            .to_token_stream()
            .to_string()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect::<String>();

        let name = format!("array_{len}_{array_elem_as_valid_name}");

        Ok(quote! {
            ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
                .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                .fields(vec![
                    #(#fields, )*
                ])
                .attributes(
                    [
                        ("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true)),
                    ].into()
                )
                .build()
            )
        })
    }
}

fn type_to_record_fields(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Slice(_) | Type::Path(_) | Type::Reference(_) => Ok(
            quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)},
        ),
        Type::Array(array) => array_to_record_fields(array),
        Type::Tuple(tuple) => tuple_to_record_fields(tuple),
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
    }
}

/// Create a schema definition for a tuple.
///
/// # Mapping
/// - `0-tuple` => `Schema::Null`,
/// - `1-tuple` => Schema of the only element,
/// - `n-tuple` => `Schema::Record`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Schema`.
fn tuple_to_record_fields(tuple: &TypeTuple) -> Result<TokenStream, Vec<syn::Error>> {
    if tuple.elems.is_empty() {
        Ok(quote! {::std::option::Option::None})
    } else if tuple.elems.len() == 1 {
        type_to_record_fields(&tuple.elems.iter().next().unwrap())
    } else {
        let mut fields = Vec::with_capacity(tuple.elems.len());

        for (index, elem) in tuple.elems.iter().enumerate() {
            let name = format!("field_{index}");
            let field_schema_expr = type_to_schema_expr(elem)?;
            fields.push(quote! {
                ::apache_avro::schema::RecordField::builder()
                    .name(#name.to_string())
                    .schema(#field_schema_expr)
                    .build()
            });
        }

        Ok(quote! {
            ::std::option::Option::Some(vec![#(#fields, )*])
        })
    }
}
/// Create a schema definition for an array.
///
/// # Mapping
/// - `[T; 0]` => `Schema::Null`,
/// - `[T; 1]` => Schema of `T`,
/// - `[T; N]` => `Schema::Record`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Schema`.
fn array_to_record_fields(array: &TypeArray) -> Result<TokenStream, Vec<syn::Error>> {
    let Expr::Lit(ExprLit {
                      lit: Lit::Int(lit), ..
                  }) = &array.len
    else {
        return Err(vec![syn::Error::new(
            array.span(),
            "AvroSchema: Expected a integer literal for the array length",
        )]);
    };
    // This should always work as the length always needs to fit in a usize
    let len: usize = lit.base10_parse().map_err(|e| vec![e])?;

    if len == 0 {
        Ok(quote! {::std::option::Option::None})
    } else if len == 1 {
        type_to_record_fields(&array.elem)
    } else {
        let t_schema_expr = type_to_schema_expr(&array.elem)?;
        let fields = (0..len).map(|index| {
            let name = format!("field_{index}");
            quote! {
                ::apache_avro::schema::RecordField::builder()
                    .name(#name.to_string())
                    .schema(#t_schema_expr)
                    .build()
            }
        });

        Ok(quote! {
            ::std::option::Option::Some(vec![#(#fields, )*])
        })
    }
}

fn type_to_field_default(ty: &Type) -> Result<TokenStream, Vec<syn::Error>> {
    match ty {
        Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
            Ok(quote! {<#ty as :: apache_avro::AvroSchemaComponent>::field_default()})
        }
        Type::Ptr(_) => Err(vec![syn::Error::new_spanned(
            ty,
            "AvroSchema: derive does not support raw pointers",
        )]),
        Type::Tuple(_) | Type::Array(_) => Ok(quote! { ::std::option::Option::None }),
        _ => Err(vec![syn::Error::new_spanned(
            ty,
            format!(
                "AvroSchema: Unexpected type encountered! Please open an issue if this kind of type should be supported: {ty:?}"
            ),
        )]),
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
