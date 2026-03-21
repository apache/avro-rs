use crate::RecordField;
use crate::attributes::{FieldDefault, With};
use crate::utils::{Schema, TypedTokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{Expr, Field, Type};

pub fn to_schema(field: &Field, with: With) -> Result<TypedTokenStream<Schema>, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_schema_expr(&field.ty)?),
        With::Serde(path) => Ok(TypedTokenStream::<Schema>::new(
            quote! { #path::get_schema_in_ctxt(named_schemas, enclosing_namespace) },
        )),
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(TypedTokenStream::<Schema>::new(quote! { (#closure)() }))
            } else {
                Err(vec![syn::Error::new(
                    field.span(),
                    "Expected closure with 0 parameters",
                )])
            }
        }
        With::Expr(Expr::Path(path)) => Ok(TypedTokenStream::<Schema>::new(
            quote! { #path(named_schemas, enclosing_namespace) },
        )),
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
pub fn to_record_fields(
    field: &Field,
    with: With,
) -> Result<TypedTokenStream<Option<Vec<RecordField>>>, Vec<syn::Error>> {
    match with {
        With::Trait => Ok(type_to_record_fields(&field.ty)?),
        With::Serde(path) => Ok(TypedTokenStream::<Option<Vec<RecordField>>>::new(
            quote! { #path::get_record_fields_in_ctxt(named_schemas, enclosing_namespace) },
        )),
        With::Expr(Expr::Closure(closure)) => {
            if closure.inputs.is_empty() {
                Ok(TypedTokenStream::<Option<Vec<RecordField>>>::new(quote! {
                    ::apache_avro::serde::get_record_fields_in_ctxt(
                        named_schemas,
                        enclosing_namespace,
                        |_, _| (#closure)(),
                    )
                }))
            } else {
                Err(vec![syn::Error::new(
                    field.span(),
                    "Expected closure with 0 parameters",
                )])
            }
        }
        With::Expr(Expr::Path(path)) => {
            Ok(TypedTokenStream::<Option<Vec<RecordField>>>::new(quote! {
                ::apache_avro::serde::get_record_fields_in_ctxt(named_schemas, enclosing_namespace, #path)
            }))
        }
        With::Expr(_expr) => Err(vec![syn::Error::new(
            field.span(),
            "Invalid expression, expected function or closure",
        )]),
    }
}

pub fn to_default(
    field: &Field,
    default: FieldDefault,
) -> Result<TypedTokenStream<Option<serde_json::Value>>, Vec<syn::Error>> {
    match default {
        FieldDefault::Disabled => Ok(TypedTokenStream::none()),
        FieldDefault::Trait => type_to_field_default(&field.ty),
        FieldDefault::Value(default_value) => {
            let _: serde_json::Value = serde_json::from_str(&default_value[..]).map_err(|e| {
                vec![syn::Error::new(
                    field.span(),
                    format!("Invalid avro default json: \n{e}"),
                )]
            })?;
            Ok(TypedTokenStream::<Option<serde_json::Value>>::new(quote! {
                ::std::option::Option::Some(::serde_json::from_str(#default_value).expect("Unreachable! Checked at compile time"))
            }))
        }
        FieldDefault::Serde(path) => {
            Ok(TypedTokenStream::<Option<serde_json::Value>>::new(quote! {
                #path::field_default()
            }))
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
fn type_to_schema_expr(ty: &Type) -> Result<TypedTokenStream<Schema>, Vec<syn::Error>> {
    match ty {
        Type::Tuple(_) | Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
            Ok(TypedTokenStream::<Schema>::new(
                quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(named_schemas, enclosing_namespace)},
            ))
        }
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

fn type_to_record_fields(
    ty: &Type,
) -> Result<TypedTokenStream<Option<Vec<RecordField>>>, Vec<syn::Error>> {
    match ty {
        Type::Tuple(_) | Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
            Ok(TypedTokenStream::<Option<Vec<RecordField>>>::new(
                quote! {<#ty as :: apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(named_schemas, enclosing_namespace)},
            ))
        }
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

fn type_to_field_default(
    ty: &Type,
) -> Result<TypedTokenStream<Option<serde_json::Value>>, Vec<syn::Error>> {
    match ty {
        Type::Tuple(_) | Type::Array(_) | Type::Slice(_) | Type::Path(_) | Type::Reference(_) => {
            Ok(TypedTokenStream::<Option<serde_json::Value>>::new(
                quote! {<#ty as :: apache_avro::AvroSchemaComponent>::field_default()},
            ))
        }
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
