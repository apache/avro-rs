use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{Expr, ExprLit, FieldsUnnamed, Lit, TypeArray, TypeTuple, spanned::Spanned};

use crate::{FieldOptions, doc_into_tokenstream, field_aliases, type_to_schema_expr};

/// Create a `Schema::Record` from this tuple definition.
///
/// Fields are named `field_{field_index}` and the struct will have the provided name.
///
/// The schema will have the attribute `org.apache.avro.rust.tuple` any any other specified in `extra_attributes`.
/// All attributes will have a value of `true`.
pub fn tuple_struct_variant_to_record_schema(
    unnamed: FieldsUnnamed,
    name: &str,
    extra_attributes: &[&str],
) -> Result<TokenStream, Vec<syn::Error>> {
    let fields = unnamed_to_record_fields(unnamed)?;

    Ok(quote! {
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
            .fields(#fields)
            .attributes(
                [
                    ("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true)),
                    #((#extra_attributes.to_string(), ::serde_json::value::Value::Bool(true)),)*
                ].into()
            )
            .build()
        )
    })
}

/// Create a schema definition for a tuple.
///
/// # Mapping
/// - `0-tuple` => `Schema::Null`,
/// - `1-tuple` => Schema of the only element,
/// - `n-tuple` => `Schema::Record`.
pub fn tuple_to_schema(tuple: &TypeTuple) -> Result<TokenStream, Vec<syn::Error>> {
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
pub fn array_to_schema(array: &TypeArray) -> Result<TokenStream, Vec<syn::Error>> {
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

/// Create a vector of `RecordField`s named `field_{field_index}`.
pub fn unnamed_to_record_fields(unnamed: FieldsUnnamed) -> Result<TokenStream, Vec<syn::Error>> {
    let mut fields = Vec::with_capacity(unnamed.unnamed.len());
    for (index, field) in unnamed.unnamed.into_iter().enumerate() {
        let field_attrs = FieldOptions::new(&field.attrs, field.span())?;
        if field_attrs.skip {
            continue;
        } else if field_attrs.flatten {
            return Err(vec![syn::Error::new(
                field.span(),
                "AvroSchema: `#[serde(flatten)]` is not supported on tuple fields",
            )]);
        }
        let default_value = field_attrs
            .default
            .into_tokenstream(field.ident.span(), &field.ty)?;
        let aliases = field_aliases(&field_attrs.alias);
        let doc = doc_into_tokenstream(field_attrs.doc);
        let name = field_attrs
            .rename
            .unwrap_or_else(|| format!("field_{index}"));
        let field_schema_expr = type_to_schema_expr(&field.ty)?;
        fields.push(quote! {
            ::apache_avro::schema::RecordField::builder()
                .name(#name.to_string())
                .doc(#doc)
                .maybe_default(#default_value)
                .aliases(#aliases)
                .schema(#field_schema_expr)
                .build()
        });
    }
    Ok(quote! {
        vec![
            #(#fields, )*
        ]
    })
}
