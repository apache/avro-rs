use proc_macro2::TokenStream;
use quote::quote;
use syn::{FieldsUnnamed, spanned::Spanned};

use crate::{FieldOptions, doc_into_tokenstream, field_aliases, fields};

/// Create a `Schema::Record` from this tuple definition.
///
/// Fields are named `field_{field_index}` and the struct will have the provided name.
///
/// The schema will have the attribute `org.apache.avro.rust.tuple` any other specified in `extra_attributes`.
/// All attributes will have a value of `true`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Schema`.
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

/// Create a vector of `RecordField`s named `field_{field_index}`.
///
/// # `TokenStream`
/// ## Context
/// The token stream expects the following variables to be defined:
/// - `named_schemas`: `&mut HashSet<Name>`
/// - `enclosing_namespace`: `Option<&str>`
/// ## Returns
/// An `Expr` that resolves to an instance of `Vec<RecordField>`.
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
        let default_value = fields::to_default(&field, field_attrs.default)?;
        let aliases = field_aliases(&field_attrs.alias);
        let doc = doc_into_tokenstream(field_attrs.doc);
        let name = field_attrs
            .rename
            .unwrap_or_else(|| format!("field_{index}"));
        let field_schema_expr = crate::fields::to_schema(&field, field_attrs.with)?;
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
