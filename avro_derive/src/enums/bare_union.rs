use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::tuple::tuple_struct_variant_to_record_schema;
use crate::utils::{Schema, TypedTokenStream};
use crate::{FieldOptions, fields};
use proc_macro2::Span;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TypedTokenStream<Schema>, Vec<syn::Error>> {
    let mut variant_expr = Vec::new();
    let mut have_null = false;
    for variant in data_enum.variants {
        let variant_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
        let name = variant_attrs.rename.unwrap_or_else(|| {
            container_attrs
                .rename_all
                .apply_to_variant(&variant.ident.to_string())
        });
        match variant.fields {
            Fields::Named(named) => {
                // TODO: Support struct variants for untagged enums. All fields of all variants need to be in one record with
                //       defaults for every field and the record named as the enum.
                return Err(vec![syn::Error::new(
                    named.span(),
                    "AvroSchema: Struct variants are not supported for untagged structs",
                )]);
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.is_empty() {
                    return Err(vec![syn::Error::new(
                        unnamed.span(),
                        "AvroSchema: Empty tuple variants are not supported for bare unions",
                    )]);
                } else if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let field_attrs =
                        FieldOptions::new_for_newtype(&only_one.attrs, only_one.span())?;
                    let schema_expr = fields::to_schema(only_one, field_attrs.with)?;
                    variant_expr.push(schema_expr);
                } else if unnamed.unnamed.len() > 1 {
                    let schema_expr = tuple_struct_variant_to_record_schema(unnamed, &name, &[])?;

                    variant_expr.push(schema_expr);
                }
            }
            Fields::Unit => {
                if have_null {
                    return Err(vec![syn::Error::new(
                        ident_span,
                        "More than one variant maps to Schema::Null, this is not supported for bare unions",
                    )]);
                }
                variant_expr.push(TypedTokenStream::<Schema>::new(
                    quote! { ::apache_avro::schema::Schema::Null },
                ));
                have_null = true;
            }
        }
    }
    Ok(TypedTokenStream::new(quote! {{
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        #(builder.variant(#variant_expr).expect("Duplicate Schema found");)*

        ::apache_avro::schema::Schema::Union(builder.build())
    }}))
}
