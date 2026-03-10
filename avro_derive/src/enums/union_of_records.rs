use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::named_to_record_fields;
use crate::tuple::tuple_struct_variant_to_record_schema;
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut variant_expr = Vec::new();
    for variant in data_enum.variants {
        let variant_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
        let name = variant_attrs.rename.unwrap_or_else(|| {
            container_attrs
                .rename_all
                .apply_to_variant(&variant.ident.to_string())
        });
        match variant.fields {
            Fields::Named(named) => {
                let fields = named_to_record_fields(
                    named,
                    variant_attrs
                        .rename_all
                        .or(container_attrs.rename_all_fields),
                )?;

                let schema_expr = quote! {
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                            .fields(#fields)
                            .build()
                    )
                };
                variant_expr.push(schema_expr);
            }
            Fields::Unnamed(unnamed) => {
                let schema_expr = if unnamed.unnamed.len() == 1 {
                    tuple_struct_variant_to_record_schema(
                        unnamed,
                        &name,
                        &["org.apache.avro.rust.union_of_records"],
                    )?
                } else {
                    tuple_struct_variant_to_record_schema(unnamed, &name, &[])?
                };

                variant_expr.push(schema_expr);
            }
            Fields::Unit => {
                let schema_expr = quote! {
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                            .build()
                    )
                };
                variant_expr.push(schema_expr);
            }
        }
    }
    Ok(quote! {
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        #(builder.variant(#variant_expr).expect("Duplicate Schema found");)*

        ::apache_avro::schema::Schema::Union(builder.build())
    })
}
