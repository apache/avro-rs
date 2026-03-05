use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::type_to_schema_expr;
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
        let field_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
        let name = field_attrs.rename.unwrap_or_else(|| {
            container_attrs
                .rename_all
                .apply_to_variant(&variant.ident.to_string())
        });
        match variant.fields {
            Fields::Named(named) => {
                let mut fields = Vec::with_capacity(named.named.len());
                for field in named.named {
                    let ident = field_attrs
                        .rename_all
                        .or(container_attrs.rename_all_fields)
                        .apply_to_field(&field.ident.unwrap().to_string());
                    let schema_expr = type_to_schema_expr(&field.ty)?;
                    fields.push(quote! {
                        ::apache_avro::schema::RecordField::builder()
                            .name(#ident.to_string())
                            .schema(#schema_expr)
                            .build()
                    });
                }

                let schema_expr = quote! {
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                            .fields(vec![
                                #(#fields,)*
                            ])
                            .build()
                    )
                };
                variant_expr.push(schema_expr);
            }
            Fields::Unnamed(unnamed) => {
                let mut fields = Vec::with_capacity(unnamed.unnamed.len());
                for (index, field) in unnamed.unnamed.iter().enumerate() {
                    let field_schema_expr = type_to_schema_expr(&field.ty)?;
                    fields.push(quote! {
                        ::apache_avro::schema::RecordField::builder()
                            .name(format!("field_{}", #index))
                            .schema(#field_schema_expr)
                            .build()
                    });
                }

                let amount_of_fields = unnamed.unnamed.len();

                let schema_expr = quote! {
                    let mut builder = ::apache_avro::schema::RecordSchema::builder()
                            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                            .fields(vec![
                                #(#fields,)*
                            ]);
                    if #amount_of_fields == 1 {
                        builder = builder.attributes([("org.apache.avro.rust.union_of_records".to_string(), ::serde_json::value::Value::Bool(true))].into());
                    } else if #amount_of_fields > 1 {
                        builder = builder.attributes([("org.apache.avro.rust.tuple".to_string(), ::serde_json::value::Value::Bool(true))].into());
                    }

                    ::apache_avro::schema::Schema::Record(builder.build())
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
