use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::type_to_schema_expr;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    let mut variant_expr = Vec::new();
    let mut have_null = false;
    for variant in data_enum.variants {
        let field_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
        let name = field_attrs.rename.unwrap_or_else(|| {
            container_attrs
                .rename_all
                .apply_to_variant(&variant.ident.to_string())
        });
        match variant.fields {
            Fields::Named(named) => {
                for field in named.named {
                    let ident = field_attrs
                        .rename_all
                        .or(container_attrs.rename_all_fields)
                        .apply_to_field(&field.ident.unwrap().to_string());
                    let schema_expr = type_to_schema_expr(&field.ty)?;
                    variant_expr.push(quote! {
                        fields.push(::apache_avro::schema::RecordField::builder()
                            .name(#ident.to_string())
                            .schema(#schema_expr)
                            .build())
                    });
                }
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.len() == 0 {
                    return Err(vec![syn::Error::new(
                        unnamed.span(),
                        "AvroSchema: Empty tuple variants are not supported for bare unions",
                    )]);
                } else if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let schema_expr = type_to_schema_expr(&only_one.ty)?;
                    variant_expr.push(schema_expr);
                } else if unnamed.unnamed.len() > 1 {
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
            }
            Fields::Unit => {
                if have_null {
                    return Err(vec![syn::Error::new(
                        ident_span,
                        "More than one variant maps to Schema::Null, this is not supported for bare unions",
                    )]);
                }
                variant_expr.push(quote! { ::apache_avro::schema::Schema::Null });
                have_null = true;
            }
        }
    }
    Ok(quote! {
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        #(builder.variant(#variant_expr).expect("Duplicate Schema found");)*

        ::apache_avro::schema::Schema::Union(builder.build())
    })
}
