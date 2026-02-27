use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::{aliases, preserve_optional, type_to_schema_expr};
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    tag: &str,
    content: &str,
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    let mut symbols = Vec::new();
    let mut schema_definitions = Vec::new();
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
                schema_definitions.push(schema_expr);
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.is_empty() {
                    let schema_expr = quote! {
                        ::apache_avro::schema::Schema::Null
                    };
                    schema_definitions.push(schema_expr);
                } else if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let field_schema_expr = type_to_schema_expr(&only_one.ty)?;
                    schema_definitions.push(field_schema_expr);
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
                    schema_definitions.push(schema_expr);
                }
            }
            Fields::Unit => schema_definitions.push(quote! { ::apache_avro::schema::Schema::Null }),
        }
        symbols.push(name);
    }
    Ok(quote! {
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        #(
            builder.variant_ignore_duplicates(#schema_definitions).expect("Unions cannot have duplicates");
        )*

        let content_contains_null = builder.contains(&::apache_avro::schema::Schema::Null);
        let content_schema = ::apache_avro::schema::Schema::Union(builder.build());

        let tag_name = ::apache_avro::schema::Name::new_with_enclosing_namespace(#tag, enclosing_namespace).expect(&format!("Unable to parse name for schema tag {}", #tag)[..]);
        let tag_schema = ::apache_avro::schema::Schema::r#enum(tag_name, vec![#(#symbols.to_owned()),*]).build();

        let mut fields = ::std::vec::Vec::with_capacity(2);
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(tag_schema)
            .build()
        );
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#content)
            .schema(content_schema)
            // This is needed for unit variants, for which Serde won't serialize content
            .maybe_default(if content_contains_null { Some(::serde_json::Value::Null) } else { None })
            .build()
        );
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(name)
            .maybe_aliases(#enum_aliases)
            .maybe_doc(#doc)
            .fields(fields)
            .build()
        )
    })
}
