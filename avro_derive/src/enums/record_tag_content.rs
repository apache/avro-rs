use crate::attributes::{FieldOptions, NamedTypeOptions, VariantOptions};
use crate::tuple::tuple_struct_variant_to_record_schema;
use crate::utils::{Schema, TypedTokenStream};
use crate::{aliases, fields, named_to_record_fields, preserve_optional};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    tag: &str,
    content: &str,
) -> Result<TypedTokenStream<Schema>, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    let mut symbols = Vec::new();
    let mut schema_definitions = Vec::new();
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

                let schema_expr = TypedTokenStream::<Schema>::new(quote! {
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(::apache_avro::schema::Name::new_with_enclosing_namespace(#name, enclosing_namespace).expect(&format!("Unable to parse variant record name for schema {}", #name)[..]))
                            .fields(#fields)
                            .build()
                    )
                });
                schema_definitions.push(schema_expr);
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.is_empty() {
                    let schema_expr = TypedTokenStream::<Schema>::new(quote! {
                        ::apache_avro::schema::Schema::Null
                    });
                    schema_definitions.push(schema_expr);
                } else if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let field_attrs =
                        FieldOptions::new_for_newtype(&only_one.attrs, only_one.span())?;
                    let schema_expr = fields::to_schema(only_one, field_attrs.with)?;
                    schema_definitions.push(schema_expr);
                } else if unnamed.unnamed.len() > 1 {
                    let schema_expr = tuple_struct_variant_to_record_schema(unnamed, &name, &[])?;

                    schema_definitions.push(schema_expr);
                }
            }
            Fields::Unit => schema_definitions.push(TypedTokenStream::<Schema>::new(
                quote! { ::apache_avro::schema::Schema::Null },
            )),
        }
        symbols.push(name);
    }
    Ok(TypedTokenStream::new(quote! {{
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
            .doc(#doc)
            .fields(fields)
            .build()
        )
    }}))
}
