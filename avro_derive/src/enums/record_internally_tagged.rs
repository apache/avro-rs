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
) -> Result<TokenStream, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    let mut symbols = Vec::new();
    let mut field_additions = Vec::new();
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
                    field_additions.push(quote! {
                        fields.push(::apache_avro::schema::RecordField::builder()
                            .name(#ident.to_string())
                            .schema(#schema_expr)
                            .build())
                    });
                }
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let schema_expr = type_to_schema_expr(&only_one.ty)?;
                    field_additions.push(schema_expr);
                } else if unnamed.unnamed.len() > 1 {
                    return Err(vec![syn::Error::new(
                        unnamed.span(),
                        "Tuple variants are not supported for internally tagged enums",
                    )]);
                }
            }
            Fields::Unit => {}
        }
        symbols.push(name);
    }
    Ok(quote! {
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        let tag_name = ::apache_avro::schema::Name::new_with_enclosing_namespace(#tag, enclosing_namespace).expect(&format!("Unable to parse name for schema tag {}", #tag)[..]);
        let tag_schema = ::apache_avro::schema::Schema::r#enum(tag_name, vec![#(#symbols.to_owned()),*]).build();

        let mut fields = ::std::vec::Vec::new();
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(tag_schema)
            .build()
        );
        #(
            #field_additions
        )*
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(name)
            .maybe_aliases(#enum_aliases)
            .maybe_doc(#doc)
            .fields(fields)
            .build()
        )
    })
}
