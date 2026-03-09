use crate::attributes::{NamedTypeOptions, VariantOptions};
use crate::utils::{Schema, TypedTokenStream};
use crate::{FieldOptions, aliases, fields, named_to_record_fields, preserve_optional};
use quote::quote;
use syn::spanned::Spanned;
use syn::{DataEnum, Fields};

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    tag: &str,
) -> Result<TypedTokenStream<Schema>, Vec<syn::Error>> {
    let doc = preserve_optional(container_attrs.doc.as_ref());
    let enum_aliases = aliases(&container_attrs.aliases);
    let mut field_additions = Vec::new();
    for variant in data_enum.variants {
        let variant_attrs = VariantOptions::new(&variant.attrs, variant.span())?;
        match variant.fields {
            Fields::Named(named) => {
                let named_fields = named_to_record_fields(
                    named,
                    variant_attrs
                        .rename_all
                        .or(container_attrs.rename_all_fields),
                )?;
                field_additions.push(quote! {
                    fields.extend(#named_fields);
                });
            }
            Fields::Unnamed(unnamed) => {
                if unnamed.unnamed.len() == 1 {
                    let only_one = unnamed.unnamed.iter().next().expect("There is one");
                    let field_attrs =
                        FieldOptions::new_for_newtype(&only_one.attrs, only_one.span())?;
                    let schema_expr = fields::to_record_fields(only_one, field_attrs.with)?;
                    field_additions.push(quote! {
                        if let Some(record_fields) = #schema_expr {
                            fields.extend(record_fields);
                        } else {
                            panic!("Newtype variant type must implement `get_record_fields` for internally tagged enums");
                        }
                    });
                } else if unnamed.unnamed.len() > 1 {
                    return Err(vec![syn::Error::new(
                        unnamed.span(),
                        "Tuple variants are not supported for internally tagged enums",
                    )]);
                }
            }
            Fields::Unit => {}
        }
    }
    Ok(TypedTokenStream::new(quote! {{
        let mut builder = ::apache_avro::schema::UnionSchema::builder();

        let mut fields = ::std::vec::Vec::new();
        fields.push(::apache_avro::schema::RecordField::builder()
            .name(#tag)
            .schema(::apache_avro::schema::Schema::String)
            .build()
        );
        #(
            #field_additions
        )*
        ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema::builder()
            .name(name)
            .maybe_aliases(#enum_aliases)
            .doc(#doc)
            .fields(fields)
            .build()
        )
    }}))
}
