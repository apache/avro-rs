use crate::attributes::NamedTypeOptions;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::DataEnum;

pub fn get_data_enum_schema_def(
    container_attrs: &NamedTypeOptions,
    data_enum: DataEnum,
    ident_span: Span,
) -> Result<TokenStream, Vec<syn::Error>> {
    Ok(quote! {
        panic!("Hello world")
    })
}
