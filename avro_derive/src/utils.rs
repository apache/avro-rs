use std::{fmt::Display, marker::PhantomData};

use quote::{ToTokens, quote};

/// A [`TokenStream`] that is a expression that evaluates to `Output`
///
/// This token stream can define and expect any variables names.
pub struct TypedTokenStream<Output> {
    output: PhantomData<Output>,
    stream: proc_macro2::TokenStream,
}

/// Stand-in for `apache_avro::Schema` for [`TypedTokenStream`]
pub struct Schema;

/// Stand-in for `apache_avro::schema::RecordField` for [`TypedTokenStream`]
pub struct RecordField;

impl<Output> TypedTokenStream<Output> {
    pub fn new(stream: proc_macro2::TokenStream) -> Self {
        Self {
            output: PhantomData,
            stream,
        }
    }
}

impl<Output> TypedTokenStream<Option<Output>> {
    pub fn none() -> Self {
        Self {
            output: PhantomData,
            stream: quote! { ::std::option::Option::None },
        }
    }
}

impl<Output> ToTokens for TypedTokenStream<Output> {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        self.stream.to_tokens(tokens);
    }
}

impl<Output> Display for TypedTokenStream<Output> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.stream.fmt(f)
    }
}

/// Stolen from serde
pub fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

pub fn preserve_optional(op: Option<&String>) -> TypedTokenStream<Option<String>> {
    match op {
        Some(tt) => TypedTokenStream::new(quote! {::std::option::Option::Some(#tt.into())}),
        None => TypedTokenStream::new(quote! {::std::option::Option::None}),
    }
}

pub fn doc_into_tokenstream(doc: Option<String>) -> TypedTokenStream<Option<String>> {
    match doc {
        Some(doc) => TypedTokenStream::new(quote! {::std::option::Option::Some(#doc.to_string())}),
        None => TypedTokenStream::new(quote! {::std::option::Option::None}),
    }
}

pub fn aliases(op: &[String]) -> TypedTokenStream<Option<Vec<String>>> {
    let items: Vec<proc_macro2::TokenStream> = op
        .iter()
        .map(|tt| quote! {#tt.try_into().expect("Alias is invalid")})
        .collect();
    if items.is_empty() {
        TypedTokenStream::new(quote! {::std::option::Option::None})
    } else {
        TypedTokenStream::new(quote! {::std::option::Option::Some(vec![#(#items),*])})
    }
}

pub fn field_aliases(op: &[impl quote::ToTokens]) -> TypedTokenStream<Vec<String>> {
    let items: Vec<proc_macro2::TokenStream> = op
        .iter()
        .map(|tt| quote! {#tt.try_into().expect("Alias is invalid")})
        .collect();
    if items.is_empty() {
        TypedTokenStream::new(quote! {::std::vec::Vec::new()})
    } else {
        TypedTokenStream::new(quote! {vec![#(#items),*]})
    }
}
