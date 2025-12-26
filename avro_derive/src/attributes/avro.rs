use crate::case::RenameRule;

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct FieldAttributes {
    #[darling(default)]
    pub doc: Option<String>,
    #[darling(default)]
    pub default: Option<String>,
    #[darling(multiple)]
    pub alias: Vec<String>,
    #[darling(default)]
    pub rename: Option<String>,
    #[darling(default)]
    pub skip: bool,
    #[darling(default)]
    pub flatten: bool,
}

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct VariantAttributes {
    #[darling(default)]
    pub rename: Option<String>,
}

#[derive(darling::FromAttributes)]
#[darling(attributes(avro))]
pub struct ContainerAttributes {
    #[darling(default)]
    pub name: Option<String>,
    #[darling(default)]
    pub namespace: Option<String>,
    #[darling(default)]
    pub doc: Option<String>,
    #[darling(multiple)]
    pub alias: Vec<String>,
    #[darling(default)]
    pub rename_all: RenameRule,
}
