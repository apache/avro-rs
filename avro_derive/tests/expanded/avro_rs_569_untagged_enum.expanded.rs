use apache_avro::AvroSchema;
#[serde(untagged)]
enum Foo {
    One,
    Two,
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for Foo {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "Foo",
                enclosing_namespace,
            )
            .expect("Unable to parse `Foo` as a Name");
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref {
                name,
            }
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            {
                let mut builder = ::apache_avro::schema::UnionSchema::builder();
                builder
                    .variant(::apache_avro::schema::Schema::Null)
                    .expect("Duplicate Schema found");
                builder
                    .variant(::apache_avro::schema::Schema::Null)
                    .expect("Duplicate Schema found");
                ::apache_avro::schema::Schema::Union(builder.build())
            }
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        ::std::option::Option::None
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
pub fn main() {}
