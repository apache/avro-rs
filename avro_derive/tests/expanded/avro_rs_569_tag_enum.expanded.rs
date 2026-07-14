use apache_avro::AvroSchema;
#[serde(tag = "bar")]
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
                let mut fields = ::std::vec::Vec::new();
                fields
                    .push(
                        ::apache_avro::schema::RecordField::builder()
                            .name("bar")
                            .schema(::apache_avro::schema::Schema::String)
                            .build(),
                    );
                ::apache_avro::schema::Schema::Record(
                    ::apache_avro::schema::RecordSchema::builder()
                        .name(name)
                        .aliases(::std::option::Option::None)
                        .doc(::std::option::Option::None)
                        .fields(fields)
                        .build(),
                )
            }
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        {
            let mut fields = ::std::vec::Vec::new();
            fields
                .push(
                    ::apache_avro::schema::RecordField::builder()
                        .name("bar")
                        .schema(::apache_avro::schema::Schema::String)
                        .build(),
                );
            Some(fields)
        }
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
pub fn main() {}
