use apache_avro::AvroSchema;
#[serde(rename_all_fields = "UPPERCASE")]
enum Foo {
    Bar { a: String, b: i32 },
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for Foo {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        {
            let mut builder = ::apache_avro::schema::UnionSchema::builder();
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .aliases(::std::option::Option::None)
                            .maybe_doc(::std::option::Option::None)
                            .fields({
                                let mut fields = ::std::vec::Vec::with_capacity(2usize);
                                fields
                                    .push(::apache_avro::schema::RecordField {
                                        name: "A".to_string(),
                                        doc: ::std::option::Option::None,
                                        default: <String as ::apache_avro::AvroSchemaComponent>::field_default(),
                                        aliases: ::alloc::vec::Vec::new(),
                                        schema: <String as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                        custom_attributes: ::std::collections::BTreeMap::new(),
                                    });
                                fields
                                    .push(::apache_avro::schema::RecordField {
                                        name: "B".to_string(),
                                        doc: ::std::option::Option::None,
                                        default: <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                        aliases: ::alloc::vec::Vec::new(),
                                        schema: <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                        custom_attributes: ::std::collections::BTreeMap::new(),
                                    });
                                fields
                            })
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "Bar",
                                        enclosing_namespace,
                                    )
                                    .expect("Unable to parse `Bar` as a Name"),
                            )
                            .build(),
                    ),
                )
                .expect("Duplicate names found");
            ::apache_avro::schema::Schema::Union(builder.build())
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
