use apache_avro::AvroSchema;
struct A<'a> {
    a: &'a Vec<i32>,
    b: &'static str,
}
#[automatically_derived]
impl<'a> ::apache_avro::AvroSchemaComponent for A<'a> {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "A",
                enclosing_namespace,
            )
            .expect("Unable to parse `A` as a Name");
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref {
                name,
            }
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            ::apache_avro::schema::Schema::Record(
                ::apache_avro::schema::RecordSchema::builder()
                    .aliases(::std::option::Option::None)
                    .maybe_doc(::std::option::Option::None)
                    .fields({
                        let mut fields = ::std::vec::Vec::with_capacity(2usize);
                        fields
                            .push(::apache_avro::schema::RecordField {
                                name: "a".to_string(),
                                doc: ::std::option::Option::None,
                                default: <&'a Vec<
                                    i32,
                                > as ::apache_avro::AvroSchemaComponent>::field_default(),
                                aliases: ::alloc::vec::Vec::new(),
                                schema: <&'a Vec<
                                    i32,
                                > as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                    named_schemas,
                                    enclosing_namespace,
                                ),
                                custom_attributes: ::std::collections::BTreeMap::new(),
                            });
                        fields
                            .push(::apache_avro::schema::RecordField {
                                name: "b".to_string(),
                                doc: ::std::option::Option::None,
                                default: <&'static str as ::apache_avro::AvroSchemaComponent>::field_default(),
                                aliases: ::alloc::vec::Vec::new(),
                                schema: <&'static str as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                    named_schemas,
                                    enclosing_namespace,
                                ),
                                custom_attributes: ::std::collections::BTreeMap::new(),
                            });
                        fields
                    })
                    .name(name)
                    .build(),
            )
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        ::std::option::Option::Some({
            let mut fields = ::std::vec::Vec::with_capacity(2usize);
            fields
                .push(::apache_avro::schema::RecordField {
                    name: "a".to_string(),
                    doc: ::std::option::Option::None,
                    default: <&'a Vec<
                        i32,
                    > as ::apache_avro::AvroSchemaComponent>::field_default(),
                    aliases: ::alloc::vec::Vec::new(),
                    schema: <&'a Vec<
                        i32,
                    > as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                        named_schemas,
                        enclosing_namespace,
                    ),
                    custom_attributes: ::std::collections::BTreeMap::new(),
                });
            fields
                .push(::apache_avro::schema::RecordField {
                    name: "b".to_string(),
                    doc: ::std::option::Option::None,
                    default: <&'static str as ::apache_avro::AvroSchemaComponent>::field_default(),
                    aliases: ::alloc::vec::Vec::new(),
                    schema: <&'static str as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                        named_schemas,
                        enclosing_namespace,
                    ),
                    custom_attributes: ::std::collections::BTreeMap::new(),
                });
            fields
        })
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
