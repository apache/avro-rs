use apache_avro::AvroSchema;
struct B(i32, String);
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for B {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "B",
                enclosing_namespace,
            )
            .expect("Unable to parse `B` as a Name");
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
                    .fields(
                        ::alloc::boxed::box_assume_init_into_vec_unsafe(
                            ::alloc::intrinsics::write_box_via_move(
                                ::alloc::boxed::Box::new_uninit(),
                                [
                                    ::apache_avro::schema::RecordField {
                                        name: "field_0".to_string(),
                                        doc: ::std::option::Option::None,
                                        default: <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                        aliases: ::alloc::vec::Vec::new(),
                                        schema: <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                        custom_attributes: ::std::collections::BTreeMap::new(),
                                    },
                                    ::apache_avro::schema::RecordField {
                                        name: "field_1".to_string(),
                                        doc: ::std::option::Option::None,
                                        default: <String as ::apache_avro::AvroSchemaComponent>::field_default(),
                                        aliases: ::alloc::vec::Vec::new(),
                                        schema: <String as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                        custom_attributes: ::std::collections::BTreeMap::new(),
                                    },
                                ],
                            ),
                        ),
                    )
                    .name(name)
                    .build(),
            )
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        ::std::option::Option::Some(
            ::alloc::boxed::box_assume_init_into_vec_unsafe(
                ::alloc::intrinsics::write_box_via_move(
                    ::alloc::boxed::Box::new_uninit(),
                    [
                        ::apache_avro::schema::RecordField {
                            name: "field_0".to_string(),
                            doc: ::std::option::Option::None,
                            default: <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                            aliases: ::alloc::vec::Vec::new(),
                            schema: <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                named_schemas,
                                enclosing_namespace,
                            ),
                            custom_attributes: ::std::collections::BTreeMap::new(),
                        },
                        ::apache_avro::schema::RecordField {
                            name: "field_1".to_string(),
                            doc: ::std::option::Option::None,
                            default: <String as ::apache_avro::AvroSchemaComponent>::field_default(),
                            aliases: ::alloc::vec::Vec::new(),
                            schema: <String as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                named_schemas,
                                enclosing_namespace,
                            ),
                            custom_attributes: ::std::collections::BTreeMap::new(),
                        },
                    ],
                ),
            ),
        )
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
fn main() {}
