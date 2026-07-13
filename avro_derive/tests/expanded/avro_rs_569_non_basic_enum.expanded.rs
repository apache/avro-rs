use apache_avro::AvroSchema;
enum NonBasic {
    A(i32),
    B,
    C,
    D,
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for NonBasic {
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
                                        ],
                                    ),
                                ),
                            )
                            .attributes(
                                [
                                    (
                                        "org.apache.avro.rust.tuple".to_string(),
                                        ::serde_json::value::Value::Bool(true),
                                    ),
                                    (
                                        "org.apache.avro.rust.union_of_records".to_string(),
                                        ::serde_json::value::Value::Bool(true),
                                    ),
                                ]
                                    .into(),
                            )
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "A",
                                        enclosing_namespace,
                                    )
                                    .expect("Unable to parse `A` as a Name"),
                            )
                            .build(),
                    ),
                )
                .expect("Duplicate names found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::record(
                            ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                    "B",
                                    enclosing_namespace,
                                )
                                .expect("Unable to parse `B` as a Name"),
                        )
                        .build(),
                )
                .expect("Duplicate names found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::record(
                            ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                    "C",
                                    enclosing_namespace,
                                )
                                .expect("Unable to parse `C` as a Name"),
                        )
                        .build(),
                )
                .expect("Duplicate names found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::record(
                            ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                    "D",
                                    enclosing_namespace,
                                )
                                .expect("Unable to parse `D` as a Name"),
                        )
                        .build(),
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
fn main() {}
