use apache_avro::AvroSchema;
#[avro(repr = "union_of_records")]
enum Abc {
    A,
    B(bool),
    D {},
    E { is_it_true: bool },
    F { #[avro(doc = "This is X")] x: f64, y: f32 },
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for Abc {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "Abc",
                enclosing_namespace,
            )
            .expect("Unable to parse schema name Abc");
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref {
                name,
            }
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            let mut builder = ::apache_avro::schema::UnionSchema::builder();
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "A",
                                        enclosing_namespace,
                                    )
                                    .expect(
                                        &::alloc::__export::must_use({
                                            ::alloc::fmt::format(
                                                format_args!(
                                                    "Unable to parse variant record name for schema {0}", "A",
                                                ),
                                            )
                                        })[..],
                                    ),
                            )
                            .build(),
                    ),
                )
                .expect("Duplicate Schema found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "B",
                                        enclosing_namespace,
                                    )
                                    .expect(
                                        &::alloc::__export::must_use({
                                            ::alloc::fmt::format(
                                                format_args!(
                                                    "Unable to parse variant record name for schema {0}", "B",
                                                ),
                                            )
                                        })[..],
                                    ),
                            )
                            .fields(
                                ::alloc::boxed::box_assume_init_into_vec_unsafe(
                                    ::alloc::intrinsics::write_box_via_move(
                                        ::alloc::boxed::Box::new_uninit(),
                                        [
                                            ::apache_avro::schema::RecordField::builder()
                                                .name("field_0".to_string())
                                                .doc(::std::option::Option::None)
                                                .maybe_default(
                                                    <bool as ::apache_avro::AvroSchemaComponent>::field_default(),
                                                )
                                                .aliases(::std::vec::Vec::new())
                                                .schema(
                                                    <bool as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                                        named_schemas,
                                                        enclosing_namespace,
                                                    ),
                                                )
                                                .build(),
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
                            .build(),
                    ),
                )
                .expect("Duplicate Schema found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "D",
                                        enclosing_namespace,
                                    )
                                    .expect(
                                        &::alloc::__export::must_use({
                                            ::alloc::fmt::format(
                                                format_args!(
                                                    "Unable to parse variant record name for schema {0}", "D",
                                                ),
                                            )
                                        })[..],
                                    ),
                            )
                            .fields({
                                let mut fields = ::std::vec::Vec::with_capacity(0usize);
                                fields
                            })
                            .build(),
                    ),
                )
                .expect("Duplicate Schema found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "E",
                                        enclosing_namespace,
                                    )
                                    .expect(
                                        &::alloc::__export::must_use({
                                            ::alloc::fmt::format(
                                                format_args!(
                                                    "Unable to parse variant record name for schema {0}", "E",
                                                ),
                                            )
                                        })[..],
                                    ),
                            )
                            .fields({
                                let mut fields = ::std::vec::Vec::with_capacity(1usize);
                                fields
                                    .push(
                                        ::apache_avro::schema::RecordField::builder()
                                            .name("is_it_true".to_string())
                                            .doc(::std::option::Option::None)
                                            .maybe_default(
                                                <bool as ::apache_avro::AvroSchemaComponent>::field_default(),
                                            )
                                            .aliases(::std::vec::Vec::new())
                                            .schema(
                                                <bool as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                                    named_schemas,
                                                    enclosing_namespace,
                                                ),
                                            )
                                            .build(),
                                    );
                                fields
                            })
                            .build(),
                    ),
                )
                .expect("Duplicate Schema found");
            builder
                .variant(
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(
                                ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                        "F",
                                        enclosing_namespace,
                                    )
                                    .expect(
                                        &::alloc::__export::must_use({
                                            ::alloc::fmt::format(
                                                format_args!(
                                                    "Unable to parse variant record name for schema {0}", "F",
                                                ),
                                            )
                                        })[..],
                                    ),
                            )
                            .fields({
                                let mut fields = ::std::vec::Vec::with_capacity(2usize);
                                fields
                                    .push(
                                        ::apache_avro::schema::RecordField::builder()
                                            .name("x".to_string())
                                            .doc(::std::option::Option::Some("This is X".to_string()))
                                            .maybe_default(
                                                <f64 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                            )
                                            .aliases(::std::vec::Vec::new())
                                            .schema(
                                                <f64 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                                    named_schemas,
                                                    enclosing_namespace,
                                                ),
                                            )
                                            .build(),
                                    );
                                fields
                                    .push(
                                        ::apache_avro::schema::RecordField::builder()
                                            .name("y".to_string())
                                            .doc(::std::option::Option::None)
                                            .maybe_default(
                                                <f32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                            )
                                            .aliases(::std::vec::Vec::new())
                                            .schema(
                                                <f32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                                    named_schemas,
                                                    enclosing_namespace,
                                                ),
                                            )
                                            .build(),
                                    );
                                fields
                            })
                            .build(),
                    ),
                )
                .expect("Duplicate Schema found");
            ::apache_avro::schema::Schema::Union(builder.build())
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        None
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
