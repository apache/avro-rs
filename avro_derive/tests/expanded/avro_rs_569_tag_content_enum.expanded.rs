use apache_avro::AvroSchema;
#[serde(tag = "bar", content = "spam")]
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
                    .variant_ignore_duplicates(::apache_avro::schema::Schema::Null)
                    .expect("Got two map or arrays with different inner types");
                builder
                    .variant_ignore_duplicates(::apache_avro::schema::Schema::Null)
                    .expect("Got two map or arrays with different inner types");
                let content_contains_null = builder
                    .contains(&::apache_avro::schema::Schema::Null);
                let content_schema = ::apache_avro::schema::Schema::Union(
                    builder.build(),
                );
                let tag_schema = ::apache_avro::schema::Schema::r#enum(
                        ::apache_avro::schema::Name::new_with_enclosing_namespace(
                                "bar",
                                enclosing_namespace,
                            )
                            .expect("Unable to parse `bar` as a Name"),
                        ::alloc::boxed::box_assume_init_into_vec_unsafe(
                            ::alloc::intrinsics::write_box_via_move(
                                ::alloc::boxed::Box::new_uninit(),
                                ["One".to_owned(), "Two".to_owned()],
                            ),
                        ),
                    )
                    .build();
                let mut fields = ::std::vec::Vec::with_capacity(2);
                fields
                    .push(
                        ::apache_avro::schema::RecordField::builder()
                            .name("bar")
                            .schema(tag_schema)
                            .build(),
                    );
                fields
                    .push(
                        ::apache_avro::schema::RecordField::builder()
                            .name("spam")
                            .schema(content_schema)
                            .maybe_default(
                                if content_contains_null {
                                    Some(::serde_json::Value::Null)
                                } else {
                                    None
                                },
                            )
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
            let mut builder = ::apache_avro::schema::UnionSchema::builder();
            builder
                .variant_ignore_duplicates(::apache_avro::schema::Schema::Null)
                .expect("Got two map or arrays with different inner types");
            builder
                .variant_ignore_duplicates(::apache_avro::schema::Schema::Null)
                .expect("Got two map or arrays with different inner types");
            let content_contains_null = builder
                .contains(&::apache_avro::schema::Schema::Null);
            let content_schema = ::apache_avro::schema::Schema::Union(builder.build());
            let tag_schema = ::apache_avro::schema::Schema::r#enum(
                    ::apache_avro::schema::Name::new_with_enclosing_namespace(
                            "bar",
                            enclosing_namespace,
                        )
                        .expect("Unable to parse `bar` as a Name"),
                    ::alloc::boxed::box_assume_init_into_vec_unsafe(
                        ::alloc::intrinsics::write_box_via_move(
                            ::alloc::boxed::Box::new_uninit(),
                            ["One".to_owned(), "Two".to_owned()],
                        ),
                    ),
                )
                .build();
            let mut fields = ::std::vec::Vec::with_capacity(2);
            fields
                .push(
                    ::apache_avro::schema::RecordField::builder()
                        .name("bar")
                        .schema(tag_schema)
                        .build(),
                );
            fields
                .push(
                    ::apache_avro::schema::RecordField::builder()
                        .name("spam")
                        .schema(content_schema)
                        .maybe_default(
                            if content_contains_null {
                                Some(::serde_json::Value::Null)
                            } else {
                                None
                            },
                        )
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
