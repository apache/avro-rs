use apache_avro::AvroSchema;
#[avro(repr = "record_internally_tagged")]
#[serde(tag = "type")]
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
        {
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
                {
                    let mut builder = ::apache_avro::schema::UnionSchema::builder();
                    let mut fields = ::std::vec::Vec::new();
                    fields
                        .push(
                            ::apache_avro::schema::RecordField::builder()
                                .name("type")
                                .schema(::apache_avro::schema::Schema::String)
                                .build(),
                        );
                    if let Some(record_fields) = <bool as ::apache_avro::AvroSchemaComponent>::get_record_fields_in_ctxt(
                        named_schemas,
                        enclosing_namespace,
                    ) {
                        fields.extend(record_fields);
                    } else {
                        {
                            ::core::panicking::panic_fmt(
                                format_args!(
                                    "Newtype variant type must implement `get_record_fields` for internally tagged enums",
                                ),
                            );
                        };
                    }
                    fields
                        .extend({
                            let mut fields = ::std::vec::Vec::with_capacity(0usize);
                            fields
                        });
                    fields
                        .extend({
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
                        });
                    fields
                        .extend({
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
                        });
                    ::apache_avro::schema::Schema::Record(
                        ::apache_avro::schema::RecordSchema::builder()
                            .name(name)
                            .maybe_aliases(::std::option::Option::None)
                            .doc(::std::option::Option::None)
                            .fields(fields)
                            .build(),
                    )
                }
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
