use apache_avro::AvroSchema;
#[avro(namespace = "namespace.testing")]
struct A {
    a: i32,
    b: String,
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for A {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "namespace.testing.A",
                enclosing_namespace,
            )
            .expect("Unable to parse schema name namespace.testing.A");
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref {
                name,
            }
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            {
                let mut schema_fields = ::std::vec::Vec::with_capacity(2usize);
                schema_fields
                    .push(::apache_avro::schema::RecordField {
                        name: "a".to_string(),
                        doc: ::std::option::Option::None,
                        default: <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                        aliases: ::std::vec::Vec::new(),
                        schema: <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                            named_schemas,
                            enclosing_namespace,
                        ),
                        custom_attributes: ::std::collections::BTreeMap::new(),
                    });
                schema_fields
                    .push(::apache_avro::schema::RecordField {
                        name: "b".to_string(),
                        doc: ::std::option::Option::None,
                        default: <String as ::apache_avro::AvroSchemaComponent>::field_default(),
                        aliases: ::std::vec::Vec::new(),
                        schema: <String as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                            named_schemas,
                            enclosing_namespace,
                        ),
                        custom_attributes: ::std::collections::BTreeMap::new(),
                    });
                let schema_field_set: ::std::collections::HashSet<_> = schema_fields
                    .iter()
                    .map(|rf| &rf.name)
                    .collect();
                match (&schema_fields.len(), &schema_field_set.len()) {
                    (left_val, right_val) => {
                        if !(*left_val == *right_val) {
                            let kind = ::core::panicking::AssertKind::Eq;
                            ::core::panicking::assert_failed(
                                kind,
                                &*left_val,
                                &*right_val,
                                ::core::option::Option::Some(
                                    format_args!(
                                        "Duplicate field names found: {0:?}", schema_fields,
                                    ),
                                ),
                            );
                        }
                    }
                };
                let name = ::apache_avro::schema::Name::new("namespace.testing.A")
                    .expect(
                        &::alloc::__export::must_use({
                            ::alloc::fmt::format(
                                format_args!(
                                    "Unable to parse struct name for schema {0}",
                                    "namespace.testing.A",
                                ),
                            )
                        })[..],
                    );
                let lookup: ::std::collections::BTreeMap<String, usize> = schema_fields
                    .iter()
                    .enumerate()
                    .map(|(position, field)| (field.name.to_owned(), position))
                    .collect();
                ::apache_avro::schema::Schema::Record(::apache_avro::schema::RecordSchema {
                    name,
                    aliases: ::std::option::Option::None,
                    doc: ::std::option::Option::None,
                    fields: schema_fields,
                    lookup,
                    attributes: ::std::collections::BTreeMap::new(),
                })
            }
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        let mut schema_fields = Vec::with_capacity(2usize);
        schema_fields
            .push(::apache_avro::schema::RecordField {
                name: "a".to_string(),
                doc: ::std::option::Option::None,
                default: <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                aliases: ::std::vec::Vec::new(),
                schema: <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                    named_schemas,
                    enclosing_namespace,
                ),
                custom_attributes: ::std::collections::BTreeMap::new(),
            });
        schema_fields
            .push(::apache_avro::schema::RecordField {
                name: "b".to_string(),
                doc: ::std::option::Option::None,
                default: <String as ::apache_avro::AvroSchemaComponent>::field_default(),
                aliases: ::std::vec::Vec::new(),
                schema: <String as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                    named_schemas,
                    enclosing_namespace,
                ),
                custom_attributes: ::std::collections::BTreeMap::new(),
            });
        Some(schema_fields)
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
