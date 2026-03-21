use apache_avro::AvroSchema;
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct A {
    item: i32,
    #[serde(rename = "DoubleItem")]
    double_item: i32,
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for A {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        {
            let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                    "A",
                    enclosing_namespace,
                )
                .expect("Unable to parse schema name A");
            if named_schemas.contains(&name) {
                ::apache_avro::schema::Schema::Ref {
                    name,
                }
            } else {
                let enclosing_namespace = name.namespace();
                named_schemas.insert(name.clone());
                {
                    let schema_fields = {
                        let mut fields = ::std::vec::Vec::with_capacity(2usize);
                        fields
                            .push(
                                ::apache_avro::schema::RecordField::builder()
                                    .name("ITEM".to_string())
                                    .doc(::std::option::Option::None)
                                    .maybe_default(
                                        <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                    )
                                    .aliases(::std::vec::Vec::new())
                                    .schema(
                                        <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                    )
                                    .build(),
                            );
                        fields
                            .push(
                                ::apache_avro::schema::RecordField::builder()
                                    .name("DoubleItem".to_string())
                                    .doc(::std::option::Option::None)
                                    .maybe_default(
                                        <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                                    )
                                    .aliases(::std::vec::Vec::new())
                                    .schema(
                                        <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                            named_schemas,
                                            enclosing_namespace,
                                        ),
                                    )
                                    .build(),
                            );
                        fields
                    };
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
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        ::std::option::Option::Some({
            let mut fields = ::std::vec::Vec::with_capacity(2usize);
            fields
                .push(
                    ::apache_avro::schema::RecordField::builder()
                        .name("ITEM".to_string())
                        .doc(::std::option::Option::None)
                        .maybe_default(
                            <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                        )
                        .aliases(::std::vec::Vec::new())
                        .schema(
                            <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                named_schemas,
                                enclosing_namespace,
                            ),
                        )
                        .build(),
                );
            fields
                .push(
                    ::apache_avro::schema::RecordField::builder()
                        .name("DoubleItem".to_string())
                        .doc(::std::option::Option::None)
                        .maybe_default(
                            <i32 as ::apache_avro::AvroSchemaComponent>::field_default(),
                        )
                        .aliases(::std::vec::Vec::new())
                        .schema(
                            <i32 as ::apache_avro::AvroSchemaComponent>::get_schema_in_ctxt(
                                named_schemas,
                                enclosing_namespace,
                            ),
                        )
                        .build(),
                );
            fields
        })
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
