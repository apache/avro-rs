use apache_avro::AvroSchema;
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum Basic {
    #[default]
    A,
    B,
    C,
    D,
}
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for Basic {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "Basic",
                enclosing_namespace,
            )
            .expect("Unable to parse schema name Basic");
        if named_schemas.contains(&name) {
            ::apache_avro::schema::Schema::Ref {
                name,
            }
        } else {
            let enclosing_namespace = name.namespace();
            named_schemas.insert(name.clone());
            ::apache_avro::schema::Schema::Enum(::apache_avro::schema::EnumSchema {
                name,
                aliases: ::std::option::Option::None,
                doc: ::std::option::Option::None,
                symbols: ::alloc::boxed::box_assume_init_into_vec_unsafe(
                    ::alloc::intrinsics::write_box_via_move(
                        ::alloc::boxed::Box::new_uninit(),
                        ["A".to_owned(), "B".to_owned(), "C".to_owned(), "D".to_owned()],
                    ),
                ),
                default: ::std::option::Option::Some("A".into()),
                attributes: ::std::collections::BTreeMap::new(),
            })
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
#[automatically_derived]
impl ::core::default::Default for Basic {
    #[inline]
    fn default() -> Basic {
        Self::A
    }
}
