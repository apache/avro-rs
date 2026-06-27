use apache_avro::AvroSchema;
struct AbsoluteUnit;
#[automatically_derived]
impl ::apache_avro::AvroSchemaComponent for AbsoluteUnit {
    fn get_schema_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::apache_avro::schema::Schema {
        let name = ::apache_avro::schema::Name::new_with_enclosing_namespace(
                "AbsoluteUnit",
                enclosing_namespace,
            )
            .expect("Unable to parse `AbsoluteUnit` as a Name");
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
                    .fields(::std::vec::Vec::new())
                    .name(name)
                    .build(),
            )
        }
    }
    fn get_record_fields_in_ctxt(
        named_schemas: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        enclosing_namespace: ::apache_avro::schema::NamespaceRef,
    ) -> ::std::option::Option<::std::vec::Vec<::apache_avro::schema::RecordField>> {
        ::std::option::Option::Some(::std::vec::Vec::new())
    }
    fn field_default() -> ::std::option::Option<::serde_json::Value> {
        ::std::option::Option::None
    }
}
fn main() {}
