use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[serde(untagged)]
enum Foo {
    One,
    Two,
}

pub fn main() {}
