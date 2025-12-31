use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[serde(tag = "bar")]
enum Foo {
    One,
    Two,
}

pub fn main() {}
