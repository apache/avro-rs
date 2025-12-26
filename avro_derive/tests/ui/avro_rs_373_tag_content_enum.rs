use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[serde(tag = "bar", content = "spam")]
enum Foo {
    One,
    Two,
}

pub fn main() {}
