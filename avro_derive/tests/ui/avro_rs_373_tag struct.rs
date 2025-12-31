use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[serde(tag = "bar")]
struct Foo {
    a: String,
    b: i32,
}

pub fn main() {}
