use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    a: String,
    b: i32,
}

#[derive(AvroSchema)]
#[serde(transparent)]
struct Bar {
    foo: Foo,
}

pub fn main() {}
