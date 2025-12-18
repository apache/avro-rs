use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    a: String,
    b: i32,
}

#[derive(AvroSchema)]
#[serde(remote = "Foo")]
struct FooRemote {
    a: String,
    b: i32,
}

pub fn main() {}
