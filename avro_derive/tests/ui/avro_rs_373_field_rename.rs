use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    #[avro(rename = "c")]
    a: String,
    b: i32,
}

pub fn main() {}
