use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    #[avro(alias = "c")]
    a: String,
    b: i32,
}

pub fn main() {}
