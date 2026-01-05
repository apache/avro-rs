use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    #[avro(skip)]
    a: String,
    b: i32,
}

pub fn main() {}
