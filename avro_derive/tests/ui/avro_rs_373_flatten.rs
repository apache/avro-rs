use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct Foo {
    #[avro(flatten)]
    a: Bar,
    b: i32,
}

#[derive(AvroSchema)]
struct Bar {
    a: String,
}

pub fn main() {}
