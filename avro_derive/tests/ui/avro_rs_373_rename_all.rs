use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[avro(rename_all = "snake_case")]
struct Foo {
    a: String,
    b: i32,
}

pub fn main() {}
