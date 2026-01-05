use apache_avro::AvroSchema;

#[derive(AvroSchema)]
enum Foo {
    Bar,
    #[avro(rename = "Scam")]
    Spam,
}

pub fn main() {}
