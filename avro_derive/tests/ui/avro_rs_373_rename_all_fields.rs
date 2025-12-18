use apache_avro::AvroSchema;

#[derive(AvroSchema)]
#[serde(rename_all_fields = "UPPERCASE")]
enum Foo {
    Bar {
        a: String,
        b: i32,
    }
}

pub fn main() {}
