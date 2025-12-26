use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct T {
    x: Option<i8>,
    y: Option<String>,
    #[serde(skip_serializing)]
    z: Option<i8>,
}

fn main() {}
