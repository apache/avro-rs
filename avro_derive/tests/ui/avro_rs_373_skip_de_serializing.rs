use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct T {
    x: Option<i8>,
    y: Option<String>,
    #[serde(skip_serializing, skip_deserializing)]
    z: Option<i8>,
}

fn main() {}
