use apache_avro::AvroSchema;

#[derive(AvroSchema)]
struct T {
    x: Option<i8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    y: Option<String>,
    z: Option<i8>,
}

fn main() {}
