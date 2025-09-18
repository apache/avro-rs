use serde::{Deserialize, Serialize};

// Here is an example struct that matches the schema, and another with filtered out byte array field
// The reason this is very useful is that in extremely large deeply nested avro files, structs mapped to grab fields of interest in deserialization
// is really effecient and effective. The issue is that when I'm trying to deserialize a byte array field I get the error below no matter how I approach.
// Bytes enum under value doesn't implement Deserialize in that way so I can't just make it a Value::Bytes

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ExampleByteArray {
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    data_bytes: Option<Vec<u8>>,
    description: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ExampleByteArrayFiltered {
    description: Option<String>,
}

#[test]
fn avro_rs_285_bytes_deserialization_round_trip() {
    // define schema
    let raw_schema = r#"
    {
        "type": "record",
        "name": "SimpleRecord",
        "fields": [
            {"name": "data_bytes", "type": ["null", "bytes"], "default": null},
            {"name": "description", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

    // create vector of ExampleByteArray
    let records = vec![
        ExampleByteArray {
            data_bytes: Some(vec![1, 2, 3, 4, 5]),
            description: Some("First record".to_string()),
        },
        ExampleByteArray {
            data_bytes: None,
            description: Some("Second record".to_string()),
        },
        ExampleByteArray {
            data_bytes: Some(vec![10, 20, 30]),
            description: None,
        },
    ];

    // serialize records to Avro binary format with schema
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for record in &records {
        writer.append_ser(record).unwrap();
    }

    let avro_data = writer.into_inner().unwrap();

    // deserialize Avro binary data back into ExampleByteArray structs
    let reader = apache_avro::Reader::new(&avro_data[..]).unwrap();
    let _deserialized_records: Vec<ExampleByteArray> = reader
        .map(|value| apache_avro::from_value::<ExampleByteArray>(&value.unwrap()).unwrap())
        .collect();
}

#[test]
fn avro_rs_285_bytes_deserialization_filtered_round_trip() {
    // define schema
    let raw_schema = r#"
    {
        "type": "record",
        "name": "SimpleRecord",
        "fields": [
            {"name": "data_bytes", "type": ["null", "bytes"], "default": null},
            {"name": "description", "type": ["null", "string"], "default": null}
        ]
    }
    "#;

    let schema = apache_avro::Schema::parse_str(raw_schema).unwrap();

    // create vector of ExampleByteArray
    let records = vec![
        ExampleByteArray {
            data_bytes: Some(vec![1, 2, 3, 4, 5]),
            description: Some("First record".to_string()),
        },
        ExampleByteArray {
            data_bytes: None,
            description: Some("Second record".to_string()),
        },
        ExampleByteArray {
            data_bytes: Some(vec![10, 20, 30]),
            description: None,
        },
    ];

    // serialize records to Avro binary format with schema
    let mut writer = apache_avro::Writer::new(&schema, Vec::new());
    for record in &records {
        writer.append_ser(record).unwrap();
    }

    let avro_data = writer.into_inner().unwrap();

    // deserialize Avro binary data back into ExampleByteArray structs
    let reader = apache_avro::Reader::new(&avro_data[..]).unwrap();
    let _deserialized_records: Vec<ExampleByteArrayFiltered> = reader
        .map(|value| apache_avro::from_value::<ExampleByteArrayFiltered>(&value.unwrap()).unwrap())
        .collect();
}
