use apache_avro::{from_value, Reader};
use serde::{Serialize,Deserialize};
use std::fs::File;
use std::io::BufReader;


// This is the schema that was used to write
// schema = {
//     "type": "record",
//     "name": "SimpleRecord",
//     "fields": [
//         {"name": "data_bytes", "type": ["null", "bytes"], "default": None},
//         {"name": "description", "type": ["null", "string"], "default": None}
//     ]
// }


// Here is an example struct that matches the schema, and another with filtered out byte array field
// The reason this is very useful is that in extremely large deeply nested avro files, structs mapped to grab fields of interest in deserialization
// is really effecient and effective. The issue is that when I'm trying to deserialize a byte array field I get the error below no matter how I approach.
// Bytes enum under value doesn't implement Deserialize in that way so I can't just make it a Value::Bytes

#[derive(Debug, Deserialize, Serialize, Clone)]

struct ExampleByteArray{
    data_bytes: Option<Vec<u8>>,
    description: Option<String>
}


#[derive(Debug, Deserialize, Serialize, Clone)]
struct ExampleByteArrayFiltered{
    description: Option<String>
}

#[test]
fn avro_rs_285_bytes_deserialization_failure(){

    // Load the example file into reader
    let file = File::open("./tests/avro-rs-285-bytes_deserialization.avro".to_string()).unwrap();
    let reader = BufReader::new(file);
    let avro_reader = Reader::new(reader).unwrap();


    // attempt to deserialize into struct with byte array field
    for value in avro_reader{
        let value = value.unwrap();
        let deserialized = from_value::<ExampleByteArray>(&value).unwrap();
        println!("{:?}", deserialized);
    }

}

#[test]
fn avro_rs_285_bytes_deserialization_pass_when_filtered(){

    // Load the example file into reader
    let file = File::open("./tests/avro-rs-285-bytes_deserialization.avro".to_string()).unwrap();
    let reader = BufReader::new(file);
    let avro_reader = Reader::new(reader).unwrap();

    // attempt to deserialize into struct with byte array field filtered out, this will be successful
    for value in avro_reader{
        let value = value.unwrap();
        let deserialized = from_value::<ExampleByteArrayFiltered>(&value).unwrap();
        println!("{:?}", deserialized);
    }

}