use apache_avro::{AvroSchema, Schema, Writer, from_value};
use apache_avro_test_helper::TestResult;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;

fn ser_deser<T>(schema: &Schema, record: T) -> TestResult
where
    T: Serialize + DeserializeOwned + Debug + PartialEq + Clone,
{
    let record2 = record.clone();
    let mut writer = Writer::new(schema, vec![]);
    writer.append_ser(record)?;
    let bytes_written = writer.into_inner()?;

    let reader = apache_avro::Reader::new(&bytes_written[..])?;
    for value in reader {
        let value = value?;
        let deserialized = from_value::<T>(&value)?;
        assert_eq!(deserialized, record2);
    }

    Ok(())
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_middle_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        x: Option<i8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        y: Option<String>,
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: None,
            y: None,
            z: Some(1),
        },
    )
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_first_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        #[serde(skip_serializing_if = "Option::is_none")]
        x: Option<i8>,
        y: Option<String>,
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: None,
            y: None,
            z: Some(1),
        },
    )
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_last_field() -> TestResult {
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        x: Option<i8>,
        y: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: Some(0),
            y: None,
            z: None,
        },
    )
}

#[test]
fn avro_rs_226_index_out_of_bounds_with_serde_skip_serializing_skip_multiple_fields() -> TestResult
{
    #[derive(AvroSchema, Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct T {
        x: Option<i8>,
        #[serde(skip_serializing_if = "Option::is_none", skip)]
        y: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        z: Option<i8>,
    }

    ser_deser::<T>(
        &T::get_schema(),
        T {
            x: Some(0),
            y: None,
            z: None,
        },
    )
}
