use apache_avro::{AvroSchema, Writer};
use apache_avro_test_helper::TestResult;
use serde::Serialize;

#[test]
fn avro_rs_225_index_out_of_bounds_with_serde_skip_serializing_skip_middle_field() -> TestResult {
    #[derive(Serialize, AvroSchema)]
    struct T {
        x: Option<i8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        y: Option<String>,
        z: Option<i8>,
    }

    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, vec![]);
    writer.append_ser(T {
        x: None,
        y: None,
        z: Some(1),
    })?;
    writer.into_inner()?;
    Ok(())
}

#[test]
fn avro_rs_225_index_out_of_bounds_with_serde_skip_serializing_skip_first_field() -> TestResult {
    #[derive(Serialize, AvroSchema)]
    struct T {
        #[serde(skip_serializing_if = "Option::is_none")]
        x: Option<i8>,
        y: Option<String>,
        z: Option<i8>,
    }

    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, vec![]);
    writer.append_ser(T {
        x: None,
        y: None,
        z: Some(1),
    })?;
    writer.into_inner()?;
    Ok(())
}

#[test]
fn avro_rs_225_index_out_of_bounds_with_serde_skip_serializing_skip_last_field() -> TestResult {
    #[derive(Serialize, AvroSchema)]
    struct T {
        x: Option<i8>,
        y: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        z: Option<i8>,
    }

    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, vec![]);
    writer.append_ser(T {
        x: Some(0),
        y: None,
        z: None,
    })?;
    writer.into_inner()?;
    Ok(())
}

#[test]
fn avro_rs_225_index_out_of_bounds_with_serde_skip_serializing_skip_multiple_fields() -> TestResult
{
    #[derive(Serialize, AvroSchema)]
    struct T {
        x: Option<i8>,
        #[serde(skip_serializing_if = "Option::is_none")]
        y: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        z: Option<i8>,
    }

    let schema = T::get_schema();
    let mut writer = Writer::new(&schema, vec![]);
    writer.append_ser(T {
        x: Some(0),
        y: None,
        z: None,
    })?;
    writer.into_inner()?;
    Ok(())
}
