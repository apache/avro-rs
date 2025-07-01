use serde::Deserialize;

#[test]
fn avro_rs_219_failing_deserialization_due_to_bigdecimal_dependency() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct S3 {
        f1: Option<f64>,

        #[serde(flatten)]
        inner: Inner,
    }

    #[derive(Deserialize, PartialEq, Debug)]
    struct Inner {
        f2: f64,
    }

    let test = r#"{
      "f1": 0.3,
      "f2": 3.76
    }"#;

    let result = serde_json::from_str::<S3>(test);
    println!("result : {result:#?}");
    result.unwrap();
}
