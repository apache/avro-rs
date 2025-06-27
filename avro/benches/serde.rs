// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use apache_avro::{
    schema::Schema,
    types::{Record, Value},
    AvroResult, Reader, Writer,
};
use criterion::{criterion_group, criterion_main, Criterion};
use serde::Serialize;
use std::time::Duration;

const RAW_SMALL_SCHEMA: &str = r#"
{
  "namespace": "test",
  "type": "record",
  "name": "Test",
  "fields": [
    {
      "type": {
        "type": "string"
      },
      "name": "field"
    }
  ]
}
"#;

#[derive(Serialize, Clone)]
struct SmallRecord {
    field: String,
}

const RAW_BIG_SCHEMA: &str = r#"
{
  "namespace": "my.example",
  "type": "record",
  "name": "userInfo",
  "fields": [
    {
      "default": null,
      "type": ["null", "string"],
      "name": "username"
    },
    {
      "default": -1,
      "type": "int",
      "name": "age"
    },
    {
      "default": null,
      "type": ["null", "string"],
      "name": "phone"
    },
    {
      "default": null,
      "type": ["null", "string"],
      "name": "housenum"
    },
    {
      "default": {},
      "type": {
        "fields": [
          {
            "default": "NONE",
            "type": "string",
            "name": "street"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "city"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "state_prov"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "country"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "zip"
          }
        ],
        "type": "record",
        "name": "mailing_address"
      },
      "name": "address"
    }
  ]
}
"#;

#[derive(Serialize, Clone)]
struct MailingAddress {
    street: String,
    city: String,
    state_prov: String,
    country: String,
    zip: String,
}

#[derive(Serialize, Clone)]
struct BigRecord {
    username: Option<String>,
    age: i32,
    phone: Option<String>,
    housenum: Option<String>,
    address: MailingAddress,
}

const RAW_ADDRESS_SCHEMA: &str = r#"
{
  "fields": [
    {
      "default": "NONE",
      "type": "string",
      "name": "street"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "city"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "state_prov"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "country"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "zip"
    }
  ],
  "type": "record",
  "name": "mailing_address"
}
"#;

fn make_small_record() -> anyhow::Result<(Schema, Value)> {
    let small_schema = Schema::parse_str(RAW_SMALL_SCHEMA)?;
    let small_record = {
        let mut small_record = Record::new(&small_schema).unwrap();
        small_record.put("field", "foo");
        small_record.into()
    };
    Ok((small_schema, small_record))
}

fn make_small_record_ser() -> anyhow::Result<(Schema, SmallRecord)> {
    let small_schema = Schema::parse_str(RAW_SMALL_SCHEMA)?;
    let small_record = SmallRecord {
        field: String::from("foo"),
    };
    Ok((small_schema, small_record))
}

fn make_big_record() -> anyhow::Result<(Schema, Value)> {
    let big_schema = Schema::parse_str(RAW_BIG_SCHEMA)?;
    let address_schema = Schema::parse_str(RAW_ADDRESS_SCHEMA)?;
    let mut address = Record::new(&address_schema).unwrap();
    address.put("street", "street");
    address.put("city", "city");
    address.put("state_prov", "state_prov");
    address.put("country", "country");
    address.put("zip", "zip");

    let big_record = {
        let mut big_record = Record::new(&big_schema).unwrap();
        big_record.put(
            "username",
            Value::Union(1, Box::new(Value::String("username".to_owned()))),
        );
        big_record.put("age", 10i32);
        big_record.put(
            "phone",
            Value::Union(1, Box::new(Value::String("000000000".to_owned()))),
        );
        big_record.put(
            "housenum",
            Value::Union(1, Box::new(Value::String("0000".to_owned()))),
        );
        big_record.put("address", address);
        big_record.into()
    };

    Ok((big_schema, big_record))
}

fn make_big_record_ser() -> anyhow::Result<(Schema, BigRecord)> {
    let big_schema = Schema::parse_str(RAW_BIG_SCHEMA)?;
    let big_record = BigRecord {
        username: Some(String::from("username")),
        age: 10,
        phone: Some(String::from("000000000")),
        housenum: Some(String::from("0000")),
        address: MailingAddress {
            street: String::from("street"),
            city: String::from("city"),
            state_prov: String::from("state_prov"),
            country: String::from("country"),
            zip: String::from("zip"),
        },
    };
    Ok((big_schema, big_record))
}

fn make_records(record: Value, count: usize) -> Vec<Value> {
    std::iter::repeat_n(record, count).collect()
}

fn make_records_ser<T: Serialize + Clone>(record: T, count: usize) -> Vec<T> {
    std::iter::repeat_n(record, count).collect()
}

fn write(schema: &Schema, records: &[Value]) -> AvroResult<Vec<u8>> {
    let mut writer = Writer::new(schema, Vec::new());
    writer.extend_from_slice(records).unwrap();
    writer.into_inner()
}

fn write_ser<T: Serialize>(schema: &Schema, records: &[T]) -> AvroResult<Vec<u8>> {
    let mut writer = Writer::new(schema, Vec::new());
    writer.extend_ser(records)?;
    writer.into_inner()
}

fn read(schema: &Schema, bytes: &[u8]) -> anyhow::Result<()> {
    let reader = Reader::with_schema(schema, bytes)?;

    for record in reader {
        let _ = record?;
    }
    Ok(())
}

fn read_schemaless(bytes: &[u8]) -> anyhow::Result<()> {
    let reader = Reader::new(bytes)?;

    for record in reader {
        let _ = record?;
    }
    Ok(())
}

fn bench_write(
    c: &mut Criterion,
    make_record: impl Fn() -> anyhow::Result<(Schema, Value)>,
    n_records: usize,
    name: &str,
) -> anyhow::Result<()> {
    let (schema, record) = make_record()?;
    let records = make_records(record, n_records);
    c.bench_function(name, |b| b.iter(|| write(&schema, &records)));
    Ok(())
}

fn bench_write_ser<T: Serialize + Clone>(
    c: &mut Criterion,
    make_record: impl Fn() -> anyhow::Result<(Schema, T)>,
    n_records: usize,
    name: &str,
) -> anyhow::Result<()> {
    let (schema, record) = make_record()?;
    let records = make_records_ser(record, n_records);
    c.bench_function(name, |b| b.iter(|| write_ser(&schema, &records)));
    Ok(())
}

fn bench_read(
    c: &mut Criterion,
    make_record: impl Fn() -> anyhow::Result<(Schema, Value)>,
    n_records: usize,
    name: &str,
) -> anyhow::Result<()> {
    let (schema, record) = make_record()?;
    let records = make_records(record, n_records);
    let bytes = write(&schema, &records).unwrap();
    c.bench_function(name, |b| b.iter(|| read(&schema, &bytes)));
    Ok(())
}

fn bench_from_file(c: &mut Criterion, file_path: &str, name: &str) -> anyhow::Result<()> {
    let bytes = std::fs::read(file_path)?;
    c.bench_function(name, |b| b.iter(|| read_schemaless(&bytes)));
    Ok(())
}

fn bench_small_schema_write_1_record(c: &mut Criterion) {
    bench_write(c, make_small_record, 1, "small schema, write 1 record").unwrap();
}

fn bench_small_schema_write_1_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_small_record_ser,
        1,
        "small schema, write 1 record (serde way)",
    )
    .unwrap();
}

fn bench_small_schema_write_100_record(c: &mut Criterion) {
    bench_write(c, make_small_record, 100, "small schema, write 100 records").unwrap();
}

fn bench_small_schema_write_100_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_small_record_ser,
        100,
        "small schema, write 100 records (serde way)",
    )
    .unwrap();
}

fn bench_small_schema_write_10_000_record(c: &mut Criterion) {
    bench_write(
        c,
        make_small_record,
        10_000,
        "small schema, write 10k records",
    )
    .unwrap();
}

fn bench_small_schema_write_10_000_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_small_record_ser,
        10_000,
        "small schema, write 10k records (serde way)",
    )
    .unwrap()
}

fn bench_small_schema_read_1_record(c: &mut Criterion) {
    bench_read(c, make_small_record, 1, "small schema, read 1 record").unwrap();
}

fn bench_small_schema_read_100_record(c: &mut Criterion) {
    bench_read(c, make_small_record, 100, "small schema, read 100 records").unwrap();
}

fn bench_small_schema_read_10_000_record(c: &mut Criterion) {
    bench_read(
        c,
        make_small_record,
        10_000,
        "small schema, read 10k records",
    )
    .unwrap();
}

fn bench_big_schema_write_1_record(c: &mut Criterion) {
    bench_write(c, make_big_record, 1, "big schema, write 1 record").unwrap();
}

fn bench_big_schema_write_1_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_big_record_ser,
        1,
        "big schema, write 1 record (serde way)",
    )
    .unwrap();
}

fn bench_big_schema_write_100_record(c: &mut Criterion) {
    bench_write(c, make_big_record, 100, "big schema, write 100 records").unwrap();
}

fn bench_big_schema_write_100_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_big_record_ser,
        100,
        "big schema, write 100 records (serde way)",
    )
    .unwrap();
}

fn bench_big_schema_write_10_000_record(c: &mut Criterion) {
    bench_write(c, make_big_record, 10_000, "big schema, write 10k records").unwrap();
}

fn bench_big_schema_write_10_000_record_ser(c: &mut Criterion) {
    bench_write_ser(
        c,
        make_big_record_ser,
        10_000,
        "big scheam, write 10k records (serde way)",
    )
    .unwrap();
}

fn bench_big_schema_read_1_record(c: &mut Criterion) {
    bench_read(c, make_big_record, 1, "big schema, read 1 record").unwrap();
}

fn bench_big_schema_read_100_record(c: &mut Criterion) {
    bench_read(c, make_big_record, 100, "big schema, read 100 records").unwrap();
}

fn bench_big_schema_read_10_000_record(c: &mut Criterion) {
    bench_read(c, make_big_record, 10_000, "big schema, read 10k records").unwrap();
}

fn bench_big_schema_read_100_000_record(c: &mut Criterion) {
    bench_read(c, make_big_record, 100_000, "big schema, read 100k records").unwrap();
}

// This benchmark reads from the `benches/quickstop-null.avro` file, which was pulled from
// the `goavro` project benchmarks:
// https://github.com/linkedin/goavro/blob/master/fixtures/quickstop-null.avro
// This was done for the sake of comparing this crate against the `goavro` implementation.
fn bench_file_quickstop_null(c: &mut Criterion) {
    bench_from_file(c, "benches/quickstop-null.avro", "quickstop null file").unwrap();
}

criterion_group!(
    benches,
    bench_small_schema_write_1_record,
    bench_small_schema_write_100_record,
    bench_small_schema_read_1_record,
    bench_small_schema_read_100_record,
    bench_big_schema_write_1_record,
    bench_big_schema_write_100_record,
    bench_big_schema_read_1_record,
    bench_big_schema_read_100_record,
);

criterion_group!(
    benches_ser,
    bench_small_schema_write_1_record_ser,
    bench_small_schema_write_100_record_ser,
    bench_big_schema_write_1_record_ser,
    bench_big_schema_write_100_record_ser,
);

criterion_group!(
    name = long_benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(10));
    targets =
        bench_file_quickstop_null,
        bench_small_schema_write_10_000_record,
        bench_small_schema_read_10_000_record,
        bench_big_schema_read_10_000_record,
        bench_big_schema_write_10_000_record
);

criterion_group!(
  name = long_benches_ser;
  config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(10));
  targets =
    bench_small_schema_write_10_000_record_ser,
    bench_big_schema_write_10_000_record_ser
);

criterion_group!(
    name = very_long_benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(20));
    targets =
        bench_big_schema_read_100_000_record,
);

criterion_main!(
    benches,
    benches_ser,
    long_benches,
    long_benches_ser,
    very_long_benches
);
