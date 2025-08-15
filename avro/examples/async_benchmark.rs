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
    AsyncReader as Reader, AsyncWriter as Writer, Schema,
    schema::tokio::SchemaExt,
    types::{Record, Value},
};
use futures::StreamExt;
use std::time::{Duration, Instant};
use tokio::io::{BufReader, BufWriter};

fn nanos(duration: Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64
}

fn seconds(nanos: u64) -> f64 {
    (nanos as f64) / 1_000_000_000f64
}

/*
fn duration(nanos: u64) -> Duration {
    Duration::new(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
}
*/

async fn benchmark(
    schema: &Schema,
    record: &Value,
    big_or_small: &str,
    count: usize,
    runs: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut records = Vec::new();
    for __ in 0..count {
        records.push(record.clone());
    }

    let mut durations = Vec::with_capacity(runs);

    let mut bytes = None;
    for _ in 0..runs {
        let records = records.clone();

        let start = Instant::now();
        let mut writer = Writer::new(schema, BufWriter::new(Vec::new()));
        writer.extend(records).await?;

        let duration = Instant::now().duration_since(start);
        durations.push(duration);

        bytes = Some(writer.into_inner().await?.into_inner());
    }

    let total_duration_write = durations.into_iter().fold(0u64, |a, b| a + nanos(b));

    // println!("Write: {} {} {:?}", count, runs, seconds(total_duration));

    let bytes = bytes.unwrap();

    let mut durations = Vec::with_capacity(runs);

    for _ in 0..runs {
        let start = Instant::now();
        let mut reader = Reader::with_schema(schema, BufReader::new(&bytes[..])).await?;

        let mut read_records = Vec::with_capacity(count);
        while let Some(record) = reader.next().await {
            read_records.push(record);
        }

        let duration = Instant::now().duration_since(start);
        durations.push(duration);

        assert_eq!(count, read_records.len());
    }

    let total_duration_read = durations.into_iter().fold(0u64, |a, b| a + nanos(b));

    // println!("Read: {} {} {:?}", count, runs, seconds(total_duration));
    let (total_write_secs, total_read_secs) =
        (seconds(total_duration_write), seconds(total_duration_read));

    println!("{count}\t\t{runs}\t\t{big_or_small}\t\t{total_write_secs}\t\t{total_read_secs}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let raw_small_schema = r#"
        {"namespace": "test", "type": "record", "name": "Test", "fields": [{"type": {"type": "string"}, "name": "field"}]}
    "#;

    let raw_big_schema = r#"
        {"namespace": "my.example", "type": "record", "name": "userInfo", "fields": [{"default": "NONE", "type": "string", "name": "username"}, {"default": -1, "type": "int", "name": "age"}, {"default": "NONE", "type": "string", "name": "phone"}, {"default": "NONE", "type": "string", "name": "housenum"}, {"default": {}, "type": {"fields": [{"default": "NONE", "type": "string", "name": "street"}, {"default": "NONE", "type": "string", "name": "city"}, {"default": "NONE", "type": "string", "name": "state_prov"}, {"default": "NONE", "type": "string", "name": "country"}, {"default": "NONE", "type": "string", "name": "zip"}], "type": "record", "name": "mailing_address"}, "name": "address"}]}
    "#;

    let small_schema = SchemaExt::parse_str(raw_small_schema).await?;
    let big_schema = SchemaExt::parse_str(raw_big_schema).await?;

    println!("{small_schema:?}");
    println!("{big_schema:?}");

    let mut small_record = Record::new(&small_schema).unwrap();
    small_record.put("field", "foo");
    let small_record = small_record.into();

    let raw_address_schema = r#"{"fields": [{"default": "NONE", "type": "string", "name": "street"}, {"default": "NONE", "type": "string", "name": "city"}, {"default": "NONE", "type": "string", "name": "state_prov"}, {"default": "NONE", "type": "string", "name": "country"}, {"default": "NONE", "type": "string", "name": "zip"}], "type": "record", "name": "mailing_address"}"#;
    let address_schema = SchemaExt::parse_str(raw_address_schema).await?;
    let mut address = Record::new(&address_schema).unwrap();
    address.put("street", "street");
    address.put("city", "city");
    address.put("state_prov", "state_prov");
    address.put("country", "country");
    address.put("zip", "zip");

    let mut big_record = Record::new(&big_schema).unwrap();
    big_record.put("username", "username");
    big_record.put("age", 10i32);
    big_record.put("phone", "000000000");
    big_record.put("housenum", "0000");
    big_record.put("address", address);
    let big_record = big_record.into();

    println!();
    println!("Count\t\tRuns\t\tBig/Small\tTotal write secs\tTotal read secs");

    benchmark(&small_schema, &small_record, "Small", 10_000, 1).await?;
    benchmark(&big_schema, &big_record, "Big", 10_000, 1).await?;

    benchmark(&small_schema, &small_record, "Small", 1, 100_000).await?;
    benchmark(&small_schema, &small_record, "Small", 100, 1000).await?;
    benchmark(&small_schema, &small_record, "Small", 10_000, 10).await?;

    benchmark(&big_schema, &big_record, "Big", 1, 100_000).await?;
    benchmark(&big_schema, &big_record, "Big", 100, 1000).await?;
    benchmark(&big_schema, &big_record, "Big", 10_000, 10).await?;

    Ok(())
}
