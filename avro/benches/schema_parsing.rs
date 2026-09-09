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

use apache_avro::schema::Schema;
use criterion::{Criterion, criterion_group, criterion_main};
use std::time::Duration;

fn read_large_record() -> String {
    std::fs::read_to_string("benches/large_schema.avsc").unwrap()
}

fn read_small_record() -> String {
    std::fs::read_to_string("benches/small_schema.avsc").unwrap()
}

fn bench_large_record(c: &mut Criterion) {
    let string = read_large_record();
    c.bench_function("large record (old)", |b| {
        b.iter(|| Schema::parse_str(&string));
    });
}
fn bench_large_record2(c: &mut Criterion) {
    let string = read_large_record();
    c.bench_function("large record (new)", |b| {
        b.iter(|| Schema::parse_str2(&string));
    });
}
fn bench_small_record(c: &mut Criterion) {
    let string = read_small_record();
    c.bench_function("small record (old)", |b| {
        b.iter(|| Schema::parse_str(&string));
    });
}
fn bench_small_record2(c: &mut Criterion) {
    let string = read_small_record();
    c.bench_function("small record (new)", |b| {
        b.iter(|| Schema::parse_str2(&string));
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(200).measurement_time(Duration::from_secs(10));
    targets =
        bench_small_record,
        bench_small_record2,
        bench_large_record,
        bench_large_record2,
);
criterion_main!(benches);
