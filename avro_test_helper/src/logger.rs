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

#![cfg(not(target_arch = "wasm32"))]

use crate::LOG_MESSAGES;
use log::{LevelFilter, Log, Metadata};
use std::sync::OnceLock;

struct TestLogger {
    delegate: env_logger::Logger,
}

impl Log for TestLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            LOG_MESSAGES.with(|msgs| msgs.borrow_mut().push(format!("{}", record.args())));

            self.delegate.log(record);
        }
    }

    fn flush(&self) {}
}

fn test_logger() -> &'static TestLogger {
    // Lazy static because the Logger has to be 'static
    static TEST_LOGGER_ONCE: OnceLock<TestLogger> = OnceLock::new();
    TEST_LOGGER_ONCE.get_or_init(|| TestLogger {
        delegate: env_logger::Builder::from_default_env()
            .filter_level(LevelFilter::Off)
            .parse_default_env()
            .build(),
    })
}

/// Clears all log messages of this thread.
pub fn clear_log_messages() {
    LOG_MESSAGES.with_borrow_mut(Vec::clear);
}

/// Asserts that the message is not the last in the log for this thread.
///
/// # Panics
/// Will panic if the provided message is an exact match for the last log message.
///
/// # Example
/// ```should_panic
/// use apache_avro_test_helper::logger::assert_not_logged;
///
/// log::error!("Something went wrong");
/// log::error!("Unexpected Error");
///
/// // This will not panic as it is not an exact match
/// assert_not_logged("No Unexpected Error");
///
/// // This will not panic as it is not the last log message
/// assert_not_logged("Something went wrong");
///
/// // This will panic
/// assert_not_logged("Unexpected Error");
/// ```
#[track_caller]
pub fn assert_not_logged(unexpected_message: &str) {
    LOG_MESSAGES.with_borrow(|msgs| {
        let is_logged = msgs.iter().any(|msg| msg == unexpected_message);
        assert!(
            !is_logged,
            "The following log message should not have been logged: '{unexpected_message}'"
        );
    });
}

/// Asserts that the message has been logged and removes it from the log of this thread.
///
/// # Panics
/// Will panic if the message does not appear in the log.
///
/// # Example
/// ```should_panic
/// use apache_avro_test_helper::logger::assert_logged;
///
/// log::error!("Something went wrong");
/// log::info!("Something happened");
/// log::error!("Something went wrong");
///
/// // This will not panic as the message was logged
/// assert_logged("Something went wrong");
///
/// // This will not panic as the message was logged
/// assert_logged("Something happened");
///
/// // This will not panic as the first call removed only the first matching log message
/// assert_logged("Something went wrong");
///
/// // This will panic as all matching log messages have been removed
/// assert_logged("Something went wrong");
/// ```
#[track_caller]
pub fn assert_logged(expected_message: &str) {
    let mut deleted = false;
    LOG_MESSAGES.with_borrow_mut(|msgs| {
        if let Some(pos) = msgs.iter().position(|msg| msg == expected_message) {
            msgs.remove(pos);
            deleted = true;
        }
    });

    assert!(
        deleted,
        "Expected log message has not been logged: '{expected_message}'"
    );
}

pub(crate) fn install() {
    log::set_logger(test_logger())
        .map(|()| log::set_max_level(LevelFilter::Trace))
        .map_err(|err| {
            eprintln!("Failed to set the custom logger: {err:?}");
        })
        .expect("Failed to set the custom TestLogger");
}
