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
    #[inline]
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

/// Clears all log messages stored in the thread-local storage.
///
/// # Panics
/// This function will panic if:
/// - The thread-local `LOG_MESSAGES` is already borrowed and cannot
///   be mutably borrowed.
///
/// The panic includes a debug representation of the error encountered.
pub fn clear_log_messages() {
    LOG_MESSAGES.with_borrow_mut(Vec::clear);
}

/// Asserts that a specific log message has not been logged.
///
/// This function checks the most recently logged message from a thread-local
/// storage and ensures that it does not match the provided `unexpected_message`.
/// If the message does match, the function will panic with an appropriate error
/// message indicating the unexpected log entry.
///
/// # Arguments
/// - `unexpected_message`: A string slice that represents the log message
///   that is not expected to be logged. If this message matches the last
///   logged message, the function will panic.
///
/// # Panics
/// - This function will panic if the `unexpected_message` matches the most
///   recently logged message.
///
/// # Example
/// ```ignore
/// // Assume LOG_MESSAGES is set up to capture log messages.
/// log_message("Unexpected Error");
/// assert_not_logged("Unexpected Error");
/// // This will panic with the message:
/// // "The following log message should not have been logged: 'Unexpected Error'"
///
/// assert_not_logged("Non-existent Message");
/// // This will pass without issue since the message was not logged.
#[track_caller]
pub fn assert_not_logged(unexpected_message: &str) {
    LOG_MESSAGES.with(|msgs| match msgs.borrow().last() {
        Some(last_log) if last_log == unexpected_message => {
            panic!("The following log message should not have been logged: '{unexpected_message}'")
        }
        _ => (),
    });
}

/// Asserts that the specified log message has been logged and removes it from
/// the stored log messages.
///
/// # Parameters
/// - `expected_message`: A string slice that holds the log message to be asserted.
///
/// # Panics
/// This function will panic if the `expected_message` has not been logged. The
/// panic message will indicate the missing log message.
///
/// # Behavior
/// - The function verifies if the `expected_message` exists in the log messages
///   stored using the thread-local storage (`LOG_MESSAGES`).
/// - If the message is found, it is removed from the log messages and the function
///   completes successfully.
/// - If the message is not found, a panic is triggered with an explanatory message.
///
/// # Usage
/// This function is typically used in unit tests or scenarios where it is important
/// to ensure specific messages are logged during execution.
///
/// # Example
/// ```ignore
/// // Assuming LOG_MESSAGES is set up and some log messages have been recorded:
/// assert_logged("Expected log entry");
/// ```
///
/// # Notes
/// - The function uses the `#[track_caller]` attribute to improve debugging by
///   providing more accurate information about the location of the panic in the
///   source code.
/// - The `LOG_MESSAGES` must be a thread-local data structure that supports
///   borrowing and mutating of a collection of string messages.
///
/// # Thread Safety
/// This function relies on thread-local variables to manage logs for each thread
/// independently.
#[track_caller]
pub fn assert_logged(expected_message: &str) {
    let mut deleted = false;
    LOG_MESSAGES.with(|msgs| {
        msgs.borrow_mut().retain(|msg| {
            if msg == expected_message {
                deleted = true;
                false
            } else {
                true
            }
        });
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
