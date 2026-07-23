// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Lock Module Tests
//!
//! This module organizes all tests for the lock module,
//! including tests for traits and their implementations.

// Trait tests
#[cfg(feature = "async-lock")]
mod async_data_lock_tests;
#[cfg(feature = "async-lock")]
mod async_lock_tests;
#[cfg(feature = "async-lock")]
mod async_read_lock_tests;
#[cfg(feature = "async-lock")]
mod async_read_write_lock_tests;
#[cfg(feature = "async-lock")]
mod async_write_lock_tests;
mod data_lock_tests;
mod exclusive_lock_tests;
mod lock_tests;
mod mod_tests;
mod read_lock_tests;
mod read_write_lock_tests;
mod write_lock_tests;

// Implementation tests
#[cfg(feature = "parking-lot")]
mod parking_lot_mutex_tests;
#[cfg(feature = "parking-lot")]
mod parking_lot_rw_lock_tests;
mod try_lock_error_tests;
