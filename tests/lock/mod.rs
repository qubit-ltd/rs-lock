/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Lock Module Tests
//!
//! This module organizes all tests for the lock module,
//! including tests for traits and their implementations.

// Trait tests
#[cfg(feature = "async")]
mod async_lock_tests;
mod lock_tests;

// Implementation tests
#[cfg(feature = "async")]
mod arc_async_mutex_tests;
#[cfg(feature = "async")]
mod arc_async_rw_lock_tests;
mod arc_mutex_tests;
mod arc_rw_lock_tests;
mod arc_std_mutex_tests;
mod parking_lot_mutex_tests;
mod try_lock_error_tests;
