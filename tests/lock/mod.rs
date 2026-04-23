/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Lock Module Tests
//!
//! This module organizes all tests for the lock module,
//! including tests for traits and their implementations.

// Trait tests
mod async_lock_tests;
mod lock_tests;
mod monitor_guard_tests;
mod monitor_tests;

// Implementation tests
mod arc_async_mutex_tests;
mod arc_async_rw_lock_tests;
mod arc_monitor_tests;
mod arc_mutex_tests;
mod arc_rw_lock_tests;
mod arc_std_mutex_tests;
mod parking_lot_mutex_tests;
