// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Loom-facing adapters over production monitor state machines.

mod loom_waiter_registry;

pub use loom_waiter_registry::LoomWaiterRegistry;
