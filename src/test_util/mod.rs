// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Test-only adapters for validating internal state machines.

#[cfg(all(loom, feature = "loom-model"))]
#[doc(hidden)]
pub mod loom;
