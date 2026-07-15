// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Notification capability for monitor-style synchronization.

/// Sends notification signals to waiters.
///
/// Notifications have memoryless condition-variable semantics: they do not
/// carry protected state, and callers cannot rely on a signal being retained
/// for a future waiter. A notification selects only waiters that have already
/// registered. Condition waiters therefore recheck the protected state after
/// every wake. A selection belongs only to the selected waiter; if that waiter
/// is subsequently cancelled, the selection is discarded rather than
/// transferred. No notification method guarantees fairness or FIFO selection.
pub trait Notifier {
    /// Selects at most one already registered waiter without a fairness
    /// guarantee.
    fn notify_one(&self);

    /// Selects all already registered waiters without retaining state for
    /// future waiters.
    fn notify_all(&self);
}
