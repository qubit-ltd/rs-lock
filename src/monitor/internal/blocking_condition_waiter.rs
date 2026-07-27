// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Defines one latched blocking condition waiter and Timer waker.

use std::sync::Arc;
use std::task::{
    Context,
    Poll,
    Wake,
    Waker,
};

use qubit_clock::{
    TimeError,
    TimerFuture,
};

use super::{
    blocking_condition_waiter_state::BlockingConditionWaiterState,
    sync::{
        Condvar,
        Mutex,
        recover,
    },
};

/// Private signal shared by monitor notification and a TimerFuture Waker.
pub(in crate::monitor) struct BlockingConditionWaiter {
    /// Latched notification state.
    state: Mutex<BlockingConditionWaiterState>,
    /// Parks the waiting thread until the signal is latched.
    changed: Condvar,
}

impl BlockingConditionWaiter {
    /// Creates an unsignalled waiter.
    ///
    /// # Returns
    ///
    /// A waiter ready for registry insertion and Timer polling.
    #[must_use]
    #[inline]
    pub(in crate::monitor) fn new() -> Self {
        Self {
            state: Mutex::new(BlockingConditionWaiterState {
                signalled: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Polls a TimerFuture using this waiter as its task Waker.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Shared waiter retained by the generated Waker.
    /// * `future` - Fixed Timer registration to poll.
    ///
    /// # Returns
    ///
    /// [`Poll::Ready`] after the deadline, otherwise [`Poll::Pending`].
    ///
    /// # Errors
    ///
    /// A ready result contains any Timer completion error.
    #[inline]
    pub(in crate::monitor) fn poll_timer(
        waiter: &Arc<Self>,
        future: &mut TimerFuture,
    ) -> Poll<Result<(), TimeError>> {
        let waker = Waker::from(Arc::clone(waiter));
        let mut context = Context::from_waker(&waker);
        future.as_mut().poll(&mut context)
    }

    /// Blocks until monitor notification or the Timer Waker signals this
    /// waiter.
    pub(in crate::monitor) fn wait(&self) {
        let mut state = recover(self.state.lock());
        while !state.signalled {
            state = recover(self.changed.wait(state));
        }
    }

    /// Latches one signal and unparks the blocking thread.
    #[inline]
    fn signal(&self) {
        let mut state = recover(self.state.lock());
        state.signalled = true;
        self.changed.notify_one();
    }
}

impl Wake for BlockingConditionWaiter {
    /// Latches a TimerFuture wake notification.
    #[inline(always)]
    fn wake(self: Arc<Self>) {
        self.signal();
    }

    /// Latches a borrowed TimerFuture wake notification.
    #[inline(always)]
    fn wake_by_ref(self: &Arc<Self>) {
        self.signal();
    }
}
