/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Asynchronous Lock Trait
//!
//! Defines an asynchronous lock abstraction that supports acquiring
//! locks without blocking threads.
//!
//! # Author
//!
//! Haixing Hu
use std::future::Future;

use tokio::sync::{
    Mutex as AsyncMutex,
    RwLock as AsyncRwLock,
};

use super::try_lock_error::TryLockError;

/// Unified asynchronous lock trait
///
/// Provides a unified interface for different types of asynchronous
/// locks, supporting both read and write operations. This trait allows
/// locks to be used in async contexts through closures, avoiding the
/// complexity of explicitly managing lock guards and their lifetimes.
///
/// # Design Philosophy
///
/// This trait unifies both exclusive async locks (like `tokio::sync::Mutex`)
/// and read-write async locks (like `tokio::sync::RwLock`) under a single
/// interface. The key insight is that all async locks can be viewed as
/// supporting two operations:
///
/// - **Read operations**: Provide immutable access (`&T`) to the data
/// - **Write operations**: Provide mutable access (`&mut T`) to the data
///
/// For exclusive async locks (Mutex), both read and write operations
/// acquire the same exclusive lock, but the API clearly indicates the
/// intended usage. For read-write async locks (RwLock), read operations
/// use shared locks while write operations use exclusive locks.
///
/// This design enables:
/// - Unified API across different async lock types
/// - Clear semantic distinction between read and write operations
/// - Generic async code that works with any lock type
/// - Performance optimization through appropriate lock selection
/// - Non-blocking async operations
///
/// Only lock acquisition is asynchronous. The closure passed to `read` or
/// `write` executes synchronously while the guard is held, so it cannot
/// `.await` and should not perform blocking or long-running work. Compute
/// expensive values before acquiring the lock, or move blocking work to a
/// dedicated blocking task and keep the locked closure short.
///
/// # Performance Characteristics
///
/// Different async lock implementations have different performance
/// characteristics:
///
/// ## Mutex-based async locks (ArcAsyncMutex, AsyncMutex)
/// - `read`: Acquires exclusive lock, same performance as write
/// - `write`: Acquires exclusive lock, same performance as read
/// - **Use case**: When you need exclusive access or don't know access
///   patterns
///
/// ## RwLock-based async locks (ArcAsyncRwLock, AsyncRwLock)
/// - `read`: Acquires shared lock, allows concurrent readers
/// - `write`: Acquires exclusive lock, blocks all other operations
/// - **Use case**: Read-heavy async workloads where multiple readers can
///   proceed concurrently
///
/// # Type Parameters
///
/// * `T` - The type of data protected by the lock
///
/// # Author
///
/// Haixing Hu
pub trait AsyncLock<T: ?Sized> {
    /// Acquires a read lock asynchronously and executes a closure
    ///
    /// This method awaits until a read lock can be acquired without
    /// blocking the thread, then executes the provided closure with
    /// immutable access to the protected data. For exclusive async
    /// locks (Mutex), this acquires the same exclusive lock as write
    /// operations. For read-write async locks (RwLock), this acquires
    /// a shared lock allowing concurrent readers.
    ///
    /// # Use Cases
    ///
    /// - **Data inspection**: Reading values, checking state, validation
    /// - **Read-only operations**: Computing derived values, formatting
    ///   output
    /// - **Condition checking**: Evaluating predicates without modification
    /// - **Logging and debugging**: Accessing data for diagnostic purposes
    ///
    /// # Performance Notes
    ///
    /// - **Mutex-based async locks**: Same performance as write operations
    /// - **RwLock-based async locks**: Allows concurrent readers, better
    ///   for read-heavy async workloads
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives an immutable reference (`&T`) to
    ///   the protected data
    ///
    /// # Returns
    ///
    /// Returns a future that resolves to the result produced by the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{AsyncLock, ArcAsyncRwLock};
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread()
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// rt.block_on(async {
    ///     let lock = ArcAsyncRwLock::new(vec![1, 2, 3]);
    ///
    ///     // Read operation - allows concurrent readers with RwLock
    ///     let len = lock.read(|data| data.len()).await;
    ///     assert_eq!(len, 3);
    ///
    ///     // Multiple concurrent readers possible with RwLock
    ///     let sum = lock.read(|data|
    ///         data.iter().sum::<i32>()
    ///     ).await;
    ///     assert_eq!(sum, 6);
    /// });
    /// ```
    fn read<R, F>(&self, f: F) -> impl Future<Output = R> + Send
    where
        F: FnOnce(&T) -> R + Send,
        R: Send;

    /// Acquires a write lock asynchronously and executes a closure
    ///
    /// This method awaits until a write lock can be acquired without
    /// blocking the thread, then executes the provided closure with
    /// mutable access to the protected data. For all async lock types,
    /// this acquires an exclusive lock that blocks all other operations
    /// until the closure completes.
    ///
    /// # Use Cases
    ///
    /// - **Data modification**: Updating values, adding/removing elements
    /// - **State changes**: Transitioning between different states
    /// - **Initialization**: Setting up data structures
    /// - **Cleanup operations**: Releasing resources, resetting state
    ///
    /// # Performance Notes
    ///
    /// - **All async lock types**: Exclusive access, blocks all other
    ///   operations
    /// - **RwLock advantage**: Only blocks during actual writes, not reads
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference (`&mut T`) to
    ///   the protected data
    ///
    /// # Returns
    ///
    /// Returns a future that resolves to the result produced by the closure
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{AsyncLock, ArcAsyncRwLock};
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread()
    ///     .enable_all()
    ///     .build()
    ///     .unwrap();
    /// rt.block_on(async {
    ///     let lock = ArcAsyncRwLock::new(vec![1, 2, 3]);
    ///
    ///     // Write operation - exclusive access
    ///     lock.write(|data| {
    ///         data.push(4);
    ///         data.sort();
    ///     }).await;
    ///
    ///     // Verify the changes
    ///     let result = lock.read(|data| data.clone()).await;
    ///     assert_eq!(result, vec![1, 2, 3, 4]);
    /// });
    /// ```
    fn write<R, F>(&self, f: F) -> impl Future<Output = R> + Send
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send;

    /// Attempts to acquire a read lock without waiting
    ///
    /// This method tries to acquire a read lock immediately. If the lock
    /// cannot be acquired, it returns [`TryLockError::WouldBlock`] without
    /// waiting. Otherwise, it executes the closure and returns `Ok` containing
    /// the result.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives an immutable reference (`&T`) to
    ///   the protected data if the lock is successfully acquired
    ///
    /// # Returns
    ///
    /// * `Ok(R)` - If the lock was acquired and closure executed
    /// * `Err(TryLockError::WouldBlock)` - If the lock is currently held in write mode
    ///
    /// # Errors
    ///
    /// Returns [`TryLockError::WouldBlock`] when the async lock cannot be
    /// acquired immediately. Tokio locks are not poisoned, so this method does
    /// not return [`TryLockError::Poisoned`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{AsyncLock, ArcAsyncRwLock};
    ///
    /// let lock = ArcAsyncRwLock::new(42);
    /// if let Ok(value) = lock.try_read(|data| *data) {
    ///     println!("Got value: {}", value);
    /// } else {
    ///     println!("Lock is busy with write operation");
    /// }
    /// ```
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R;

    /// Attempts to acquire a write lock without waiting
    ///
    /// This method tries to acquire a write lock immediately. If the lock
    /// is currently held by another task (in either read or write mode),
    /// it returns [`TryLockError::WouldBlock`] without waiting. Otherwise, it
    /// executes the closure and returns `Ok` containing the result.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure that receives a mutable reference (`&mut T`) to
    ///   the protected data if the lock is successfully acquired
    ///
    /// # Returns
    ///
    /// * `Ok(R)` - If the lock was acquired and closure executed
    /// * `Err(TryLockError::WouldBlock)` - If the lock is currently held by another task
    ///
    /// # Errors
    ///
    /// Returns [`TryLockError::WouldBlock`] when the async lock cannot be
    /// acquired immediately. Tokio locks are not poisoned, so this method does
    /// not return [`TryLockError::Poisoned`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use qubit_lock::lock::{AsyncLock, ArcAsyncMutex};
    ///
    /// let lock = ArcAsyncMutex::new(42);
    /// if let Ok(result) = lock.try_write(|data| {
    ///     *data += 1;
    ///     *data
    /// }) {
    ///     println!("New value: {}", result);
    /// } else {
    ///     println!("Lock is busy");
    /// }
    /// ```
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R;
}

/// Asynchronous mutex implementation for tokio::sync::Mutex
///
/// This implementation uses Tokio's `Mutex` type to provide an
/// asynchronous lock that can be awaited without blocking threads.
/// Both read and write operations acquire the same exclusive lock,
/// ensuring thread safety at the cost of concurrent access.
///
/// # Type Parameters
///
/// * `T` - The type of data protected by the lock
///
/// # Author
///
/// Haixing Hu
impl<T: ?Sized + Send> AsyncLock<T> for AsyncMutex<T> {
    /// Acquires the mutex and executes a read-only closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    #[inline]
    async fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R + Send,
        R: Send,
    {
        let guard = self.lock().await;
        f(&*guard)
    }

    /// Acquires the mutex and executes a mutable closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    #[inline]
    async fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        let mut guard = self.lock().await;
        f(&mut *guard)
    }

    /// Attempts to acquire the mutex without waiting for a read-only closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access when the mutex is acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if the mutex is acquired, or
    /// [`TryLockError::WouldBlock`] if it is busy.
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R,
    {
        self.try_lock()
            .map(|guard| f(&*guard))
            .map_err(|_| TryLockError::WouldBlock)
    }

    /// Attempts to acquire the mutex without waiting for a mutable closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when the mutex is acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if the mutex is acquired, or
    /// [`TryLockError::WouldBlock`] if it is busy.
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.try_lock()
            .map(|mut guard| f(&mut *guard))
            .map_err(|_| TryLockError::WouldBlock)
    }
}

/// Asynchronous read-write lock implementation for tokio::sync::RwLock
///
/// This implementation uses Tokio's `RwLock` type to provide an
/// asynchronous read-write lock that supports multiple concurrent
/// readers or a single writer without blocking threads. Read operations
/// use shared locks allowing concurrent readers, while write operations
/// use exclusive locks that block all other operations.
///
/// # Type Parameters
///
/// * `T` - The type of data protected by the lock
///
/// # Author
///
/// Haixing Hu
impl<T: ?Sized + Send + Sync> AsyncLock<T> for AsyncRwLock<T> {
    /// Acquires a shared read lock and executes a closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    #[inline]
    async fn read<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R + Send,
        R: Send,
    {
        let guard = self.read().await;
        f(&*guard)
    }

    /// Acquires an exclusive write lock and executes a closure.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access to the protected value.
    ///
    /// # Returns
    ///
    /// A future resolving to the value returned by `f`.
    #[inline]
    async fn write<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R + Send,
        R: Send,
    {
        let mut guard = self.write().await;
        f(&mut *guard)
    }

    /// Attempts to acquire a shared read lock without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving immutable access when the read lock is
    ///   acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a read lock is acquired, or
    /// [`TryLockError::WouldBlock`] if it is busy.
    #[inline]
    fn try_read<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&T) -> R,
    {
        self.try_read()
            .map(|guard| f(&*guard))
            .map_err(|_| TryLockError::WouldBlock)
    }

    /// Attempts to acquire an exclusive write lock without waiting.
    ///
    /// # Arguments
    ///
    /// * `f` - Closure receiving mutable access when the write lock is
    ///   acquired.
    ///
    /// # Returns
    ///
    /// `Ok(result)` if a write lock is acquired, or
    /// [`TryLockError::WouldBlock`] if it is busy.
    #[inline]
    fn try_write<R, F>(&self, f: F) -> Result<R, TryLockError>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.try_write()
            .map(|mut guard| f(&mut *guard))
            .map_err(|_| TryLockError::WouldBlock)
    }
}
