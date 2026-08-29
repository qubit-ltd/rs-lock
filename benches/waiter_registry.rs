// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compares the production BTreeMap waiter index with former index strategies.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::btree_map::IntoValues;
use std::hint::black_box;

use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_collections::map::OrderedIndexMap;

/// Registry sizes used to expose order-index maintenance costs.
const WAITER_COUNTS: [usize; 4] = [32, 128, 512, 2_048];

/// FIFO waiter index matching the former production OrderedIndexMap strategy.
struct OrderedIndexWaiters {
    /// Next nonzero registration identifier.
    next_waiter_id: u64,
    /// Values indexed by registration identifier and attachment order.
    waiters: OrderedIndexMap<u64, (), usize>,
}

/// FIFO waiter index matching the production single-BTreeMap strategy.
struct BTreeWaiters {
    /// Next nonzero registration identifier.
    next_waiter_id: u64,
    /// Values indexed by monotonically increasing FIFO registration
    /// identifier.
    waiters: BTreeMap<u64, usize>,
}

impl BTreeWaiters {
    /// Creates an empty FIFO waiter index.
    ///
    /// # Returns
    ///
    /// An index ready to register benchmark waiter values.
    fn new() -> Self {
        Self {
            next_waiter_id: 1,
            waiters: BTreeMap::new(),
        }
    }

    /// Registers `waiter` and returns its cancellation identifier.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Benchmark value made eligible for notification.
    ///
    /// # Returns
    ///
    /// The identifier required to cancel `waiter`.
    fn register(&mut self, waiter: usize) -> u64 {
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id = waiter_id
            .checked_add(1)
            .expect("benchmark waiter identifiers should not exhaust");
        let _ = self.waiters.insert(waiter_id, waiter);
        waiter_id
    }

    /// Removes and returns the longest-waiting registered value.
    ///
    /// # Returns
    ///
    /// The selected value, or `None` when no waiter remains.
    fn take_one(&mut self) -> Option<usize> {
        self.waiters.pop_first().map(|(_, waiter)| waiter)
    }

    /// Removes every waiter in FIFO order.
    ///
    /// # Returns
    ///
    /// An owning iterator over values active when the operation began.
    fn take_all(&mut self) -> IntoValues<u64, usize> {
        // Match production by moving the tree directly into iteration so the
        // benchmark measures the removal of the intermediate Vec allocation.
        std::mem::take(&mut self.waiters).into_values()
    }

    /// Removes every waiter through the former intermediate-Vec strategy.
    ///
    /// # Returns
    ///
    /// Values active when the operation began.
    fn take_all_via_vec(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.waiters).into_values().collect()
    }

    /// Cancels the waiter associated with `waiter_id`.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Registration identifier returned by [`Self::register`].
    ///
    /// # Returns
    ///
    /// The cancelled value, or `None` when it was already selected.
    fn unregister(&mut self, waiter_id: u64) -> Option<usize> {
        self.waiters.remove(&waiter_id)
    }
}

impl OrderedIndexWaiters {
    /// Creates an empty FIFO waiter index.
    ///
    /// # Returns
    ///
    /// An index ready to register benchmark waiter values.
    fn new() -> Self {
        Self {
            next_waiter_id: 1,
            waiters: OrderedIndexMap::new(),
        }
    }

    /// Registers `waiter` and returns its cancellation identifier.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Benchmark value made eligible for notification.
    ///
    /// # Returns
    ///
    /// The identifier required to cancel `waiter`.
    fn register(&mut self, waiter: usize) -> u64 {
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id = waiter_id
            .checked_add(1)
            .expect("benchmark waiter identifiers should not exhaust");
        let _ = self.waiters.try_insert(waiter_id, (), waiter);
        waiter_id
    }

    /// Removes and returns the longest-waiting registered value.
    ///
    /// # Returns
    ///
    /// The selected value, or `None` when no waiter remains.
    fn take_one(&mut self) -> Option<usize> {
        self.waiters.pop_first().map(|entry| entry.into_value())
    }

    /// Removes every waiter in FIFO order.
    ///
    /// # Returns
    ///
    /// Values active when the operation began.
    fn take_all(&mut self) -> Vec<usize> {
        let mut waiters = Vec::with_capacity(self.waiters.len());
        while let Some(waiter) = self.take_one() {
            waiters.push(waiter);
        }
        waiters
    }

    /// Cancels the waiter associated with `waiter_id`.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Registration identifier returned by [`Self::register`].
    ///
    /// # Returns
    ///
    /// The cancelled value, or `None` when it was already selected.
    fn unregister(&mut self, waiter_id: u64) -> Option<usize> {
        self.waiters.remove(&waiter_id).map(|entry| entry.into_value())
    }
}

/// FIFO waiter index using separate order and identifier maps.
struct BTreeHashWaiters {
    /// Next nonzero registration identifier.
    next_waiter_id: u64,
    /// Values indexed by cancellation identifier.
    values: HashMap<u64, usize>,
    /// FIFO order keyed by monotonically increasing registration identifier.
    order: BTreeMap<u64, ()>,
}

impl BTreeHashWaiters {
    /// Creates an empty dual-map FIFO waiter index.
    ///
    /// # Returns
    ///
    /// An index ready to register benchmark waiter values.
    fn new() -> Self {
        Self {
            next_waiter_id: 1,
            values: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    /// Registers `waiter` and returns its cancellation identifier.
    ///
    /// # Parameters
    ///
    /// * `waiter` - Benchmark value made eligible for notification.
    ///
    /// # Returns
    ///
    /// The identifier required to cancel `waiter`.
    fn register(&mut self, waiter: usize) -> u64 {
        let waiter_id = self.next_waiter_id;
        self.next_waiter_id = waiter_id
            .checked_add(1)
            .expect("benchmark waiter identifiers should not exhaust");
        let _ = self.values.insert(waiter_id, waiter);
        let _ = self.order.insert(waiter_id, ());
        waiter_id
    }

    /// Removes and returns the longest-waiting registered value.
    ///
    /// # Returns
    ///
    /// The selected value, or `None` when no waiter remains.
    fn take_one(&mut self) -> Option<usize> {
        self.order
            .pop_first()
            .and_then(|(waiter_id, _)| self.values.remove(&waiter_id))
    }

    /// Removes every waiter in FIFO order.
    ///
    /// # Returns
    ///
    /// Values active when the operation began.
    fn take_all(&mut self) -> Vec<usize> {
        let mut waiters = Vec::with_capacity(self.values.len());
        while let Some(waiter) = self.take_one() {
            waiters.push(waiter);
        }
        waiters
    }

    /// Cancels the waiter associated with `waiter_id`.
    ///
    /// # Parameters
    ///
    /// * `waiter_id` - Registration identifier returned by [`Self::register`].
    ///
    /// # Returns
    ///
    /// The cancelled value, or `None` when it was already selected.
    fn unregister(&mut self, waiter_id: u64) -> Option<usize> {
        let waiter = self.values.remove(&waiter_id);
        if waiter.is_some() {
            let _ = self.order.remove(&waiter_id);
        }
        waiter
    }
}

/// Validates each benchmark registry before Criterion starts timing it.
fn validate_waiter_implementations() {
    let mut ordered = OrderedIndexWaiters::new();
    assert_eq!(ordered.register(10), 1);
    assert_eq!(ordered.register(20), 2);
    assert_eq!(ordered.take_one(), Some(10));
    assert_eq!(ordered.unregister(2), Some(20));

    let mut btree = BTreeWaiters::new();
    assert_eq!(btree.register(10), 1);
    assert_eq!(btree.register(20), 2);
    assert_eq!(btree.take_one(), Some(10));
    assert_eq!(btree.unregister(2), Some(20));

    let mut btree_hash = BTreeHashWaiters::new();
    assert_eq!(btree_hash.register(10), 1);
    assert_eq!(btree_hash.register(20), 2);
    assert_eq!(btree_hash.take_one(), Some(10));
    assert_eq!(btree_hash.unregister(2), Some(20));
    assert!(btree_hash.values.is_empty());
    assert!(btree_hash.order.is_empty());
}

/// Builds an OrderedIndexMap fixture outside the measured operation.
///
/// # Parameters
///
/// * `waiter_count` - Number of registered values in the fixture.
///
/// # Returns
///
/// A populated waiter index with identifiers from one through `waiter_count`.
fn prepare_ordered_index_waiters(waiter_count: usize) -> OrderedIndexWaiters {
    let mut waiters = OrderedIndexWaiters::new();
    for waiter in 0..waiter_count {
        waiters.register(waiter);
    }
    waiters
}

/// Builds a single-BTreeMap fixture outside the measured operation.
///
/// # Parameters
///
/// * `waiter_count` - Number of registered values in the fixture.
///
/// # Returns
///
/// A populated waiter index with identifiers from one through `waiter_count`.
fn prepare_btree_waiters(waiter_count: usize) -> BTreeWaiters {
    let mut waiters = BTreeWaiters::new();
    for waiter in 0..waiter_count {
        waiters.register(waiter);
    }
    waiters
}

/// Builds a BTreeMap and HashMap fixture outside the measured operation.
///
/// # Parameters
///
/// * `waiter_count` - Number of registered values in the fixture.
///
/// # Returns
///
/// A populated waiter index with identifiers from one through `waiter_count`.
fn prepare_btree_hash_waiters(waiter_count: usize) -> BTreeHashWaiters {
    let mut waiters = BTreeHashWaiters::new();
    for waiter in 0..waiter_count {
        waiters.register(waiter);
    }
    waiters
}

/// Benchmarks registration, cancellation, and notification selection paths.
///
/// # Parameters
///
/// * `criterion` - Criterion runner receiving each registry workload.
fn benchmark_waiter_registry(criterion: &mut Criterion) {
    validate_waiter_implementations();
    let mut group = criterion.benchmark_group("waiter_registry");
    for waiter_count in WAITER_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("ordered_index/register", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    OrderedIndexWaiters::new,
                    |waiters| {
                        for waiter in 0..waiter_count {
                            black_box(waiters.register(waiter));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree/register", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    BTreeWaiters::new,
                    |waiters| {
                        for waiter in 0..waiter_count {
                            black_box(waiters.register(waiter));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree_hash/register", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    BTreeHashWaiters::new,
                    |waiters| {
                        for waiter in 0..waiter_count {
                            black_box(waiters.register(waiter));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordered_index/cancel_reverse", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_ordered_index_waiters(waiter_count),
                    |waiters| {
                        for waiter_id in (1..=waiter_count as u64).rev() {
                            black_box(waiters.unregister(waiter_id));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree/cancel_reverse", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_waiters(waiter_count),
                    |waiters| {
                        for waiter_id in (1..=waiter_count as u64).rev() {
                            black_box(waiters.unregister(waiter_id));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree_hash/cancel_reverse", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_hash_waiters(waiter_count),
                    |waiters| {
                        for waiter_id in (1..=waiter_count as u64).rev() {
                            black_box(waiters.unregister(waiter_id));
                        }
                        black_box(waiters);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordered_index/notify_one", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_ordered_index_waiters(waiter_count),
                    |waiters| black_box(waiters.take_one()),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree/notify_one", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_waiters(waiter_count),
                    |waiters| black_box(waiters.take_one()),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree_hash/notify_one", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_hash_waiters(waiter_count),
                    |waiters| black_box(waiters.take_one()),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("ordered_index/notify_all", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_ordered_index_waiters(waiter_count),
                    |waiters| black_box(waiters.take_all()),
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree_vec/notify_all", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_waiters(waiter_count),
                    |waiters| {
                        // Consume every value just like the iterator case so
                        // only the intermediate Vec allocation differs.
                        for waiter in waiters.take_all_via_vec() {
                            black_box(waiter);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree/notify_all", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_waiters(waiter_count),
                    |waiters| {
                        // Consume the owning iterator inside the timed routine
                        // so moving iteration out of take_all cannot fake a
                        // win.
                        for waiter in waiters.take_all() {
                            black_box(waiter);
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("btree_hash/notify_all", waiter_count),
            &waiter_count,
            |bencher, &waiter_count| {
                bencher.iter_batched_ref(
                    || prepare_btree_hash_waiters(waiter_count),
                    |waiters| black_box(waiters.take_all()),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_waiter_registry);
criterion_main!(benches);
