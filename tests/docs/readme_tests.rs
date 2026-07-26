// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! README and lock documentation consistency tests.

use semver::{
    Version,
    VersionReq,
};

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const LIB_RS: &str = include_str!("../../src/lib.rs");
const READ_WRITE_LOCK_SRC: &str =
    include_str!("../../src/lock/read_write_lock.rs");
const CONDITION_WAITER_SRC: &str =
    include_str!("../../src/monitor/condition_waiter.rs");
const ASYNC_CONDITION_WAITER_SRC: &str =
    include_str!("../../src/monitor/async_condition_waiter.rs");
const ASYNC_READ_WRITE_LOCK_SRC: &str =
    include_str!("../../src/lock/async_read_write_lock.rs");
const TIMEOUT_CONDITION_WAITER_SRC: &str =
    include_str!("../../src/monitor/timeout_condition_waiter.rs");
const ASYNC_TIMEOUT_CONDITION_WAITER_SRC: &str =
    include_str!("../../src/monitor/async_timeout_condition_waiter.rs");
const PARKING_LOT_MONITOR_SRC: &str =
    include_str!("../../src/monitor/parking_lot_monitor.rs");
const PARKING_LOT_MONITOR_GUARD_SRC: &str =
    include_str!("../../src/monitor/parking_lot_monitor_guard.rs");
const STD_MONITOR_SRC: &str = include_str!("../../src/monitor/std_monitor.rs");
const STD_MONITOR_GUARD_SRC: &str =
    include_str!("../../src/monitor/std_monitor_guard.rs");

/// Collapses Markdown whitespace so prose assertions do not depend on line
/// wrapping.
fn normalize_readme_text(content: &str) -> String {
    content.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
/// Ensures README files document the current data-lock method names.
fn test_readme_documents_data_lock_api_names() {
    assert!(README_EN.contains("with_read"));
    assert!(README_EN.contains("with_write"));
    assert!(README_ZH.contains("with_read"));
    assert!(README_ZH.contains("with_write"));
}

#[test]
/// Ensures README quick-start snippets import the trait needed for lock
/// methods.
fn test_readme_quick_start_imports_data_lock_trait() {
    assert!(README_EN.contains("use qubit_lock::DataLock;"));
    assert!(README_ZH.contains("use qubit_lock::DataLock;"));
    assert!(README_EN.contains("let counter = std::sync::Mutex::new(0);"));
    assert!(README_ZH.contains("let counter = std::sync::Mutex::new(0);"));
    assert!(!README_EN.contains("let counter = parking_lot::Mutex::new(0);"));
    assert!(!README_ZH.contains("let counter = parking_lot::Mutex::new(0);"));
}

#[test]
/// Ensures timed-wait API docs cover failures after Timer registration.
fn test_monitor_docs_cover_timer_registration_and_completion_errors() {
    let sources = [
        ("timeout_condition_waiter.rs", TIMEOUT_CONDITION_WAITER_SRC),
        (
            "async_timeout_condition_waiter.rs",
            ASYNC_TIMEOUT_CONDITION_WAITER_SRC,
        ),
        ("parking_lot_monitor.rs", PARKING_LOT_MONITOR_SRC),
        (
            "parking_lot_monitor_guard.rs",
            PARKING_LOT_MONITOR_GUARD_SRC,
        ),
        ("std_monitor.rs", STD_MONITOR_SRC),
        ("std_monitor_guard.rs", STD_MONITOR_GUARD_SRC),
    ];

    for (filename, source) in sources {
        assert!(
            source.contains("Timer registration or completion errors"),
            "{filename} omits Timer completion errors",
        );
    }

    assert!(README_EN.contains("Timer registration or completion errors"));
    assert!(README_ZH.contains("Timer 注册或完成错误"));
}

#[test]
/// Ensures README files document direct access to wrapped primitives.
fn test_readme_documents_native_lock_support() {
    assert!(README_EN.contains("`std::sync::Mutex<T>`"));
    assert!(README_EN.contains("`parking_lot::RwLock<T>`"));
    assert!(README_ZH.contains("`std::sync::Mutex<T>`"));
    assert!(README_ZH.contains("`parking_lot::RwLock<T>`"));
}

#[test]
/// Ensures both READMEs distinguish generic acquisition modes from exclusive
/// ones.
fn test_readme_documents_exclusive_lock_capability() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);

    assert!(readme_en.contains("`Lock` represents one acquisition mode"));
    assert!(readme_en.contains("`ExclusiveLock` marks acquisition modes"));
    assert!(readme_en.contains("`ReadLock` implements `Lock` only"));
    assert!(readme_zh.contains("`Lock` 表示一种获取模式"));
    assert!(readme_zh.contains("`ExclusiveLock` 标记"));
    assert!(readme_zh.contains("`ReadLock` 只实现 `Lock`"));
}

#[test]
/// Ensures README monitor snippets show the combined write-and-notify API.
fn test_readme_monitor_example_uses_with_write_notify_one() {
    assert!(README_EN.contains("use qubit_lock::ArcParkingLotMonitor;"));
    assert!(README_EN.contains("with_write_notify_one"));
    assert!(README_EN.contains("combined write-and-notify helpers by default"));
    assert!(README_ZH.contains("use qubit_lock::ArcParkingLotMonitor;"));
    assert!(README_ZH.contains("with_write_notify_one"));
    assert!(README_ZH.contains("默认使用组合 write-and-notify helper"));
}

#[test]
/// Ensures both READMEs describe monitor notification and timeout semantics.
fn test_readme_documents_monitor_wait_semantics() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);
    assert!(readme_en.contains("memoryless condition-variable semantics"));
    assert!(readme_en.contains("already registered waiters"));
    assert!(readme_en.contains("condition-wait budget"));
    assert!(readme_en.contains("one fixed deadline"));
    assert!(readme_en.contains("A zero timeout still checks the predicate"));
    assert!(readme_en.contains("final locked predicate check wins"));
    assert!(readme_en.contains(
        "Timer registration or completion error takes precedence over every post-wait predicate result"
    ));
    assert!(readme_en.contains("the action is not run"));
    assert!(readme_zh.contains("无记忆的条件变量语义"));
    assert!(readme_zh.contains("已经注册的 waiter"));
    assert!(readme_zh.contains("条件等待预算"));
    assert!(readme_zh.contains("同一个固定 deadline"));
    assert!(readme_zh.contains("零 timeout 仍会检查 predicate"));
    assert!(readme_zh.contains("最后一次持锁 predicate 检查优先"));
    assert!(
        readme_zh
            .contains("Timer 注册或完成错误优先于任何等待后的 predicate 结果")
    );
    assert!(readme_zh.contains("不会执行 action"));
}

#[test]
/// Ensures public docs explain the monitor handshake for external predicate
/// state.
fn test_monitor_docs_cover_external_predicate_state_handshake() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);

    assert!(readme_en.contains("external predicate state"));
    assert!(readme_en.contains("Atomic ordering alone cannot prevent"));
    assert!(readme_en.contains("same monitor-lock handshake"));
    assert!(readme_zh.contains("monitor 外部的 predicate 状态"));
    assert!(readme_zh.contains("仅靠 atomic ordering 无法防止"));
    assert!(readme_zh.contains("同一个 monitor-lock handshake"));

    for source in [CONDITION_WAITER_SRC, ASYNC_CONDITION_WAITER_SRC] {
        assert!(source.contains("External predicate state"));
        assert!(source.contains("Atomic ordering alone"));
        assert!(source.contains("same monitor lock"));
    }
    assert!(ASYNC_CONDITION_WAITER_SRC.contains("let waiter = tokio::spawn"));
    assert!(
        ASYNC_CONDITION_WAITER_SRC.contains(".with_write_notify_all_async")
    );
    assert!(readme_en.contains("with_write_notify_all_async"));
    assert!(readme_zh.contains("with_write_notify_all_async"));
}

#[test]
/// Ensures both READMEs describe async cancellation and Tokio timer needs.
fn test_readme_documents_async_monitor_contract() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);
    assert!(readme_en.contains("returned future is lazy"));
    assert!(readme_en.contains("may be polled from another runtime context"));
    assert!(readme_en.contains("target runtime must remain alive"));
    assert!(readme_en.contains("have time enabled"));
    assert!(readme_en.contains("does not run the action"));
    assert!(readme_en.contains("does not roll back protected-state changes"));
    assert!(readme_en.contains("discards that selection"));
    assert!(readme_zh.contains("返回的 future 是惰性的"));
    assert!(readme_zh.contains("其他 runtime context 中 poll"));
    assert!(readme_zh.contains("目标 runtime 必须保持存活"));
    assert!(readme_zh.contains("启用 time driver"));
    assert!(readme_zh.contains("不会执行 action"));
    assert!(readme_zh.contains("不会回滚受保护状态的变化"));
    assert!(readme_zh.contains("丢弃该次选择"));
}

#[test]
/// Ensures both READMEs describe RPITIT and Arc monitor ownership boundaries.
fn test_readme_documents_monitor_api_boundaries() {
    assert!(README_EN.contains("return `impl Future`"));
    assert!(README_EN.contains("`from_arc`, `as_arc`, and `into_arc`"));
    assert!(README_EN.contains("resolve through `Deref`"));
    assert!(README_ZH.contains("返回 `impl Future`"));
    assert!(README_ZH.contains("`from_arc`、`as_arc` 和 `into_arc`"));
    assert!(README_ZH.contains("通过 `Deref` 解析"));
}

#[test]
/// Ensures both READMEs explain concrete and generic monitor selection.
fn test_readme_documents_monitor_capability_selection() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);
    assert!(readme_en.contains("Choosing monitor capabilities"));
    assert!(readme_en.contains("use the narrowest capability"));
    assert!(readme_en.contains("static generic bounds"));
    assert!(readme_en.contains("Every concrete monitor exposes `with_timer`"));
    assert!(readme_zh.contains("选择 monitor 能力"));
    assert!(readme_zh.contains("使用能够表达操作的最小能力"));
    assert!(readme_zh.contains("静态泛型约束"));
    assert!(readme_zh.contains("每个具体 monitor 都提供 `with_timer`"));
}

#[test]
/// Ensures both READMEs describe deterministic testing through Timer IOC.
fn test_readme_documents_timer_ioc_testing() {
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);
    assert!(readme_en.contains("there is no separate mock wait algorithm"));
    assert!(
        readme_en.contains("`ManualMonotonicClock` is the test control plane")
    );
    assert!(readme_zh.contains("不再维护另一套 mock 等待算法"));
    assert!(readme_zh.contains("`ManualMonotonicClock` 是测试控制面"));
}

#[test]
/// Ensures README files document the default, async-lock, and async-monitor
/// feature tiers.
fn test_readme_documents_feature_tiers() {
    assert!(README_EN.contains("default feature set"));
    assert!(README_EN.contains("`monitor` and `parking-lot`"));
    assert!(README_EN.contains("default-features = false"));
    assert!(README_EN.contains("`async-lock`"));
    assert!(README_EN.contains("`async-monitor`"));
    assert!(!README_EN.contains("`mock` feature"));
    assert!(README_ZH.contains("默认特性集"));
    assert!(README_ZH.contains("`monitor` 和 `parking-lot`"));
    assert!(README_ZH.contains("default-features = false"));
    assert!(README_ZH.contains("`async-lock`"));
    assert!(README_ZH.contains("`async-monitor`"));
    assert!(!README_ZH.contains("`mock` feature"));
}

#[test]
/// Ensures public API documentation stays aligned with root-only exports.
fn test_readme_documents_root_only_public_api() {
    assert!(!README_EN.contains("from `qubit_lock::monitor`"));
    assert!(!README_ZH.contains("\u{6216} crate root"));
    assert!(
        README_EN.contains("Import public types directly from the crate root.")
    );
    assert!(README_ZH.contains("crate root"));
    assert!(LIB_RS.contains("mod lock;"));
    assert!(LIB_RS.contains("mod monitor;"));
    assert!(!LIB_RS.contains("pub mod lock;"));
    assert!(!LIB_RS.contains("pub mod monitor;"));
}

#[test]
/// Ensures lock source examples reference the current trait names.
fn test_rw_lock_docs_use_current_trait_names() {
    assert!(READ_WRITE_LOCK_SRC.contains("ReadWriteLock"));
    assert!(ASYNC_READ_WRITE_LOCK_SRC.contains("AsyncReadWriteLock"));
}

#[test]
/// Ensures Cargo exposes only the current feature surface.
fn test_cargo_features_match_current_api() {
    assert_eq!(
        cargo_feature_names(CARGO_TOML),
        [
            "default",
            "async-lock",
            "async-monitor",
            "loom-model",
            "monitor",
            "parking-lot",
        ],
    );
}

#[test]
/// Ensures all README `qubit-lock` version requirements accept the crate
/// version in Cargo.toml.
fn test_readme_dependency_versions_match_cargo_toml() {
    let cargo_version = extract_package_version(CARGO_TOML)
        .expect("Failed to extract version from Cargo.toml");
    let package_ver = Version::parse(cargo_version)
        .expect("Invalid package version in Cargo.toml");

    let readme_en_reqs = extract_readme_dependency_versions(README_EN);
    let readme_zh_reqs = extract_readme_dependency_versions(README_ZH);
    let readme_en_dependency_count = count_readme_dependency_lines(README_EN);
    let readme_zh_dependency_count = count_readme_dependency_lines(README_ZH);

    assert!(
        !readme_en_reqs.is_empty(),
        "README.md does not contain any qubit-lock dependency versions"
    );
    assert!(
        !readme_zh_reqs.is_empty(),
        "README.zh_CN.md does not contain any qubit-lock dependency versions"
    );
    assert_eq!(
        readme_en_reqs.len(),
        readme_en_dependency_count,
        "README.md has qubit-lock dependency lines that were not parsed"
    );
    assert_eq!(
        readme_zh_reqs.len(),
        readme_zh_dependency_count,
        "README.zh_CN.md has qubit-lock dependency lines that were not parsed"
    );

    assert_readme_versions_match(
        "README.md",
        &readme_en_reqs,
        &package_ver,
        cargo_version,
    );
    assert_readme_versions_match(
        "README.zh_CN.md",
        &readme_zh_reqs,
        &package_ver,
        cargo_version,
    );
}

#[test]
/// Ensures both README files use the same `qubit-clock` requirement as
/// Cargo.toml.
fn test_readme_qubit_clock_dependency_version_matches_cargo_toml() {
    let cargo_requirement =
        extract_cargo_dependency_version(CARGO_TOML, "qubit-clock")
            .expect("Cargo.toml does not declare qubit-clock");

    for (filename, content) in
        [("README.md", README_EN), ("README.zh_CN.md", README_ZH)]
    {
        let readme_requirement =
            extract_inline_dependency_version(content, "qubit-clock")
                .unwrap_or_else(|| {
                    panic!("{filename} does not mention qubit-clock")
                });
        assert_eq!(
            readme_requirement, cargo_requirement,
            "{filename} qubit-clock version differs from Cargo.toml",
        );
    }
}

/// Extracts the first package version entry from Cargo.toml content.
fn extract_package_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("version = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Extracts feature names from the Cargo feature section.
fn cargo_feature_names(content: &str) -> Vec<&str> {
    let (_, feature_section) = content
        .split_once("[features]\n")
        .expect("Cargo.toml must define features");

    feature_section
        .lines()
        .take_while(|line| !line.starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .collect()
}

/// Extracts a dependency version requirement from Cargo.toml content.
///
/// # Parameters
///
/// * `content` - Cargo.toml content to inspect.
/// * `dependency` - Dependency name to locate.
///
/// # Returns
///
/// The first matching dependency version requirement, if present.
fn extract_cargo_dependency_version<'a>(
    content: &'a str,
    dependency: &str,
) -> Option<&'a str> {
    content.lines().find_map(|line| {
        let value = line.trim().strip_prefix(dependency)?.trim();
        let value = value.strip_prefix('=')?.trim();
        let version = value
            .strip_prefix('{')?
            .split(',')
            .find_map(|field| field.trim().strip_prefix("version = \""))?;
        version.split_once('"').map(|(requirement, _)| requirement)
    })
}

/// Extracts an inline dependency version from prose or a code snippet.
///
/// # Parameters
///
/// * `content` - Documentation content to inspect.
/// * `dependency` - Dependency name to locate.
///
/// # Returns
///
/// The first quoted version following `dependency =`, if present.
fn extract_inline_dependency_version<'a>(
    content: &'a str,
    dependency: &str,
) -> Option<&'a str> {
    let marker = format!("{dependency} = \"");
    let (_, value) = content.split_once(&marker)?;
    value.split_once('"').map(|(requirement, _)| requirement)
}

/// Asserts that every README dependency version accepts the package version.
fn assert_readme_versions_match(
    filename: &str,
    readme_reqs: &[&str],
    package_ver: &Version,
    cargo_version: &str,
) {
    for (index, readme_req) in readme_reqs.iter().enumerate() {
        let req = VersionReq::parse(readme_req).unwrap_or_else(|_| {
            panic!("Invalid version req in {filename}: {readme_req}")
        });
        assert!(
            req.matches(package_ver),
            "{filename} qubit-lock dependency #{index} = \"{readme_req}\" does not accept package version {cargo_version}"
        );
    }
}

/// Extracts all `qubit-lock` dependency versions from a README file.
fn extract_readme_dependency_versions(content: &str) -> Vec<&str> {
    content
        .lines()
        .filter_map(|line| extract_readme_dependency_version(line.trim()))
        .collect()
}

/// Extracts a `qubit-lock` dependency version from one README line.
fn extract_readme_dependency_version(line: &str) -> Option<&str> {
    let value = line.strip_prefix("qubit-lock = ")?;
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted.split_once('"').map(|(version, _)| version);
    }

    value
        .strip_prefix('{')?
        .strip_suffix('}')?
        .split(',')
        .find_map(|field| {
            field
                .trim()
                .strip_prefix("version = \"")
                .and_then(|version| version.strip_suffix('"'))
        })
}

/// Counts `qubit-lock` dependency declaration lines in a README file.
fn count_readme_dependency_lines(content: &str) -> usize {
    content
        .lines()
        .filter(|line| line.trim().starts_with("qubit-lock = "))
        .count()
}
