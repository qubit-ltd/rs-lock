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
const USER_GUIDE_EN: &str = include_str!("../../doc/user_guide.md");
const USER_GUIDE_ZH: &str = include_str!("../../doc/user_guide.zh_CN.md");
const LIB_RS: &str = include_str!("../../src/lib.rs");
const LOCK_SRC: &str = include_str!("../../src/lock/lock.rs");
const READ_WRITE_LOCK_SRC: &str =
    include_str!("../../src/lock/read_write_lock.rs");
const WAIT_TIMEOUT_RESULT_SRC: &str =
    include_str!("../../src/monitor/wait_timeout_result.rs");
const WAIT_TIMEOUT_STATUS_SRC: &str =
    include_str!("../../src/monitor/wait_timeout_status.rs");
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
const TOKIO_MONITOR_SRC: &str =
    include_str!("../../src/monitor/tokio_monitor.rs");
const ARC_PARKING_LOT_MONITOR_SRC: &str =
    include_str!("../../src/monitor/arc_parking_lot_monitor.rs");
const ARC_STD_MONITOR_SRC: &str =
    include_str!("../../src/monitor/arc_std_monitor.rs");

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
/// Ensures public docs cover failures after Timer registration.
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

    assert!(USER_GUIDE_EN.contains("Timer registration or completion errors"));
    assert!(USER_GUIDE_ZH.contains("Timer 注册或完成错误"));
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
/// Ensures both user guides distinguish generic acquisition modes from
/// exclusive ones.
fn test_readme_documents_exclusive_lock_capability() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);

    assert!(guide_en.contains("`Lock` does not promise"));
    assert!(guide_en.contains("marker trait `ExclusiveLock`"));
    assert!(guide_en.contains("`ReadLock` implements `Lock`"));
    assert!(guide_zh.contains("`Lock` 不承诺"));
    assert!(guide_zh.contains("标记 trait `ExclusiveLock`"));
    assert!(guide_zh.contains("`ReadLock` 实现 `Lock`"));
}

#[test]
/// Ensures guide monitor snippets show the combined write-and-notify API.
fn test_readme_monitor_example_uses_with_write_notify_one() {
    assert!(USER_GUIDE_EN.contains("use qubit_lock::ArcParkingLotMonitor;"));
    assert!(USER_GUIDE_EN.contains("with_write_notify_one"));
    assert!(USER_GUIDE_EN.contains("state-update-and-notify protocol"));
    assert!(USER_GUIDE_ZH.contains("use qubit_lock::ArcParkingLotMonitor;"));
    assert!(USER_GUIDE_ZH.contains("with_write_notify_one"));
    assert!(USER_GUIDE_ZH.contains("状态更新与通知协议"));
}

#[test]
/// Ensures both guides explain the untimed and timed monitor aggregates.
fn test_readme_documents_monitor_capability_split() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);

    assert!(guide_en.contains("`Monitor` plus timed synchronous waits"));
    assert!(guide_en.contains("`AsyncMonitor` plus timed waits"));
    assert!(guide_zh.contains("`Monitor` 加同步计时等待"));
    assert!(guide_zh.contains("`AsyncMonitor` 加计时等待"));
}

#[test]
/// Ensures both guides describe monitor notification and timeout semantics.
fn test_readme_documents_monitor_wait_semantics() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);
    let readme_en = normalize_readme_text(README_EN);
    let readme_zh = normalize_readme_text(README_ZH);
    assert!(guide_en.contains("Notifications are memoryless"));
    assert!(guide_en.contains("already registered waiter"));
    assert!(guide_en.contains("condition-wait budget"));
    assert!(guide_en.contains(
        "After acquiring the state lock and before the first predicate check,"
    ));
    assert!(guide_en.contains(
        "may return after the timeout while reacquiring the state lock"
    ));
    assert!(guide_en.contains("one fixed absolute deadline"));
    assert!(guide_en.contains("A zero timeout still performs"));
    assert!(guide_en.contains("final predicate check under the lock wins"));
    assert!(
        guide_en.contains(
            "Timer registration or completion errors take precedence"
        )
    );
    assert!(guide_en.contains("the action is not run"));
    assert!(guide_zh.contains("Notification 是无记忆的"));
    assert!(guide_zh.contains("已经注册的 waiter"));
    assert!(guide_zh.contains("条件等待预算"));
    assert!(guide_zh.contains("取得状态锁后、首次 predicate 检查前"));
    assert!(guide_zh.contains("和条件变量一样，重新获取"));
    assert!(guide_zh.contains("状态锁时可能在 timeout 后返回"));
    assert!(guide_zh.contains("固定的 绝对 deadline"));
    assert!(guide_zh.contains("零时长 timeout 仍会执行初始 predicate 检查"));
    assert!(guide_zh.contains("最后一次持锁 predicate 检查优先"));
    assert!(guide_zh.contains("Timer 注册或完成错误优先"));
    assert!(guide_zh.contains("不会执行 action"));
    for readme in [readme_en, readme_zh] {
        assert!(readme.contains("std::sync::Condvar::wait_timeout_while"));
    }
}

#[test]
/// Ensures public docs explain the monitor handshake for external predicate
/// state.
fn test_monitor_docs_cover_external_predicate_state_handshake() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);

    assert!(guide_en.contains("predicate reads state outside the monitor"));
    assert!(guide_en.contains("Atomic ordering alone cannot stop"));
    assert!(guide_en.contains("monitor-lock handshake"));
    assert!(guide_zh.contains("predicate 读取 monitor 外的状态"));
    assert!(guide_zh.contains("仅靠 atomic ordering 无法阻止"));
    assert!(guide_zh.contains("monitor-lock handshake"));

    for source in [CONDITION_WAITER_SRC, ASYNC_CONDITION_WAITER_SRC] {
        assert!(source.contains("External predicate state"));
        assert!(source.contains("Atomic ordering alone"));
        assert!(source.contains("same monitor lock"));
    }
    assert!(ASYNC_CONDITION_WAITER_SRC.contains("let waiter = tokio::spawn"));
    assert!(
        ASYNC_CONDITION_WAITER_SRC.contains(".with_write_notify_all_async")
    );
    assert!(guide_en.contains("with_write_notify_all_async"));
    assert!(guide_zh.contains("with_write_notify_all_async"));
}

#[test]
/// Ensures both guides describe async cancellation and Tokio timer needs.
fn test_readme_documents_async_monitor_contract() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);
    assert!(guide_en.contains("Async wait futures are lazy"));
    assert!(guide_en.contains("may be polled from another runtime context"));
    assert!(guide_en.contains("target runtime must stay alive"));
    assert!(guide_en.contains("have time enabled"));
    assert!(guide_en.contains("does not run the action"));
    assert!(guide_en.contains("does not roll back protected-state changes"));
    assert!(guide_en.contains("cancellation discards the selection"));
    assert!(guide_zh.contains("异步等待 future 是惰性的"));
    assert!(guide_zh.contains("另一个 runtime context 中 poll"));
    assert!(guide_zh.contains("目标 runtime 必须保持存活"));
    assert!(guide_zh.contains("启用 time driver"));
    assert!(guide_zh.contains("不会执行 action"));
    assert!(guide_zh.contains("不会回滚受保护状态的变化"));
    assert!(guide_zh.contains("取消会丢弃这次选择"));
}

#[test]
/// Ensures both guides describe RPITIT and Arc monitor ownership boundaries.
fn test_readme_documents_monitor_api_boundaries() {
    assert!(USER_GUIDE_EN.contains("return-position `impl Future`"));
    assert!(USER_GUIDE_EN.contains("`from_arc`, `as_arc`, and"));
    assert!(USER_GUIDE_EN.contains("dereferences to its inner monitor"));
    assert!(USER_GUIDE_ZH.contains("返回位置的 `impl Future`"));
    assert!(USER_GUIDE_ZH.contains("`from_arc`、`as_arc` 和"));
    assert!(USER_GUIDE_ZH.contains("通过 `Deref` 访问内部 monitor"));
}

#[test]
/// Ensures both guides explain concrete and generic monitor selection.
fn test_readme_documents_monitor_capability_selection() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);
    assert!(guide_en.contains("Choosing components and avoiding mistakes"));
    assert!(guide_en.contains("narrowest capability trait"));
    assert!(guide_en.contains("static generic bounds"));
    assert!(guide_en.contains("Every concrete monitor provides `with_timer`"));
    assert!(guide_zh.contains("组件选择与常见错误"));
    assert!(guide_zh.contains("最小能力 trait"));
    assert!(guide_zh.contains("静态泛型约束"));
    assert!(guide_zh.contains("每个具体 monitor 都提供 `with_timer`"));
}

#[test]
/// Ensures both guides describe deterministic testing through Timer IOC.
fn test_readme_documents_timer_ioc_testing() {
    let guide_en = normalize_readme_text(USER_GUIDE_EN);
    let guide_zh = normalize_readme_text(USER_GUIDE_ZH);
    assert!(guide_en.contains("production wait algorithm runs"));
    assert!(guide_en.contains("`ManualMonotonicClock`"));
    assert!(guide_zh.contains("生产环境使用的 monitor 类型和等待算法"));
    assert!(guide_zh.contains("`ManualMonotonicClock`"));
}

#[test]
/// Ensures README files document the default, async-lock, and async-monitor
/// feature tiers.
fn test_readme_documents_feature_tiers() {
    const ASYNC_LOCK_DEPENDENCY: &str = "qubit-lock = { version = \"0.11\", default-features = false, features = [\"async-lock\"] }";
    const ASYNC_MONITOR_DEPENDENCY: &str = "qubit-lock = { version = \"0.11\", default-features = false, features = [\"async-monitor\"] }";

    assert!(README_EN.contains("default feature set"));
    assert!(README_EN.contains("`monitor` and `parking-lot`"));
    for document in [README_EN, README_ZH, USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(
            document.contains(ASYNC_LOCK_DEPENDENCY),
            "async-lock example must disable default features",
        );
        assert!(
            document.contains(ASYNC_MONITOR_DEPENDENCY),
            "async-monitor example must disable default features",
        );
    }
    assert!(!README_EN.contains("`mock` feature"));
    assert!(README_ZH.contains("默认特性集"));
    assert!(README_ZH.contains("`monitor` 和 `parking-lot`"));
    assert!(!README_ZH.contains("`mock` feature"));
}

#[test]
/// Ensures monitor Rustdoc records callback and default-Timer panic paths.
fn test_monitor_docs_cover_callback_and_constructor_panics() {
    for source in [
        CONDITION_WAITER_SRC,
        TIMEOUT_CONDITION_WAITER_SRC,
        ASYNC_CONDITION_WAITER_SRC,
        ASYNC_TIMEOUT_CONDITION_WAITER_SRC,
        TOKIO_MONITOR_SRC,
    ] {
        assert!(
            source.contains("# Panics")
                && source.contains("`predicate` or `action`"),
            "waiter documentation must describe callback panics",
        );
    }

    for source in [PARKING_LOT_MONITOR_SRC, STD_MONITOR_SRC] {
        assert!(
            source.contains("Panics if the registry")
                && source.contains("registration identifiers."),
            "blocking monitor documentation must describe waiter registration panics",
        );
    }

    for source in [
        PARKING_LOT_MONITOR_SRC,
        STD_MONITOR_SRC,
        ARC_PARKING_LOT_MONITOR_SRC,
        ARC_STD_MONITOR_SRC,
    ] {
        assert!(
            source.contains("Panics if all process-wide clock-domain identifiers are exhausted."),
            "default Timer constructors must describe clock-domain exhaustion",
        );
    }

    for source in [PARKING_LOT_MONITOR_GUARD_SRC, STD_MONITOR_GUARD_SRC] {
        assert!(
            source.contains(
                "Panics if the registry exhausts registration identifiers."
            ),
            "guard waits must describe waiter registration panics",
        );
    }
}

#[test]
/// Ensures standard-monitor documentation explains poison observation,
/// recovery, repair, and explicit acceptance.
fn test_monitor_docs_cover_std_monitor_poisoning_policy() {
    assert!(STD_MONITOR_SRC.contains("pub fn is_poisoned"));
    assert!(STD_MONITOR_SRC.contains("pub fn clear_poison"));
    assert!(STD_MONITOR_SRC.contains("partial mutations"));
    assert!(STD_MONITOR_SRC.contains("does not clear the poison marker"));

    assert!(USER_GUIDE_EN.contains("`is_poisoned`"));
    assert!(USER_GUIDE_EN.contains("`clear_poison`"));
    assert!(USER_GUIDE_EN.contains("partially modified"));
    assert!(USER_GUIDE_ZH.contains("`is_poisoned`"));
    assert!(USER_GUIDE_ZH.contains("`clear_poison`"));
    assert!(USER_GUIDE_ZH.contains("部分修改"));
}

#[test]
/// Ensures both user guides document the action-free asynchronous wait
/// conveniences.
fn test_monitor_docs_cover_async_ready_wait_helpers() {
    for guide in [USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(guide.contains("`wait_until_ready_async`"));
        assert!(guide.contains("`wait_until_ready_for_async`"));
    }
}

#[test]
/// Ensures the Chinese README contribution section matches the project
/// template.
fn test_readme_zh_uses_contribution_template() {
    assert!(README_ZH.contains(
        "欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交\nPull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。"
    ));
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
/// Ensures root and monitor Rustdoc preserve the implemented lock contracts.
fn test_rustdoc_contracts_match_lock_and_monitor_semantics() {
    assert!(LIB_RS.contains("synchronous lock acquisition"));
    assert!(LOCK_SRC.contains("`Lock` does not imply exclusive entry."));
    assert!(
        WAIT_TIMEOUT_RESULT_SRC.contains("deciding locked predicate check")
    );
    assert!(
        WAIT_TIMEOUT_RESULT_SRC.contains("blocking and asynchronous monitor")
    );
    assert!(WAIT_TIMEOUT_RESULT_SRC.contains("ParkingLotMonitor"));
    assert!(WAIT_TIMEOUT_RESULT_SRC.contains("TokioMonitor"));
    assert!(WAIT_TIMEOUT_STATUS_SRC.contains("blocking monitor guards"));
    assert!(WAIT_TIMEOUT_STATUS_SRC.contains("ParkingLotMonitorGuard"));

    for source in [PARKING_LOT_MONITOR_SRC, STD_MONITOR_SRC] {
        assert!(!source.contains("condition variable"));
        assert!(
            source.contains(
                "predicate stops blocking on the deciding locked check"
            )
        );
    }

    assert!(TOKIO_MONITOR_SRC.matches("fairness or FIFO").count() >= 3);
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
/// Ensures the published crate contains the guides linked from both READMEs.
fn test_cargo_package_includes_user_guides() {
    assert!(CARGO_TOML.contains("\"/doc/**\""));
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
/// Ensures both user guides use the same `qubit-clock` requirement as
/// Cargo.toml.
fn test_readme_qubit_clock_dependency_version_matches_cargo_toml() {
    let cargo_requirement =
        extract_cargo_dependency_version(CARGO_TOML, "qubit-clock")
            .expect("Cargo.toml does not declare qubit-clock");

    for (filename, content) in [
        ("doc/user_guide.md", USER_GUIDE_EN),
        ("doc/user_guide.zh_CN.md", USER_GUIDE_ZH),
    ] {
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
    content.lines().find_map(|line| {
        let value = line.trim().strip_prefix(dependency)?.trim();
        let value = value.strip_prefix('=')?.trim();
        if let Some(value) = value.strip_prefix('"') {
            return value.split_once('"').map(|(requirement, _)| requirement);
        }
        let (_, value) = value.split_once("version = \"")?;
        value.split_once('"').map(|(requirement, _)| requirement)
    })
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
