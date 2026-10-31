// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! README and lock documentation consistency tests.

use std::collections::BTreeSet;

use semver::Version;
use semver::VersionReq;

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const INLINE_MANIFEST: &str = r#"
[package]
version = "0.14.3"
include = ["/src/**", "/doc/**"]
[features]
default = []
monitor = []
[dependencies]
qubit-clock = { version = "^0.13", optional = true }
"#;
const NORMALIZED_MANIFEST: &str = r#"
[package]
include = [
  "/doc/**",
  "/src/**",
]
version = "0.14.3"
[features]
monitor = []
default = []
[dependencies.qubit-clock]
optional = true
version = "^0.13"
"#;
const STRING_DEPENDENCY_MANIFEST: &str = r#"
[dependencies]
qubit-clock = "^0.13"
"#;
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const USER_GUIDE_EN: &str = include_str!("../../doc/user_guide.md");
const USER_GUIDE_ZH: &str = include_str!("../../doc/user_guide.zh_CN.md");
const BOUNDED_QUEUE_EXAMPLE: &str = include_str!("../../examples/bounded_queue.rs");
const LIB_RS: &str = include_str!("../../src/lib.rs");
const READ_WRITE_LOCK_SRC: &str = include_str!("../../src/lock/read_write_lock.rs");
const ASYNC_CONDITION_WAITER_SRC: &str = include_str!("../../src/monitor/async_condition_waiter.rs");
const ASYNC_READ_WRITE_LOCK_SRC: &str = include_str!("../../src/lock/async_read_write_lock.rs");

/// Collapses Markdown whitespace so public-symbol checks tolerate line
/// wrapping.
fn normalize_readme_text(content: &str) -> String {
    content.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn pop_for_body_without_whitespace(content: &str, label: &str) -> String {
    let normalized = content.replace("\r\n", "\n");
    let (_, function) = normalized
        .split_once("fn pop_for<")
        .unwrap_or_else(|| panic!("{label}: missing pop_for function"));
    let (_, body) = function
        .split_once("\n{\n")
        .unwrap_or_else(|| panic!("{label}: missing pop_for body opening"));
    let (body, _) = body
        .split_once("\n}")
        .unwrap_or_else(|| panic!("{label}: missing pop_for body closing"));
    body.chars().filter(|character| !character.is_whitespace()).collect()
}

#[test]
fn test_user_guide_pop_for_matches_runnable_example() {
    for (label, guide) in [("English", USER_GUIDE_EN), ("Chinese", USER_GUIDE_ZH)] {
        assert_eq!(
            pop_for_body_without_whitespace(guide, label),
            pop_for_body_without_whitespace(BOUNDED_QUEUE_EXAMPLE, "example"),
            "{label} pop_for must preserve the runnable example notification protocol",
        );
    }
}

#[test]
fn test_user_guides_do_not_use_rustdoc_hidden_lines() {
    for (language, guide) in [("English", USER_GUIDE_EN), ("Chinese", USER_GUIDE_ZH)] {
        for section in guide.split("```rust").skip(1) {
            let code = section.split("```").next().expect("Rust block exists");
            assert!(
                !code.lines().any(|line| {
                    let line = line.trim_start();
                    line == "#" || line.starts_with("# ")
                }),
                "{language} guide must display ordinary Rust source",
            );
        }
    }
}

/// Ensures README files document the current data-lock method names.
#[test]
fn test_readme_documents_data_lock_api_names() {
    assert!(README_EN.contains("with_read"));
    assert!(README_EN.contains("with_write"));
    assert!(README_ZH.contains("with_read"));
    assert!(README_ZH.contains("with_write"));
}

/// Ensures README quick-start snippets prove one domain operation can use
/// multiple lock backends.
#[test]
fn test_readme_quick_start_proves_backend_neutral_data_access() {
    for readme in [README_EN, README_ZH] {
        assert!(readme.contains(concat!("use qubit_lock", "::DataLock;")));
        assert!(readme.contains("fn record<L>"));
        assert!(readme.contains("DataLock<ServiceStats>"));
        assert!(readme.contains("std::sync::Mutex<ServiceStats>"));
        assert!(readme.contains("std::sync::RwLock<ServiceStats>"));
    }
}

/// Ensures README files document direct access to wrapped primitives.
#[test]
fn test_readme_documents_native_lock_support() {
    assert!(README_EN.contains("`std::sync::Mutex<T>`"));
    assert!(README_EN.contains("`parking_lot::RwLock<T>`"));
    assert!(README_ZH.contains("`std::sync::Mutex<T>`"));
    assert!(README_ZH.contains("`parking_lot::RwLock<T>`"));
}

/// Ensures each overview exposes the public lock and monitor capabilities.
#[test]
fn test_documentation_overviews_name_public_capabilities() {
    for (label, content) in [
        ("README.md", README_EN),
        ("README.zh_CN.md", README_ZH),
        ("doc/user_guide.md", USER_GUIDE_EN),
        ("doc/user_guide.zh_CN.md", USER_GUIDE_ZH),
    ] {
        let normalized = normalize_readme_text(content);
        for capability in ["`Lock`", "`ParkingLotMonitor`"] {
            assert!(normalized.contains(capability), "{label}: missing {capability}");
        }
    }
}

/// Ensures each README points readers to the user guide in the same language.
#[test]
fn test_readmes_link_to_their_matching_language_user_guides() {
    assert!(README_EN.contains("](doc/user_guide.md)"));
    assert!(README_ZH.contains("](doc/user_guide.zh_CN.md)"));
}

/// Ensures the queue case study explains why its two predicates require a
/// broadcast notification after a state transition.
#[test]
fn test_user_guides_explain_queue_notification_policy() {
    assert!(USER_GUIDE_EN.contains("with_write_notify_all"));
    assert!(USER_GUIDE_EN.contains("notify_all"));
    assert!(USER_GUIDE_ZH.contains("with_write_notify_all"));
    assert!(USER_GUIDE_ZH.contains("notify_all"));
}

/// Ensures both guides use the same backend-neutral queue case study.
#[test]
fn test_user_guides_present_backend_neutral_queue_case_study() {
    for guide in [USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(guide.contains("QueueState<T>"));
        assert!(guide.contains("NonZeroUsize"));
        assert!(guide.contains("StdMonitor"));
        assert!(guide.contains("ParkingLotMonitor"));
        assert!(guide.contains("notify_all"));
        assert!(guide.contains("TimedMonitor"));
        assert!(guide.contains("ManualMonotonicClock"));
        assert!(guide.contains("AsyncTimedMonitor"));
    }
}

/// Ensures public docs explain the monitor handshake for external predicate
/// state.
#[test]
fn test_async_external_predicate_example_uses_monitor_handshake_apis() {
    assert!(ASYNC_CONDITION_WAITER_SRC.contains("let waiter = tokio::spawn"));
    assert!(ASYNC_CONDITION_WAITER_SRC.contains(".with_write_notify_all_async"));
    assert!(USER_GUIDE_EN.contains("with_write_notify_all_async"));
    assert!(USER_GUIDE_ZH.contains("with_write_notify_all_async"));
}

/// Ensures README files distinguish the blocking monitor implementations and
/// document the async feature tiers.
#[test]
fn test_readme_documents_feature_tiers() {
    const BLOCKING_MONITOR_DEPENDENCY: &str =
        "qubit-lock = { version = \"0.14\", default-features = false, features = [\"monitor\", \"parking-lot\"] }";
    const STD_MONITOR_DEPENDENCY: &str =
        "qubit-lock = { version = \"0.14\", default-features = false, features = [\"monitor\"] }";
    const ASYNC_LOCK_DEPENDENCY: &str =
        "qubit-lock = { version = \"0.14\", default-features = false, features = [\"async-lock\"] }";
    const ASYNC_MONITOR_DEPENDENCY: &str =
        "qubit-lock = { version = \"0.14\", default-features = false, features = [\"async-monitor\"] }";

    assert!(CARGO_TOML.contains("default = []"));
    for document in [README_EN, README_ZH, USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(
            document.contains(BLOCKING_MONITOR_DEPENDENCY),
            "blocking monitor example must select both required features",
        );
        assert!(
            document.contains(ASYNC_LOCK_DEPENDENCY),
            "async-lock example must select its feature",
        );
        assert!(
            document.contains(ASYNC_MONITOR_DEPENDENCY),
            "async-monitor example must select its feature",
        );
    }
    for document in [README_EN, README_ZH] {
        assert!(
            document.contains(STD_MONITOR_DEPENDENCY),
            "StdMonitor example must select only the monitor feature",
        );
    }
    for document in [README_EN, README_ZH, USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(
            !document.contains("Arc*"),
            "documentation must name standard Arc-wrapped monitor types",
        );
    }
    assert!(!README_EN.contains("`mock` feature"));
    assert!(!README_ZH.contains("`mock` feature"));
}

/// Ensures both guides name the standard-monitor poison APIs.
#[test]
fn test_user_guides_reference_std_monitor_poison_apis() {
    assert!(USER_GUIDE_EN.contains("`is_poisoned`"));
    assert!(USER_GUIDE_EN.contains("`clear_poison`"));
    assert!(USER_GUIDE_ZH.contains("`is_poisoned`"));
    assert!(USER_GUIDE_ZH.contains("`clear_poison`"));
}

/// Ensures contribution sections document the available project entry points.
#[test]
fn test_readmes_document_contribution_entry_points() {
    for (readme, heading) in [(README_EN, "## Contributing"), (README_ZH, "## 贡献")] {
        let (_, contribution) = readme
            .split_once(heading)
            .expect("README contribution section must exist");
        let contribution = contribution.split("\n## ").next().expect("section exists");
        assert!(contribution.contains("./.infra/bin/align-ci.sh"));
        assert!(contribution.contains("./.infra/bin/ci-check.sh"));
        assert!(!contribution.contains("./align-ci.sh"));
        assert!(!contribution.contains("./ci-check.sh"));
    }
}

/// Ensures public API documentation stays aligned with root-only exports.
#[test]
fn test_readme_documents_root_only_public_api() {
    assert!(!README_EN.contains(concat!("from `qubit_lock", "::monitor`")));
    assert!(LIB_RS.contains("mod lock;"));
    assert!(LIB_RS.contains("mod monitor;"));
    assert!(!LIB_RS.contains("pub mod lock;"));
    assert!(!LIB_RS.contains("pub mod monitor;"));
}

/// Ensures lock source examples reference the current trait names.
#[test]
fn test_rw_lock_docs_use_current_trait_names() {
    assert!(READ_WRITE_LOCK_SRC.contains("ReadWriteLock"));
    assert!(ASYNC_READ_WRITE_LOCK_SRC.contains("AsyncReadWriteLock"));
}

/// Ensures Cargo exposes only the current feature surface.
#[test]
fn test_cargo_features_match_current_api() {
    assert_eq!(
        manifest_feature_names(&parse_manifest(CARGO_TOML)),
        BTreeSet::from([
            "default",
            "async-lock",
            "async-monitor",
            "loom-model",
            "monitor",
            "parking-lot",
        ]),
    );
}

#[test]
fn test_manifest_reading_ignores_toml_formatting_and_dependency_form() {
    let inline = parse_manifest(INLINE_MANIFEST);
    let normalized = parse_manifest(NORMALIZED_MANIFEST);

    assert_eq!(manifest_package_version(&inline), manifest_package_version(&normalized));
    assert_eq!(manifest_feature_names(&inline), manifest_feature_names(&normalized));
    assert_eq!(
        manifest_dependency_version(&inline, "qubit-clock"),
        manifest_dependency_version(&normalized, "qubit-clock"),
    );
    assert_eq!(
        manifest_package_includes(&inline),
        manifest_package_includes(&normalized)
    );
    assert_eq!(
        manifest_dependency_version(&parse_manifest(STRING_DEPENDENCY_MANIFEST), "qubit-clock"),
        "^0.13",
    );
}

#[test]
#[should_panic(expected = "manifest must declare the dependency")]
fn test_manifest_dependency_reader_rejects_missing_dependency() {
    manifest_dependency_version(&parse_manifest("[dependencies]\n"), "qubit-clock");
}

#[test]
#[should_panic(expected = "dependency must declare a string version requirement")]
fn test_manifest_dependency_reader_rejects_invalid_version_type() {
    manifest_dependency_version(
        &parse_manifest("[dependencies]\nqubit-clock = { version = 13, optional = true }\n"),
        "qubit-clock",
    );
}

/// Ensures the published crate contains the guides linked from both READMEs.
#[test]
fn test_cargo_package_includes_user_guides() {
    assert!(manifest_package_includes(&parse_manifest(CARGO_TOML)).contains("/doc/**"));
}

/// Ensures all README `qubit-lock` version requirements accept the crate
/// version in Cargo.toml.
#[test]
fn test_readme_dependency_versions_match_cargo_toml() {
    let manifest = parse_manifest(CARGO_TOML);
    let cargo_version = manifest_package_version(&manifest);
    let package_ver = Version::parse(cargo_version).expect("Invalid package version in Cargo.toml");

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

    assert_readme_versions_match("README.md", &readme_en_reqs, &package_ver, cargo_version);
    assert_readme_versions_match("README.zh_CN.md", &readme_zh_reqs, &package_ver, cargo_version);
}

/// Ensures both user guides use the same `qubit-clock` requirement as
/// Cargo.toml.
#[test]
fn test_readme_qubit_clock_dependency_version_matches_cargo_toml() {
    let manifest = parse_manifest(CARGO_TOML);
    let cargo_requirement = manifest_dependency_version(&manifest, "qubit-clock");

    for (filename, content) in [
        ("doc/user_guide.md", USER_GUIDE_EN),
        ("doc/user_guide.zh_CN.md", USER_GUIDE_ZH),
    ] {
        let readme_requirement = extract_inline_dependency_version(content, "qubit-clock")
            .unwrap_or_else(|| panic!("{filename} does not mention qubit-clock"));
        assert_eq!(
            readme_requirement, cargo_requirement,
            "{filename} qubit-clock version differs from Cargo.toml",
        );
    }
}

/// Ensures the public documents provide navigation, deadline selection, and a
/// runnable version of the bounded-queue case study.
#[test]
fn test_documentation_provides_navigation_deadline_and_runnable_example() {
    for guide in [USER_GUIDE_EN, USER_GUIDE_ZH] {
        assert!(guide.contains("`wait_until_with_deadline_async`"));
        assert!(guide.contains("`loom-model`"));
        assert!(guide.contains("../examples/bounded_queue.rs"));
        assert!(guide.contains("cargo run --example bounded_queue --features monitor,parking-lot"));
    }

    for readme in [README_EN, README_ZH] {
        assert!(readme.contains("examples/bounded_queue.rs"));
    }

    assert!(BOUNDED_QUEUE_EXAMPLE.contains("thread::spawn"));
    assert!(BOUNDED_QUEUE_EXAMPLE.contains("Arc::new"));
}

/// Parses a Cargo manifest into its TOML value.
fn parse_manifest(content: &str) -> toml::Value {
    toml::from_str(content).expect("Cargo.toml must contain valid TOML")
}

fn manifest_package_version(manifest: &toml::Value) -> &str {
    manifest
        .get("package")
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .expect("manifest package must declare a string version")
}

fn manifest_feature_names(manifest: &toml::Value) -> BTreeSet<&str> {
    manifest
        .get("features")
        .and_then(toml::Value::as_table)
        .expect("manifest must define a feature table")
        .keys()
        .map(String::as_str)
        .collect()
}

fn manifest_dependency_version<'a>(manifest: &'a toml::Value, name: &str) -> &'a str {
    let dependency = manifest
        .get("dependencies")
        .and_then(|dependencies| dependencies.get(name))
        .expect("manifest must declare the dependency");
    dependency
        .as_str()
        .or_else(|| dependency.get("version").and_then(toml::Value::as_str))
        .expect("dependency must declare a string version requirement")
}

fn manifest_package_includes(manifest: &toml::Value) -> BTreeSet<&str> {
    manifest
        .get("package")
        .and_then(|package| package.get("include"))
        .and_then(toml::Value::as_array)
        .expect("manifest package must define an include array")
        .iter()
        .map(|entry| entry.as_str().expect("package include entries must be strings"))
        .collect()
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
fn extract_inline_dependency_version<'a>(content: &'a str, dependency: &str) -> Option<&'a str> {
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
fn assert_readme_versions_match(filename: &str, readme_reqs: &[&str], package_ver: &Version, cargo_version: &str) {
    for (index, readme_req) in readme_reqs.iter().enumerate() {
        let req =
            VersionReq::parse(readme_req).unwrap_or_else(|_| panic!("Invalid version req in {filename}: {readme_req}"));
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
