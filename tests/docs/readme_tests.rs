/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! README and lock documentation consistency tests.

use semver::{
    Version,
    VersionReq,
};

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const LIB_RS: &str = include_str!("../../src/lib.rs");
const ARC_RW_LOCK_SRC: &str = include_str!("../../src/lock/arc_rw_lock.rs");
const ARC_ASYNC_RW_LOCK_SRC: &str = include_str!("../../src/lock/arc_async_rw_lock.rs");

#[test]
/// Ensures README files only reference the current lock method names.
fn test_readme_no_legacy_lock_api_names() {
    assert!(!README_EN.contains("with_lock"));
    assert!(!README_EN.contains("try_with_lock"));
    assert!(!README_ZH.contains("with_lock"));
    assert!(!README_ZH.contains("try_with_lock"));
}

#[test]
/// Ensures README quick-start snippets import the trait needed for lock methods.
fn test_readme_quick_start_imports_lock_trait() {
    assert!(README_EN.contains("use qubit_lock::{ArcMutex, Lock};"));
    assert!(README_ZH.contains("use qubit_lock::{ArcMutex, Lock};"));
}

#[test]
/// Ensures README files document direct access to wrapped primitives.
fn test_readme_documents_deref_and_as_ref_support() {
    assert!(README_EN.contains("Deref"));
    assert!(README_EN.contains("AsRef"));
    assert!(README_ZH.contains("Deref"));
    assert!(README_ZH.contains("AsRef"));
}

#[test]
/// Ensures README monitor snippets show the combined write-and-notify API.
fn test_readme_monitor_example_uses_write_notify_one() {
    assert!(README_EN.contains("use qubit_lock::ArcMonitor;"));
    assert!(README_EN.contains("write_notify_one"));
    assert!(README_ZH.contains("use qubit_lock::ArcMonitor;"));
    assert!(README_ZH.contains("write_notify_one"));
}

#[test]
/// Ensures public API documentation stays aligned with root-only exports.
fn test_readme_documents_root_only_public_api() {
    assert!(!README_EN.contains("from `qubit_lock::monitor`"));
    assert!(!README_ZH.contains("\u{6216} crate root"));
    assert!(README_EN.contains("Import public types directly from the crate root."));
    assert!(README_ZH.contains("crate root"));
    assert!(LIB_RS.contains("mod lock;"));
    assert!(LIB_RS.contains("mod monitor;"));
    assert!(!LIB_RS.contains("pub mod lock;"));
    assert!(!LIB_RS.contains("pub mod monitor;"));
}

#[test]
/// Ensures lock source examples reference the current trait names.
fn test_rw_lock_docs_use_current_trait_names() {
    assert!(!ARC_RW_LOCK_SRC.contains("ReadWriteLock"));
    assert!(!ARC_ASYNC_RW_LOCK_SRC.contains("AsyncReadWriteLock"));
    assert!(ARC_RW_LOCK_SRC.contains("ArcRwLock, Lock"));
    assert!(ARC_ASYNC_RW_LOCK_SRC.contains("ArcAsyncRwLock, AsyncLock"));
}

#[test]
/// Ensures all README `qubit-lock` version requirements accept the crate version in Cargo.toml.
fn test_readme_dependency_versions_match_cargo_toml() {
    let cargo_version =
        extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
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
    assert_readme_versions_match(
        "README.zh_CN.md",
        &readme_zh_reqs,
        &package_ver,
        cargo_version,
    );
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

/// Asserts that every README dependency version accepts the package version.
fn assert_readme_versions_match(
    filename: &str,
    readme_reqs: &[&str],
    package_ver: &Version,
    cargo_version: &str,
) {
    for (index, readme_req) in readme_reqs.iter().enumerate() {
        let req = VersionReq::parse(readme_req)
            .unwrap_or_else(|_| panic!("Invalid version req in {filename}: {readme_req}"));
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
