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
/// Ensures lock source examples reference the current trait names.
fn test_rw_lock_docs_use_current_trait_names() {
    assert!(!ARC_RW_LOCK_SRC.contains("ReadWriteLock"));
    assert!(!ARC_ASYNC_RW_LOCK_SRC.contains("AsyncReadWriteLock"));
    assert!(ARC_RW_LOCK_SRC.contains("ArcRwLock, Lock"));
    assert!(ARC_ASYNC_RW_LOCK_SRC.contains("ArcAsyncRwLock, AsyncLock"));
}

#[test]
/// Ensures README `qubit-lock` version requirements accept the crate version in Cargo.toml.
fn test_readme_dependency_version_matches_cargo_toml() {
    let cargo_version =
        extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
    let package_ver = Version::parse(cargo_version).expect("Invalid package version in Cargo.toml");
    let readme_en_req = extract_readme_dependency_version(README_EN)
        .expect("Failed to extract version from README.md");
    let readme_zh_req = extract_readme_dependency_version(README_ZH)
        .expect("Failed to extract version from README.zh_CN.md");
    let req_en = VersionReq::parse(readme_en_req).expect("Invalid version req in README.md");
    let req_zh = VersionReq::parse(readme_zh_req).expect("Invalid version req in README.zh_CN.md");
    assert!(
        req_en.matches(&package_ver),
        "README.md qubit-lock = \"{readme_en_req}\" does not accept package version {cargo_version}"
    );
    assert!(
        req_zh.matches(&package_ver),
        "README.zh_CN.md qubit-lock = \"{readme_zh_req}\" does not accept package version {cargo_version}"
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

/// Extracts the `qubit-lock` dependency version from a README file.
fn extract_readme_dependency_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-lock = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}
