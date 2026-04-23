/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! README and lock documentation consistency tests.

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
/// Ensures lock source examples reference the current trait names.
fn test_rw_lock_docs_use_current_trait_names() {
    assert!(!ARC_RW_LOCK_SRC.contains("ReadWriteLock"));
    assert!(!ARC_ASYNC_RW_LOCK_SRC.contains("AsyncReadWriteLock"));
    assert!(ARC_RW_LOCK_SRC.contains("ArcRwLock, Lock"));
    assert!(ARC_ASYNC_RW_LOCK_SRC.contains("ArcAsyncRwLock, AsyncLock"));
}

#[test]
/// Ensures README dependency snippets stay in sync with Cargo.toml.
fn test_readme_dependency_version_matches_cargo_toml() {
    let cargo_version =
        extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
    let readme_en_version = extract_readme_dependency_version(README_EN)
        .expect("Failed to extract version from README.md");
    let readme_zh_version = extract_readme_dependency_version(README_ZH)
        .expect("Failed to extract version from README.zh_CN.md");
    assert_eq!(readme_en_version, cargo_version);
    assert_eq!(readme_zh_version, cargo_version);
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
