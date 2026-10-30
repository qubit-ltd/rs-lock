#!/usr/bin/env python3
"""Compile the supported Rust snippet combinations in both user guides."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib


FRAGMENT_NAMES = (
    "queue-state",
    "queue-ops",
    "blocking-main",
    "timed-ops",
    "manual-main",
    "tokio-main",
)
SCENARIOS = {
    "blocking": ("queue-state", "queue-ops", "blocking-main"),
    "manual": ("queue-state", "queue-ops", "timed-ops", "manual-main"),
    "tokio": ("queue-state", "tokio-main"),
}
MARKER = re.compile(r"^<!-- rs-lock-example: ([a-z-]+) -->\s*$", re.MULTILINE)


def extract_fragments(content: str, source: str) -> dict[str, str]:
    """Extract each uniquely marked Rust block, requiring its adjacent fence."""
    markers = list(MARKER.finditer(content))
    result = {}
    errors = []
    for name in FRAGMENT_NAMES:
        found = [marker for marker in markers if marker.group(1) == name]
        if len(found) != 1:
            errors.append(f"{name}: expected one marker, found {len(found)}")
            continue
        marker = found[0]
        remainder = content[marker.end():]
        opening = re.match(r"\n```rust\s*\n", remainder)
        if opening is None:
            errors.append(f"{name}: marker must be followed immediately by a Rust fence")
            continue
        body_start = marker.end() + opening.end()
        closing = re.search(r"^```\s*$", content[body_start:], re.MULTILINE)
        if closing is None:
            errors.append(f"{name}: Rust fence is not closed")
            continue
        body = content[body_start:body_start + closing.start()].strip()
        if not body:
            errors.append(f"{name}: Rust block is empty")
            continue
        result[name] = body
    unexpected = sorted({marker.group(1) for marker in markers} - set(FRAGMENT_NAMES))
    if unexpected:
        errors.append(f"unexpected marker(s): {', '.join(unexpected)}")
    if errors:
        raise ValueError(f"{source}: " + "; ".join(errors))
    return result


def assemble(fragments, scenario):
    return "\n\n".join(fragments[name] for name in SCENARIOS[scenario]) + "\n"


def toml_string(value: str) -> str:
    """Encode a TOML basic string using JSON's compatible escaping."""
    return json.dumps(value, ensure_ascii=True)


def project_manifest(root: Path, metadata: dict, scenario: str) -> str:
    package = metadata["package"]
    dependencies = metadata["dependencies"]
    lock_dep = dependencies["qubit-clock"]
    lock_version = lock_dep if isinstance(lock_dep, str) else lock_dep["version"]
    lock_path = toml_string(str(root))
    lock_features = ["async-monitor"] if scenario == "tokio" else ["monitor", "parking-lot"]
    lines = [
        "[workspace]",
        "",
        "[package]",
        'name = "rs-lock-guide-check"',
        'version = "0.0.0"',
        f"edition = {toml_string(package['edition'])}",
        "",
        "[dependencies]",
        f"{toml_string(package['name'])} = {{ path = {lock_path}, features = {json.dumps(lock_features)} }}",
    ]
    if scenario == "manual":
        lines.append(
            f'qubit-clock = {{ version = {toml_string(lock_version)}, features = ["test-util"] }}'
        )
    elif scenario == "tokio":
        tokio_dep = dependencies["tokio"]
        tokio_version = tokio_dep if isinstance(tokio_dep, str) else tokio_dep["version"]
        lines.append(
            f'tokio = {{ version = {toml_string(tokio_version)}, default-features = false, '
            'features = ["macros", "rt-multi-thread", "time"] }'
        )
    lines.extend(
        [
            "",
            "[[bin]]",
            'name = "guide-en"',
            'path = "src/bin/guide-en.rs"',
            "",
            "[[bin]]",
            'name = "guide-zh"',
            'path = "src/bin/guide-zh.rs"',
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, required=True, help="rs-lock project root")
    args = parser.parse_args()
    root = args.project.resolve()
    manifest_path = root / "Cargo.toml"
    try:
        with manifest_path.open("rb") as manifest_file:
            metadata = tomllib.load(manifest_file)
        guides = {
            "guide-en": root / "doc/user_guide.md",
            "guide-zh": root / "doc/user_guide.zh_CN.md",
        }
        fragments = {
            name: extract_fragments(path.read_text(encoding="utf-8"), path)
            for name, path in guides.items()
        }
    except (OSError, KeyError, TypeError, ValueError, tomllib.TOMLDecodeError) as error:
        parser.error(str(error))

    with tempfile.TemporaryDirectory(prefix="rs-lock-guides-") as temporary_root:
        temporary_root = Path(temporary_root)
        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = str(temporary_root / "target")
        for scenario in SCENARIOS:
            project = temporary_root / scenario
            (project / "src/bin").mkdir(parents=True)
            (project / "Cargo.toml").write_text(
                project_manifest(root, metadata, scenario), encoding="utf-8"
            )
            for name, source in guides.items():
                (project / f"src/bin/{name}.rs").write_text(
                    assemble(fragments[name], scenario), encoding="utf-8"
                )
            command = [
                "cargo",
                "check",
                "--manifest-path",
                str(project / "Cargo.toml"),
                "--bins",
            ]
            try:
                subprocess.run(
                    command,
                    check=True,
                    env=environment,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as error:
                print(
                    f"scenario {scenario} failed; sources: {guides['guide-en']} and "
                    f"{guides['guide-zh']}; temporary project: {project}",
                    file=sys.stderr,
                )
                if error.stdout:
                    print(error.stdout, file=sys.stderr, end="")
                if error.stderr:
                    print(error.stderr, file=sys.stderr, end="")
                return error.returncode
            print(f"scenario {scenario}: both guide binaries compiled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
