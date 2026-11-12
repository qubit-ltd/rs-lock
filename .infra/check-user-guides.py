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


SCENARIOS = {
    "blocking": {
        "fragments": ("queue-state", "queue-ops", "blocking-main"),
        "lock_features": ("monitor", "parking-lot"),
        "needs_manual_clock": False,
        "tokio_features": (),
    },
    "manual": {
        "fragments": ("queue-state", "queue-ops", "timed-ops", "manual-main"),
        "lock_features": ("monitor", "parking-lot"),
        "needs_manual_clock": True,
        "tokio_features": (),
    },
    "tokio": {
        "fragments": ("queue-state", "tokio-main"),
        "lock_features": ("async-monitor",),
        "needs_manual_clock": False,
        "tokio_features": ("macros", "rt-multi-thread", "time"),
    },
    "async-data-lock": {
        "fragments": ("async-data-lock",),
        "lock_features": ("async-lock",),
        "needs_manual_clock": False,
        "tokio_features": ("macros", "rt-multi-thread"),
    },
    "async-rw-lock": {
        "fragments": ("async-rw-lock",),
        "lock_features": ("async-lock",),
        "needs_manual_clock": False,
        "tokio_features": ("macros", "rt-multi-thread"),
    },
    "deadline-blocking": {
        "fragments": ("deadline-blocking",),
        "lock_features": ("monitor",),
        "needs_manual_clock": False,
        "tokio_features": (),
    },
    "deadline-async": {
        "fragments": ("deadline-async",),
        "lock_features": ("async-monitor",),
        "needs_manual_clock": False,
        "tokio_features": ("macros", "rt-multi-thread", "time"),
    },
    "external-state-blocking": {
        "fragments": ("external-state-blocking",),
        "lock_features": ("monitor",),
        "needs_manual_clock": False,
        "tokio_features": (),
    },
    "external-state-async": {
        "fragments": ("external-state-async",),
        "lock_features": ("async-monitor",),
        "needs_manual_clock": False,
        "tokio_features": ("macros", "rt-multi-thread", "time"),
    },
}
FRAGMENT_NAMES = frozenset(
    (
        "queue-state", "queue-ops", "blocking-main", "timed-ops", "manual-main", "tokio-main",
        "async-data-lock", "async-rw-lock", "deadline-blocking", "deadline-async",
        "external-state-blocking", "external-state-async",
    )
)
MARKER = re.compile(r"^<!-- rs-lock-example: ([a-z-]+) -->\s*$", re.MULTILINE)


def extract_fragments(content: str, source: str) -> dict[str, str]:
    """Extract every uniquely marked Rust block, requiring its adjacent fence."""
    markers = list(MARKER.finditer(content))
    result = {}
    errors = []
    for name in sorted(FRAGMENT_NAMES):
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
        next_marker = next(
            (candidate.start() for candidate in markers if candidate.start() > marker.start()),
            len(content),
        )
        closing = re.search(r"^```\s*$", content[body_start:next_marker], re.MULTILINE)
        if closing is None:
            errors.append(f"{name}: Rust fence is not closed")
            continue
        body = content[body_start:body_start + closing.start()].strip()
        if not body:
            errors.append(f"{name}: Rust block is empty")
            continue
        result[name] = body
    unexpected = sorted({marker.group(1) for marker in markers} - FRAGMENT_NAMES)
    if unexpected:
        errors.append(f"unexpected marker(s): {', '.join(unexpected)}")
    if errors:
        raise ValueError(f"{source}: " + "; ".join(errors))
    return result


def validate_scenario_coverage() -> None:
    """Ensure every required marker is compiled by at least one scenario."""
    referenced = {
        fragment for scenario in SCENARIOS.values() for fragment in scenario["fragments"]
    }
    missing = FRAGMENT_NAMES - referenced
    unknown = referenced - FRAGMENT_NAMES
    if missing or unknown:
        details = []
        if missing:
            details.append(f"unreferenced marker(s): {', '.join(sorted(missing))}")
        if unknown:
            details.append(f"unknown scenario fragment(s): {', '.join(sorted(unknown))}")
        raise ValueError("scenario coverage: " + "; ".join(details))


def assemble(fragments, scenario):
    """Join the original guide fragments in scenario order."""
    return "\n\n".join(fragments[name] for name in SCENARIOS[scenario]["fragments"]) + "\n"


def toml_string(value: str) -> str:
    """Encode a TOML basic string using JSON's compatible escaping."""
    return json.dumps(value, ensure_ascii=True)


def project_manifest(root: Path, metadata: dict, scenario: str) -> str:
    package = metadata["package"]
    dependencies = metadata["dependencies"]
    lock_dep = dependencies["qubit-clock"]
    lock_version = lock_dep if isinstance(lock_dep, str) else lock_dep["version"]
    lock_path = toml_string(str(root))
    config = SCENARIOS[scenario]
    lines = [
        "[workspace]",
        "",
        "[package]",
        'name = "rs-lock-guide-check"',
        'version = "0.0.0"',
        f"edition = {toml_string(package['edition'])}",
        "",
        "[dependencies]",
        f"{toml_string(package['name'])} = {{ path = {lock_path}, default-features = false, features = {json.dumps(config['lock_features'])} }}",
    ]
    if config["needs_manual_clock"]:
        lines.append(
            f'qubit-clock = {{ version = {toml_string(lock_version)}, features = ["test-util"] }}'
        )
    if config["tokio_features"]:
        tokio_dep = dependencies["tokio"]
        tokio_version = tokio_dep if isinstance(tokio_dep, str) else tokio_dep["version"]
        lines.append(
            f'tokio = {{ version = {toml_string(tokio_version)}, default-features = false, '
            f"features = {json.dumps(config['tokio_features'])} }}"
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
        validate_scenario_coverage()
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
            command = ["cargo", "check", "--manifest-path", str(project / "Cargo.toml"), "--bins"]
            try:
                subprocess.run(command, check=True, env=environment, capture_output=True, text=True)
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
