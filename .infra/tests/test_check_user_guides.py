"""Unit tests for user-guide extraction, coverage, and project assembly."""

import importlib.util
from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_user_guides", ROOT / ".infra/check-user-guides.py"
)
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


def markdown(blocks):
    return "\n\n".join(
        f"<!-- rs-lock-example: {name} -->\n```rust\n{body}\n```"
        for name, body in blocks.items()
    )


class ExtractFragmentsTests(unittest.TestCase):
    def valid_blocks(self):
        return {name: f"fn main() {{ let _ = {index}; }}" for index, name in enumerate(checker.FRAGMENT_NAMES)}

    def test_extracts_original_content(self):
        blocks = self.valid_blocks()
        extracted = checker.extract_fragments(markdown(blocks), "guide.md")
        self.assertEqual(extracted, blocks)

    def test_missing_marker_has_source_and_name(self):
        blocks = self.valid_blocks()
        del blocks["async-data-lock"]
        with self.assertRaisesRegex(ValueError, r"guide\.md:.*async-data-lock: expected one marker"):
            checker.extract_fragments(markdown(blocks), "guide.md")

    def test_repeated_marker_is_rejected(self):
        blocks = self.valid_blocks()
        source = markdown(blocks) + "\n\n" + markdown({"async-data-lock": "fn main() {}"})
        with self.assertRaisesRegex(ValueError, "async-data-lock: expected one marker, found 2"):
            checker.extract_fragments(source, "guide.md")

    def test_unknown_marker_is_rejected(self):
        source = markdown(self.valid_blocks()) + "\n\n<!-- rs-lock-example: unknown-case -->\n```rust\nfn main() {}\n```"
        with self.assertRaisesRegex(ValueError, "unexpected marker\\(s\\): unknown-case"):
            checker.extract_fragments(source, "guide.md")

    def test_marker_without_adjacent_rust_fence_is_rejected(self):
        source = markdown(self.valid_blocks()).replace(
            "<!-- rs-lock-example: async-data-lock -->\n```rust",
            "<!-- rs-lock-example: async-data-lock -->\ntext\n```rust",
        )
        with self.assertRaisesRegex(ValueError, "async-data-lock: marker must be followed immediately"):
            checker.extract_fragments(source, "guide.md")

    def test_empty_block_is_rejected(self):
        blocks = self.valid_blocks()
        blocks["async-data-lock"] = "  \n  "
        with self.assertRaisesRegex(ValueError, "async-data-lock: Rust block is empty"):
            checker.extract_fragments(markdown(blocks), "guide.md")

    def test_unclosed_fence_is_rejected(self):
        source = "<!-- rs-lock-example: async-data-lock -->\n```rust\nfn main() {}"
        with self.assertRaisesRegex(ValueError, "async-data-lock: Rust fence is not closed"):
            checker.extract_fragments(source, "guide.md")


class ScenarioAssemblyTests(unittest.TestCase):
    def test_every_required_fragment_is_referenced(self):
        original = checker.SCENARIOS["async-data-lock"]
        try:
            checker.SCENARIOS["async-data-lock"] = {
                **original,
                "fragments": (),
            }
            with self.assertRaisesRegex(ValueError, "unreferenced marker.*async-data-lock"):
                checker.validate_scenario_coverage()
        finally:
            checker.SCENARIOS["async-data-lock"] = original

    def test_nine_scenarios_have_required_fragments_and_features(self):
        self.assertEqual(len(checker.SCENARIOS), 9)
        for scenario in ("async-data-lock", "async-rw-lock"):
            config = checker.SCENARIOS[scenario]
            self.assertEqual(config["fragments"], (scenario,))
            self.assertEqual(config["lock_features"], ("async-lock",))
            self.assertEqual(config["tokio_features"], ("macros", "rt-multi-thread"))
        for scenario in ("deadline-blocking", "external-state-blocking"):
            config = checker.SCENARIOS[scenario]
            self.assertEqual(config["fragments"], (scenario,))
            self.assertEqual(config["lock_features"], ("monitor",))
            self.assertFalse(config["tokio_features"])
        for scenario in ("deadline-async", "external-state-async"):
            config = checker.SCENARIOS[scenario]
            self.assertEqual(config["fragments"], (scenario,))
            self.assertEqual(config["lock_features"], ("async-monitor",))
            self.assertEqual(config["tokio_features"], ("macros", "rt-multi-thread", "time"))
        self.assertEqual(
            checker.SCENARIOS["blocking"]["fragments"],
            ("queue-state", "queue-ops", "blocking-main"),
        )
        self.assertEqual(
            checker.SCENARIOS["manual"]["fragments"],
            ("queue-state", "queue-ops", "timed-ops", "manual-main"),
        )
        self.assertEqual(checker.SCENARIOS["tokio"]["fragments"], ("queue-state", "tokio-main"))

    def test_manifests_select_minimal_features_and_direct_dependencies(self):
        with (ROOT / "Cargo.toml").open("rb") as manifest:
            metadata = tomllib.load(manifest)
        expected = {
            "async-data-lock": ({"async-lock"}, {"macros", "rt-multi-thread"}),
            "async-rw-lock": ({"async-lock"}, {"macros", "rt-multi-thread"}),
            "deadline-blocking": ({"monitor"}, set()),
            "external-state-blocking": ({"monitor"}, set()),
            "deadline-async": ({"async-monitor"}, {"macros", "rt-multi-thread", "time"}),
            "external-state-async": ({"async-monitor"}, {"macros", "rt-multi-thread", "time"}),
        }
        for scenario, (lock_features, tokio_features) in expected.items():
            with self.subTest(scenario=scenario):
                generated = checker.project_manifest(ROOT, metadata, scenario)
                parsed = tomllib.loads(generated)
                lock = parsed["dependencies"][metadata["package"]["name"]]
                self.assertEqual(set(lock["features"]), lock_features)
                self.assertFalse(lock["default-features"])
                self.assertEqual("tokio" in parsed["dependencies"], bool(tokio_features))
                if tokio_features:
                    tokio = parsed["dependencies"]["tokio"]
                    self.assertEqual(set(tokio["features"]), tokio_features)
                    self.assertFalse(tokio["default-features"])
                self.assertEqual(
                    parsed["bin"],
                    [
                        {"name": "guide-en", "path": "src/bin/guide-en.rs"},
                        {"name": "guide-zh", "path": "src/bin/guide-zh.rs"},
                    ],
                )
                self.assertEqual(parsed["package"]["edition"], metadata["package"]["edition"])
        manual = tomllib.loads(checker.project_manifest(ROOT, metadata, "manual"))
        self.assertIn("test-util", manual["dependencies"]["qubit-clock"]["features"])

    def test_both_guides_have_same_markers_and_assembly_uses_source(self):
        english = checker.extract_fragments((ROOT / "doc/user_guide.md").read_text(), "guide-en")
        chinese = checker.extract_fragments((ROOT / "doc/user_guide.zh_CN.md").read_text(), "guide-zh")
        self.assertEqual(set(english), set(chinese))
        for scenario, config in checker.SCENARIOS.items():
            with self.subTest(scenario=scenario):
                source = english[config["fragments"][0]]
                self.assertIn(source, checker.assemble(english, scenario))
                self.assertEqual(
                    checker.assemble(english, scenario),
                    "\n\n".join(english[name] for name in config["fragments"]) + "\n",
                )


if __name__ == "__main__":
    unittest.main()
