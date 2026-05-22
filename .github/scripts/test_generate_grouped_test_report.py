#!/usr/bin/env python3
"""Unit tests for generate_grouped_test_report.py."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from generate_grouped_test_report import main


class GenerateGroupedTestReportTest(unittest.TestCase):
    def test_groups_operational_matrix_cases_as_nested_details(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            self._write_xml(
                source,
                "io.github.ghkdqhrbals.redisstreamcoordinator.CoordinatorOperationalScenarioMatrixTest",
                [
                    '<testcase classname="Matrix" name="scale up | add member | weighted capacity | 6 shards, 3 members" time="0.001"/>',
                    '<testcase classname="Matrix" name="scale up | add member | uniform capacity | 6 shards, 3 members" time="0.002"/>',
                ],
            )

            self._run_reporter(source, output / "index.html")

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("CoordinatorOperationalScenarioMatrixTest", html)
            self.assertIn("<summary>scale up", html)
            self.assertIn("<summary>add member", html)
            self.assertIn("<summary>weighted capacity", html)
            self.assertIn("6 shards, 3 members", html)
            self.assertIn('href="gradle/index.html"', html)

    def test_opens_failed_groups_and_renders_failure_detail(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            self._write_xml(
                source,
                "ExampleTest",
                [
                    '<testcase classname="ExampleTest" name="fails clearly" time="0.003">'
                    '<failure message="expected true">stack trace</failure>'
                    "</testcase>",
                ],
                failures=1,
            )

            self._run_reporter(source, output / "index.html")

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn('<details class="suite" open>', html)
            self.assertIn("<summary>Failed", html)
            self.assertIn("expected true", html)

    def test_groups_nested_junit_classes_by_category(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            self._write_xml(
                source,
                "io.github.CoordinatorGroupedWorkflowTest$MemberExpired",
                [
                    '<testcase classname="io.github.CoordinatorGroupedWorkflowTest$MemberExpired" '
                    'name="expired owner is reassigned" time="0.003"/>',
                ],
            )

            self._run_reporter(source, output / "index.html")

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("<summary>Member Expired", html)
            self.assertIn("expired owner is reassigned", html)

    def _write_xml(
        self,
        source: Path,
        suite_name: str,
        testcases: list[str],
        failures: int = 0,
    ) -> None:
        source.mkdir(parents=True, exist_ok=True)
        content = "\n".join(
            [
                '<?xml version="1.0" encoding="UTF-8"?>',
                f'<testsuite name="{suite_name}" tests="{len(testcases)}" skipped="0" failures="{failures}" errors="0" time="0.01">',
                *testcases,
                "</testsuite>",
            ]
        )
        (source / f"TEST-{suite_name}.xml").write_text(content, encoding="utf-8")

    def _run_reporter(self, source: Path, output: Path) -> None:
        import sys

        previous_argv = sys.argv
        try:
            sys.argv = ["generate_grouped_test_report.py", str(source), str(output)]
            main()
        finally:
            sys.argv = previous_argv


if __name__ == "__main__":
    unittest.main()
