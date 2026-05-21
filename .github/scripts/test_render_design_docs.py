#!/usr/bin/env python3
"""Unit tests for render_design_docs.py."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from render_design_docs import main


class RenderDesignDocsTest(unittest.TestCase):
    def test_rewrites_nested_markdown_links_to_rendered_html_paths(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text(
                "# Index\n\n- [Nested Guide](nested/guide.md)\n",
                encoding="utf-8",
            )
            nested = source / "nested"
            nested.mkdir()
            (nested / "guide.md").write_text(
                "# Nested Guide\n\n[Back](../index.md)\n",
                encoding="utf-8",
            )

            self._run_renderer(source, output)

            index_html = (output / "index.html").read_text(encoding="utf-8")
            nested_html = (output / "nested" / "guide.html").read_text(encoding="utf-8")
            self.assertIn('href="nested/guide.html"', index_html)
            self.assertIn('href="../index.html"', nested_html)

    def test_tables_collapse_with_cell_labels_on_small_viewports(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text(
                "\n".join([
                    "# Responsive Table",
                    "",
                    "| Scenario | Expected behavior |",
                    "| --- | --- |",
                    "| Coordinator restart | Rebooks pending revocations without duplicate owners. |",
                ]),
                encoding="utf-8",
            )

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn('<table class="responsive-table">', html)
            self.assertIn('data-label="Scenario"', html)
            self.assertIn('data-label="Expected behavior"', html)
            self.assertIn("@media (max-width: 720px)", html)
            self.assertIn("grid-template-columns: minmax(7.5rem, 34%) minmax(0, 1fr)", html)
            self.assertNotIn("overflow-x: hidden", html)

    def _run_renderer(self, source: Path, output: Path) -> None:
        import sys

        previous_argv = sys.argv
        try:
            sys.argv = ["render_design_docs.py", str(source), str(output)]
            main()
        finally:
            sys.argv = previous_argv


if __name__ == "__main__":
    unittest.main()
