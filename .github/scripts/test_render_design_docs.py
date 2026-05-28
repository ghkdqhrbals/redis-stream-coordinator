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
            self.assertIn("grid-template-columns: minmax(6.75rem, 32%) minmax(0, 1fr)", html)
            self.assertNotIn("overflow-x: hidden", html)

    def test_uses_compact_kip_like_document_spacing(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text("# Design Doc\n\n## Section\n\nBody\n", encoding="utf-8")

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("font-size: clamp(0.875rem, 0.84rem + 0.12vw, 0.95rem);", html)
            self.assertIn("line-height: 1.5;", html)
            self.assertIn("width: min(100%, 1180px);", html)
            self.assertIn("padding: clamp(14px, 2.6vw, 24px);", html)
            self.assertIn("grid-template-columns: 260px minmax(0, 1fr);", html)
            self.assertIn("font-size: clamp(1.45rem, 1.2rem + 0.9vw, 1.9rem);", html)
            self.assertIn("font-size: 0.93em;", html)

    def test_shows_vertical_index_sidebar_before_document_body(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text("# Design Doc\n", encoding="utf-8")
            (source / "rebalance.md").write_text("# Rebalance Protocol\n", encoding="utf-8")

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn('<aside class="sidebar" aria-label="Design document index">', html)
            self.assertIn('<div class="index-title">Pages</div>', html)
            self.assertIn('<nav class="doc-index" aria-label="Design document navigation">', html)
            self.assertLess(html.index('<aside class="sidebar"'), html.index("<main>"))
            self.assertLess(html.index('href="rebalance.html"'), html.index("<main>"))

    def test_design_docs_publish_prd_as_landing_page(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            docs = source / "docs"
            prd = docs / "prd"
            prd.mkdir(parents=True)
            (docs / "PRD.md").write_text(
                "# Redis Stream Coordinator Design\n\n[Architecture](prd/02-coordinator-architecture.md)\n",
                encoding="utf-8",
            )
            (prd / "02-coordinator-architecture.md").write_text("# Coordinator Architecture\n", encoding="utf-8")
            (source / "README.md").write_text("# Repository README\n", encoding="utf-8")

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("<h1 id=\"redis-stream-coordinator-design\">Redis Stream Coordinator Design</h1>", html)
            self.assertIn('href="docs/prd/02-coordinator-architecture.html"', html)
            self.assertIn('aria-current="page">Redis Stream Coordinator Design</a>', html)
            self.assertFalse((output / "README.html").exists())

    def test_heading_levels_do_not_render_underline_rules(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text(
                "# Design Doc\n\n## Section\n\n### Scenario\n\n#### Detail\n\nBody\n",
                encoding="utf-8",
            )

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("h1, h2, h3, h4", html)
            for selector in ("h1", "h2", "h3", "h4"):
                rule = html.split(f"{selector} {{", 1)[1].split("}", 1)[0]
                self.assertNotIn("border-bottom", rule)
                self.assertNotIn("text-decoration", rule)

    def test_uses_samsung_blue_as_accent_color(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            (source / "index.md").write_text("# Design Doc\n", encoding="utf-8")

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn("--accent: #1428a0;", html)
            self.assertNotIn("--accent: #0f766e;", html)

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
