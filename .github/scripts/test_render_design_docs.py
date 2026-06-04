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
            self.assertIn('<div class="table-scroll"><table class="responsive-table">', html)
            self.assertIn('<table class="responsive-table">', html)
            self.assertIn('data-label="Scenario"', html)
            self.assertIn('data-label="Expected behavior"', html)
            self.assertIn("@media (max-width: 720px)", html)
            self.assertIn("grid-template-columns: minmax(6.75rem, 32%) minmax(0, 1fr)", html)
            self.assertIn("overflow-x: auto;", html)
            self.assertIn("white-space: nowrap;", html)
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
            self.assertIn("width: min(100%, 1360px);", html)
            self.assertIn("padding: clamp(14px, 2.6vw, 24px);", html)
            self.assertIn("grid-template-columns: 280px minmax(0, 1fr);", html)
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

    def test_design_docs_render_as_english_site(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            docs = source / "docs"
            docs.mkdir(parents=True)
            (docs / "PRD.md").write_text(
                "# Redis Stream Coordinator Design\n\nThis is the English design document.\n",
                encoding="utf-8",
            )

            self._run_renderer(source, output)

            html = (output / "docs" / "PRD.html").read_text(encoding="utf-8")
            self.assertIn('<html lang="en">', html)
            self.assertIn("This is the English design document.", html)
            self.assertFalse((output / "docs" / "en" / "PRD.html").exists())

    def test_design_docs_publish_korean_mirror_with_query_redirect(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            docs = source / "docs"
            korean_docs = docs / "ko"
            prd = docs / "prd"
            korean_prd = korean_docs / "prd"
            prd.mkdir(parents=True)
            korean_prd.mkdir(parents=True)
            (docs / "PRD.md").write_text(
                "# Redis Stream Coordinator Design\n\n[Architecture](prd/02-coordinator-architecture.md)\n",
                encoding="utf-8",
            )
            (prd / "02-coordinator-architecture.md").write_text("# Coordinator Architecture\n", encoding="utf-8")
            (korean_docs / "PRD.md").write_text(
                "# Redis Stream Coordinator Design\n\n[아키텍처](prd/02-coordinator-architecture.md)\n",
                encoding="utf-8",
            )
            (korean_prd / "02-coordinator-architecture.md").write_text("# Coordinator Architecture\n\n한글 본문\n", encoding="utf-8")

            self._run_renderer(source, output)

            english_index = (output / "index.html").read_text(encoding="utf-8")
            korean_index = (output / "ko" / "index.html").read_text(encoding="utf-8")
            english_prd = (output / "docs" / "PRD.html").read_text(encoding="utf-8")
            korean_prd_html = (output / "ko" / "docs" / "PRD.html").read_text(encoding="utf-8")
            self.assertIn('<html lang="en">', english_index)
            self.assertIn('targetLanguage === "ko"', english_index)
            self.assertIn('new URL("ko/index.html"', english_index)
            self.assertIn('href="ko/index.html"', english_index)
            self.assertIn('<html lang="ko">', korean_index)
            self.assertIn('targetLanguage === "en"', korean_index)
            self.assertIn('new URL("../index.html"', korean_index)
            self.assertIn('href="../index.html"', korean_index)
            self.assertIn('href="../ko/docs/PRD.html"', english_prd)
            self.assertIn('href="docs/prd/02-coordinator-architecture.html"', korean_index)
            self.assertIn('href="prd/02-coordinator-architecture.html"', korean_prd_html)
            self.assertIn("한글 본문", (output / "ko" / "docs" / "prd" / "02-coordinator-architecture.html").read_text(encoding="utf-8"))

    def test_design_docs_exclude_implementation_status_from_site_navigation(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            docs = source / "docs"
            docs.mkdir(parents=True)
            (docs / "PRD.md").write_text("# Redis Stream Coordinator Design\n", encoding="utf-8")
            (docs / "implementation-status.md").write_text("# Implementation Status\n", encoding="utf-8")

            self._run_renderer(source, output)

            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertNotIn("Implementation Status", html)
            self.assertFalse((output / "docs" / "implementation-status.html").exists())

    def test_design_docs_publish_scalar_api_reference_when_openapi_spec_exists(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as output_dir:
            source = Path(source_dir)
            output = Path(output_dir)
            docs = source / "docs"
            korean_docs = docs / "ko"
            openapi = docs / "openapi"
            docs.mkdir(parents=True)
            korean_docs.mkdir(parents=True)
            openapi.mkdir(parents=True)
            (docs / "PRD.md").write_text("# Redis Stream Coordinator Design\n\n[Scalar API Reference](../api.html)\n", encoding="utf-8")
            (korean_docs / "PRD.md").write_text("# Redis Stream Coordinator Design\n\n[Scalar API Reference](../api.html)\n", encoding="utf-8")
            (openapi / "coordinator.v1.yaml").write_text("openapi: 3.1.0\ninfo:\n  title: API\n  version: v1\npaths: {}\n", encoding="utf-8")

            self._run_renderer(source, output)

            index_html = (output / "index.html").read_text(encoding="utf-8")
            prd_html = (output / "docs" / "PRD.html").read_text(encoding="utf-8")
            korean_index_html = (output / "ko" / "index.html").read_text(encoding="utf-8")
            api_html = (output / "api.html").read_text(encoding="utf-8")
            korean_api_html = (output / "ko" / "api.html").read_text(encoding="utf-8")
            self.assertIn('href="api.html">API Reference</a>', index_html)
            self.assertIn('href="api.html">Scalar API Reference</a>', index_html)
            self.assertIn('href="../api.html">Scalar API Reference</a>', prd_html)
            self.assertIn('href="api.html">Scalar API Reference</a>', korean_index_html)
            self.assertIn("https://cdn.jsdelivr.net/npm/@scalar/api-reference", api_html)
            self.assertIn('url: "openapi/coordinator.v1.yaml"', api_html)
            self.assertIn('targetLanguage === "ko"', api_html)
            self.assertIn('url: "../openapi/coordinator.v1.yaml"', korean_api_html)
            self.assertTrue((output / "openapi" / "coordinator.v1.yaml").exists())

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
