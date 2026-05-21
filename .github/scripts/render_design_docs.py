#!/usr/bin/env python3
"""Render Markdown design documents into a small static HTML site."""

from __future__ import annotations

import argparse
import html
import os
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import markdown


def title_for(markdown_text: str, fallback: str) -> str:
    for line in markdown_text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def output_path(source: Path, markdown_path: Path) -> Path:
    return markdown_path.relative_to(source).with_suffix(".html")


def relative_href(from_output: Path, to_output: Path) -> str:
    start = from_output.parent if str(from_output.parent) != "." else Path(".")
    return os.path.relpath(to_output, start=start).replace(os.sep, "/")


def rewrite_markdown_links(
    body: str,
    source_markdown: Path,
    current_output: Path,
    output_paths: dict[Path, Path],
) -> str:
    def replace(match: re.Match[str]) -> str:
        raw_href = html.unescape(match.group(1))
        parsed = urlsplit(raw_href)
        if parsed.scheme or parsed.netloc or parsed.path.startswith("/") or not parsed.path.endswith(".md"):
            return match.group(0)

        target_markdown = (source_markdown.parent / parsed.path).resolve()
        target_output = output_paths.get(target_markdown)
        if target_output is None:
            return match.group(0)

        rewritten = urlunsplit((
            "",
            "",
            relative_href(current_output, target_output),
            parsed.query,
            parsed.fragment,
        ))
        return f'href="{html.escape(rewritten, quote=True)}"'

    return re.sub(r'href="([^"]+)"', replace, body)


def render_page(title: str, body: str, nav: str) -> str:
    escaped_title = html.escape(title)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escaped_title}</title>
    <style>
      * {{
        box-sizing: border-box;
      }}
      :root {{
        color-scheme: light;
        --fg: #172026;
        --muted: #5f6c75;
        --border: #d6dde3;
        --panel: #f7f9fb;
        --accent: #0f766e;
        --code: #eef3f7;
      }}
      body {{
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        color: var(--fg);
        background: #ffffff;
        line-height: 1.62;
        overflow-x: hidden;
      }}
      header {{
        border-bottom: 1px solid var(--border);
        background: var(--panel);
      }}
      .wrap {{
        max-width: 920px;
        margin: 0 auto;
        padding: 24px clamp(16px, 4vw, 32px);
      }}
      nav {{
        border-bottom: 1px solid var(--border);
        margin-bottom: 28px;
        padding-bottom: 12px;
      }}
      nav a {{
        display: inline-block;
        color: var(--accent);
        text-decoration: none;
        margin: 0 16px 8px 0;
        overflow-wrap: anywhere;
      }}
      main {{
        min-width: 0;
        max-width: 100%;
      }}
      h1, h2, h3 {{
        line-height: 1.25;
        overflow-wrap: anywhere;
      }}
      h1 {{
        font-size: clamp(1.75rem, 6vw, 2.35rem);
        margin-top: 0;
      }}
      h2 {{
        margin-top: 2.1rem;
        border-bottom: 1px solid var(--border);
        padding-bottom: 0.35rem;
      }}
      h3 {{
        margin-top: 1.6rem;
      }}
      a {{
        color: var(--accent);
        overflow-wrap: anywhere;
      }}
      table {{
        display: block;
        width: 100%;
        max-width: 100%;
        overflow-x: auto;
        border-collapse: collapse;
        margin: 1rem 0;
      }}
      th, td {{
        border: 1px solid var(--border);
        padding: 8px 10px;
        vertical-align: top;
        overflow-wrap: anywhere;
      }}
      th {{
        background: var(--panel);
      }}
      code {{
        background: var(--code);
        border-radius: 4px;
        padding: 0.1rem 0.25rem;
        overflow-wrap: anywhere;
      }}
      pre {{
        max-width: 100%;
        overflow-x: auto;
        background: var(--code);
        border-radius: 8px;
        padding: 16px;
      }}
      pre code {{
        padding: 0;
      }}
      blockquote {{
        margin: 1rem 0;
        padding: 0.3rem 1rem;
        border-left: 4px solid var(--accent);
        color: var(--muted);
        background: var(--panel);
      }}
      img, svg {{
        max-width: 100%;
        height: auto;
      }}
      @media (max-width: 640px) {{
        .wrap {{
          padding-top: 18px;
          padding-bottom: 18px;
        }}
        th, td {{
          min-width: 140px;
        }}
      }}
    </style>
  </head>
  <body>
    <header>
      <div class="wrap">
        <strong>Redis Stream Coordinator Design Docs</strong>
      </div>
    </header>
    <div class="wrap">
      <nav aria-label="Design document navigation">
{nav}
      </nav>
      <main>
{body}
      </main>
    </div>
  </body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    destination.mkdir(parents=True, exist_ok=True)

    markdown_files = sorted(
        path for path in source.rglob("*.md")
        if ".git" not in path.parts
    )
    if not markdown_files:
        raise SystemExit(f"No Markdown design documents found in {source}")

    pages = []
    output_paths = {}
    for markdown_path in markdown_files:
        text = markdown_path.read_text(encoding="utf-8")
        title = title_for(text, markdown_path.stem.replace("-", " ").title())
        resolved_markdown_path = markdown_path.resolve()
        rendered_path = output_path(source, markdown_path)
        output_paths[resolved_markdown_path] = rendered_path
        pages.append((markdown_path, title, rendered_path))

    def nav_for(current_output: Path) -> str:
        return "\n".join(
            f'        <a href="{html.escape(relative_href(current_output, file_name))}">{html.escape(title)}</a>'
            for _, title, file_name in pages
        )

    renderer = markdown.Markdown(extensions=["extra", "tables", "toc", "fenced_code", "sane_lists"])
    for markdown_path, title, file_name in pages:
        text = markdown_path.read_text(encoding="utf-8")
        body = rewrite_markdown_links(
            renderer.reset().convert(text),
            markdown_path.resolve(),
            file_name,
            output_paths,
        )
        target = destination / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_page(title, body, nav_for(file_name)), encoding="utf-8")

    if not (destination / "index.html").exists():
        first_page = pages[0][2]
        (destination / "index.html").write_text(
            render_page(
                "Redis Stream Coordinator Design Docs",
                f'<h1>Redis Stream Coordinator Design Docs</h1><p><a href="{html.escape(first_page.as_posix())}">Open the design document</a>.</p>',
                nav_for(Path("index.html")),
            ),
            encoding="utf-8",
        )

    asset_names = {"png", "jpg", "jpeg", "gif", "svg", "webp"}
    for asset in source.rglob("*"):
        if asset.is_file() and asset.suffix.lower().lstrip(".") in asset_names:
            relative = asset.relative_to(source)
            target = destination / re.sub(r"[^A-Za-z0-9._/-]", "-", str(relative))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(asset.read_bytes())


if __name__ == "__main__":
    main()
