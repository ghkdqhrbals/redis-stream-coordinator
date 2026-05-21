#!/usr/bin/env python3
"""Render Markdown design documents into a small static HTML site."""

from __future__ import annotations

import argparse
import html
import re
from pathlib import Path

import markdown


def title_for(markdown_text: str, fallback: str) -> str:
    for line in markdown_text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def output_name(markdown_path: Path) -> str:
    if markdown_path.name == "index.md":
        return "index.html"
    return f"{markdown_path.stem}.html"


def rewrite_markdown_links(body: str) -> str:
    return re.sub(r'href="([^"#]+)\.md(#[^"]*)?"', r'href="\1.html\2"', body)


def render_page(title: str, body: str, nav: str) -> str:
    escaped_title = html.escape(title)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{escaped_title}</title>
    <style>
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
      }}
      header {{
        border-bottom: 1px solid var(--border);
        background: var(--panel);
      }}
      .wrap {{
        max-width: 1120px;
        margin: 0 auto;
        padding: 24px;
      }}
      .layout {{
        display: grid;
        grid-template-columns: 260px minmax(0, 1fr);
        gap: 36px;
        align-items: start;
      }}
      nav {{
        position: sticky;
        top: 16px;
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px;
        background: #ffffff;
      }}
      nav a {{
        display: block;
        color: var(--accent);
        text-decoration: none;
        margin: 8px 0;
      }}
      main {{
        min-width: 0;
      }}
      h1, h2, h3 {{
        line-height: 1.25;
      }}
      h1 {{
        font-size: 2.2rem;
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
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin: 1rem 0;
      }}
      th, td {{
        border: 1px solid var(--border);
        padding: 8px 10px;
        vertical-align: top;
      }}
      th {{
        background: var(--panel);
      }}
      code {{
        background: var(--code);
        border-radius: 4px;
        padding: 0.1rem 0.25rem;
      }}
      pre {{
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
      @media (max-width: 820px) {{
        .layout {{
          display: block;
        }}
        nav {{
          position: static;
          margin-bottom: 24px;
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
    <div class="wrap layout">
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
    for markdown_path in markdown_files:
        text = markdown_path.read_text(encoding="utf-8")
        title = title_for(text, markdown_path.stem.replace("-", " ").title())
        pages.append((markdown_path, title, output_name(markdown_path)))

    nav = "\n".join(
        f'        <a href="{html.escape(file_name)}">{html.escape(title)}</a>'
        for _, title, file_name in pages
    )

    renderer = markdown.Markdown(extensions=["extra", "tables", "toc", "fenced_code", "sane_lists"])
    for markdown_path, title, file_name in pages:
        text = markdown_path.read_text(encoding="utf-8")
        body = rewrite_markdown_links(renderer.reset().convert(text))
        (destination / file_name).write_text(render_page(title, body, nav), encoding="utf-8")

    if not (destination / "index.html").exists():
        first_page = pages[0][2]
        (destination / "index.html").write_text(
            render_page(
                "Redis Stream Coordinator Design Docs",
                f'<h1>Redis Stream Coordinator Design Docs</h1><p><a href="{html.escape(first_page)}">Open the design document</a>.</p>',
                nav,
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
