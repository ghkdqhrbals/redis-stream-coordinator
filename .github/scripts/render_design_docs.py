#!/usr/bin/env python3
"""Render Markdown design documents into a small static HTML site."""

from __future__ import annotations

import argparse
import html
import os
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import markdown


DESIGN_DOC_EXTRAS = (
    Path("docs/implementation-status.md"),
    Path("docs/operations-runbook.md"),
    Path("docs/testing.md"),
)


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


def render_start_tag(tag: str, attrs: list[tuple[str, str | None]], self_closing: bool = False) -> str:
    rendered_attrs = []
    for name, value in attrs:
        if value is None:
            rendered_attrs.append(f" {name}")
        else:
            rendered_attrs.append(f' {name}="{html.escape(value, quote=True)}"')
    closing = " /" if self_closing else ""
    return f"<{tag}{''.join(rendered_attrs)}{closing}>"


def add_or_append_class(attrs: list[tuple[str, str | None]], class_name: str) -> list[tuple[str, str | None]]:
    next_attrs = []
    found = False
    for name, value in attrs:
        if name == "class":
            classes = value.split() if value else []
            if class_name not in classes:
                classes.append(class_name)
            next_attrs.append((name, " ".join(classes)))
            found = True
        else:
            next_attrs.append((name, value))
    if not found:
        next_attrs.append(("class", class_name))
    return next_attrs


def has_attr(attrs: list[tuple[str, str | None]], attr_name: str) -> bool:
    return any(name == attr_name for name, _ in attrs)


class ResponsiveTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.output: list[str] = []
        self.in_table = False
        self.in_thead = False
        self.in_header_cell = False
        self.header_chunks: list[str] = []
        self.headers: list[str] = []
        self.cell_index = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self.in_table = True
            self.headers = []
            self.cell_index = 0
            attrs = add_or_append_class(attrs, "responsive-table")
        elif self.in_table and tag == "thead":
            self.in_thead = True
        elif self.in_table and tag == "tr":
            self.cell_index = 0
        elif self.in_table and self.in_thead and tag == "th":
            self.in_header_cell = True
            self.header_chunks = []
        elif self.in_table and tag == "td":
            if self.cell_index < len(self.headers) and not has_attr(attrs, "data-label"):
                attrs = attrs + [("data-label", self.headers[self.cell_index])]
            self.cell_index += 1

        self.output.append(render_start_tag(tag, attrs))

    def handle_endtag(self, tag: str) -> None:
        if self.in_table and self.in_thead and tag == "th":
            label = re.sub(r"\s+", " ", "".join(self.header_chunks)).strip()
            self.headers.append(label)
            self.in_header_cell = False
            self.header_chunks = []

        self.output.append(f"</{tag}>")

        if self.in_table and tag == "thead":
            self.in_thead = False
        elif tag == "table":
            self.in_table = False
            self.in_thead = False
            self.in_header_cell = False
            self.header_chunks = []
            self.headers = []
            self.cell_index = 0

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.output.append(render_start_tag(tag, attrs, self_closing=True))

    def handle_data(self, data: str) -> None:
        if self.in_header_cell:
            self.header_chunks.append(data)
        self.output.append(data)

    def handle_entityref(self, name: str) -> None:
        entity = f"&{name};"
        if self.in_header_cell:
            self.header_chunks.append(html.unescape(entity))
        self.output.append(entity)

    def handle_charref(self, name: str) -> None:
        entity = f"&#{name};"
        if self.in_header_cell:
            self.header_chunks.append(html.unescape(entity))
        self.output.append(entity)

    def handle_comment(self, data: str) -> None:
        self.output.append(f"<!--{data}-->")

    def handle_decl(self, decl: str) -> None:
        self.output.append(f"<!{decl}>")


def make_tables_responsive(body: str) -> str:
    parser = ResponsiveTableParser()
    parser.feed(body)
    parser.close()
    return "".join(parser.output)


def discover_markdown_files(source: Path) -> list[Path]:
    primary = source / "docs" / "PRD.md"
    if primary.exists():
        ordered = [primary]
        ordered.extend(sorted((source / "docs" / "prd").glob("*.md")))
        ordered.extend(source / extra for extra in DESIGN_DOC_EXTRAS)
        seen: set[Path] = set()
        return [
            path
            for path in ordered
            if path.exists() and path not in seen and not seen.add(path)
        ]

    return sorted(
        path for path in source.rglob("*.md")
        if ".git" not in path.parts
    )


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
      html {{
        -webkit-text-size-adjust: 100%;
        text-size-adjust: 100%;
      }}
      :root {{
        color-scheme: light;
        --fg: #172026;
        --muted: #5f6c75;
        --border: #d6dde3;
        --panel: #f7f9fb;
        --accent: #1428a0;
        --code: #eef3f7;
        --sidebar: #fbfcfd;
      }}
      body {{
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        font-size: clamp(0.875rem, 0.84rem + 0.12vw, 0.95rem);
        color: var(--fg);
        background: #ffffff;
        line-height: 1.5;
      }}
      header {{
        border-bottom: 1px solid var(--border);
        background: var(--panel);
      }}
      .topbar {{
        width: min(100%, 1180px);
        margin: 0 auto;
        padding: 12px clamp(14px, 2.2vw, 24px);
      }}
      .site-title {{
        color: var(--fg);
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: 0;
      }}
      .site-subtitle {{
        color: var(--muted);
        font-size: 0.82rem;
        margin-top: 2px;
      }}
      .layout {{
        display: grid;
        grid-template-columns: 260px minmax(0, 1fr);
        gap: clamp(18px, 3vw, 32px);
        width: min(100%, 1180px);
        margin: 0 auto;
        padding: clamp(14px, 2.6vw, 24px);
        align-items: start;
      }}
      .sidebar {{
        position: sticky;
        top: 12px;
        align-self: start;
        max-height: calc(100vh - 24px);
        overflow: auto;
        padding: 4px 12px 12px 0;
        border-right: 1px solid var(--border);
        background: var(--sidebar);
      }}
      .doc-index {{
        display: flex;
        flex-direction: column;
        gap: 2px;
        margin-top: 8px;
      }}
      .doc-index a {{
        color: var(--accent);
        display: block;
        padding: 4px 8px;
        border-radius: 4px;
        text-decoration: none;
        overflow-wrap: anywhere;
      }}
      .doc-index a:hover,
      .doc-index a[aria-current="page"] {{
        background: var(--code);
      }}
      .index-title {{
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0;
        text-transform: uppercase;
      }}
      main {{
        min-width: 0;
        max-width: 100%;
        padding-bottom: 56px;
      }}
      h1, h2, h3, h4 {{
        line-height: 1.25;
        overflow-wrap: anywhere;
      }}
      h1 {{
        font-size: clamp(1.45rem, 1.2rem + 0.9vw, 1.9rem);
        margin-top: 0;
        margin-bottom: 0.8rem;
      }}
      h2 {{
        font-size: 1.3rem;
        margin-top: 1.65rem;
        margin-bottom: 0.65rem;
      }}
      h3 {{
        font-size: 1.08rem;
        margin-top: 1.2rem;
        margin-bottom: 0.45rem;
      }}
      h4 {{
        font-size: 1rem;
        margin-top: 1rem;
        margin-bottom: 0.4rem;
      }}
      p, ul, ol {{
        margin-top: 0.55rem;
        margin-bottom: 0.75rem;
      }}
      a {{
        color: var(--accent);
        overflow-wrap: anywhere;
      }}
      .responsive-table {{
        width: 100%;
        max-width: 100%;
        border-collapse: collapse;
        margin: 0.8rem 0;
        font-size: 0.93em;
        table-layout: auto;
      }}
      th, td {{
        border: 1px solid var(--border);
        padding: 6px 8px;
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
        border-radius: 4px;
        padding: 10px 12px;
        font-size: 0.93em;
      }}
      pre code {{
        padding: 0;
        white-space: pre;
      }}
      blockquote {{
        margin: 0.75rem 0;
        padding: 0.25rem 0.8rem;
        border-left: 4px solid var(--accent);
        color: var(--muted);
        background: var(--panel);
      }}
      img, svg {{
        max-width: 100%;
        height: auto;
      }}
      @media (max-width: 720px) {{
        .layout {{
          display: block;
        }}
        .sidebar {{
          position: static;
          max-height: none;
          border-right: 0;
          border-bottom: 1px solid var(--border);
          margin-bottom: 18px;
          padding: 0 0 14px;
        }}
        .doc-index {{
          max-height: 220px;
          overflow: auto;
        }}
        .responsive-table,
        .responsive-table thead,
        .responsive-table tbody,
        .responsive-table tr,
        .responsive-table th,
        .responsive-table td {{
          display: block;
          width: 100%;
        }}
        .responsive-table {{
          border: 1px solid var(--border);
        }}
        .responsive-table thead {{
          position: absolute;
          width: 1px;
          height: 1px;
          padding: 0;
          margin: -1px;
          overflow: hidden;
          clip: rect(0, 0, 0, 0);
          white-space: nowrap;
          border: 0;
        }}
        .responsive-table tr + tr {{
          border-top: 2px solid var(--border);
        }}
        .responsive-table td {{
          display: grid;
          grid-template-columns: minmax(6.75rem, 32%) minmax(0, 1fr);
          gap: 8px;
          border: 0;
          border-top: 1px solid var(--border);
          min-width: 0;
        }}
        .responsive-table td:first-child {{
          border-top: 0;
        }}
        .responsive-table td::before {{
          content: attr(data-label);
          color: var(--muted);
          font-weight: 600;
          overflow-wrap: anywhere;
        }}
        pre {{
          white-space: pre-wrap;
        }}
        pre code {{
          white-space: inherit;
        }}
      }}
      @media (max-width: 480px) {{
        .wrap {{
          padding-top: 12px;
          padding-bottom: 12px;
        }}
        nav {{
          display: block;
        }}
        nav a {{
          display: block;
          margin-bottom: 8px;
        }}
        .responsive-table td {{
          grid-template-columns: 1fr;
          gap: 2px;
        }}
      }}
    </style>
  </head>
  <body>
    <header>
      <div class="topbar">
        <div class="site-title">Redis Stream Coordinator</div>
        <div class="site-subtitle">Design docs</div>
      </div>
    </header>
    <div class="layout">
      <aside class="sidebar" aria-label="Design document index">
        <div class="index-title">Pages</div>
        <nav class="doc-index" aria-label="Design document navigation">
{nav}
        </nav>
      </aside>
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

    markdown_files = discover_markdown_files(source)
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

    def nav_for(current_output: Path, active_output: Path | None = None) -> str:
        active = active_output or current_output
        links = []
        for _, title, file_name in pages:
            current = file_name == active
            attrs = ' aria-current="page"' if current else ""
            links.append(
                f'          <a href="{html.escape(relative_href(current_output, file_name))}"{attrs}>'
                f'{html.escape(title)}</a>'
            )
        return "\n".join(links)

    renderer = markdown.Markdown(extensions=["extra", "tables", "toc", "fenced_code", "sane_lists"])
    for markdown_path, title, file_name in pages:
        text = markdown_path.read_text(encoding="utf-8")
        body = rewrite_markdown_links(
            renderer.reset().convert(text),
            markdown_path.resolve(),
            file_name,
            output_paths,
        )
        body = make_tables_responsive(body)
        target = destination / file_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_page(title, body, nav_for(file_name)), encoding="utf-8")

    if not (destination / "index.html").exists():
        primary = source / "docs" / "PRD.md"
        resolved_primary = primary.resolve()
        primary_output = output_paths.get(resolved_primary)
        if primary_output is not None:
            text = primary.read_text(encoding="utf-8")
            title = title_for(text, primary.stem.replace("-", " ").title())
            body = rewrite_markdown_links(
                renderer.reset().convert(text),
                resolved_primary,
                Path("index.html"),
                output_paths,
            )
            body = make_tables_responsive(body)
            (destination / "index.html").write_text(
                render_page(title, body, nav_for(Path("index.html"), active_output=primary_output)),
                encoding="utf-8",
            )
        else:
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
