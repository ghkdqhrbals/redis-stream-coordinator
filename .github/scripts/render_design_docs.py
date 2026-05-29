#!/usr/bin/env python3
"""Render Markdown design documents into a small static HTML site."""

from __future__ import annotations

import argparse
import html
import os
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import markdown


DESIGN_DOC_EXTRA_NAMES = (
    "implementation-status.md",
    "operations-runbook.md",
    "testing.md",
)


@dataclass(frozen=True)
class Page:
    markdown_path: Path
    title: str
    output_path: Path
    language: str


def title_for(markdown_text: str, fallback: str) -> str:
    for line in markdown_text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return fallback


def output_path(source: Path, markdown_path: Path) -> Path:
    return markdown_path.relative_to(source).with_suffix(".html")


def localized_output_path(docs_root: Path, output_root: Path, markdown_path: Path) -> Path:
    return output_root / markdown_path.relative_to(docs_root).with_suffix(".html")


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


def discover_localized_pages(
    docs_root: Path,
    output_root: Path,
    language: str,
) -> list[Page]:
    primary = docs_root / "PRD.md"
    if not primary.exists():
        return []

    ordered = [primary]
    ordered.extend(sorted((docs_root / "prd").glob("*.md")))
    ordered.extend(docs_root / name for name in DESIGN_DOC_EXTRA_NAMES)

    pages: list[Page] = []
    seen: set[Path] = set()
    for path in ordered:
        if not path.exists() or path in seen:
            continue
        seen.add(path)
        text = path.read_text(encoding="utf-8")
        title = title_for(text, path.stem.replace("-", " ").title())
        pages.append(Page(path, title, localized_output_path(docs_root, output_root, path), language))
    return pages


def discover_pages(source: Path) -> list[Page]:
    english_pages = discover_localized_pages(source / "docs", Path("docs"), "en")
    if english_pages:
        return [
            *english_pages,
            *discover_localized_pages(source / "docs" / "ko", Path("ko") / "docs", "ko"),
        ]

    return [
        Page(
            path,
            title_for(path.read_text(encoding="utf-8"), path.stem.replace("-", " ").title()),
            output_path(source, path),
            "en",
        )
        for path in sorted(source.rglob("*.md"))
        if ".git" not in path.parts
    ]


def counterpart_output(output: Path, language: str) -> Path:
    if language == "en":
        return Path("ko") / output
    parts = output.parts
    if parts and parts[0] == "ko":
        return Path(*parts[1:])
    return output


def language_redirect_script(language: str, alternate_href: str | None) -> str:
    if not alternate_href:
        return ""

    target_language = "ko" if language == "en" else "en"
    escaped_target_language = html.escape(target_language, quote=True)
    escaped_alternate_href = html.escape(alternate_href, quote=True)
    return f"""
    <script>
      (() => {{
        const targetLanguage = new URLSearchParams(window.location.search).get("tl");
        if (targetLanguage === "{escaped_target_language}") {{
          const target = new URL("{escaped_alternate_href}", window.location.href);
          target.hash = window.location.hash;
          window.location.replace(target);
        }}
      }})();
    </script>"""


def language_switch(current_href: str, alternate_href: str | None, language: str) -> str:
    if not alternate_href:
        return ""

    if language == "en":
        english_href = current_href
        korean_href = alternate_href
        english_current = ' aria-current="true"'
        korean_current = ""
    else:
        english_href = alternate_href
        korean_href = current_href
        english_current = ""
        korean_current = ' aria-current="true"'

    return (
        '<div class="language-switch" aria-label="Language switch">'
        f'<a href="{html.escape(english_href, quote=True)}"{english_current}>English</a>'
        '<span aria-hidden="true">/</span>'
        f'<a href="{html.escape(korean_href, quote=True)}"{korean_current}>한국어</a>'
        '</div>'
    )


def render_page(
    title: str,
    body: str,
    nav: str,
    language: str = "en",
    current_href: str = "index.html",
    alternate_href: str | None = None,
) -> str:
    escaped_title = html.escape(title)
    escaped_language = html.escape(language, quote=True)
    redirect_script = language_redirect_script(language, alternate_href)
    switch = language_switch(current_href, alternate_href, language)
    return f"""<!doctype html>
<html lang="{escaped_language}">
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
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 16px;
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
      .language-switch {{
        display: flex;
        align-items: center;
        gap: 6px;
        color: var(--muted);
        font-size: 0.82rem;
        white-space: nowrap;
      }}
      .language-switch a {{
        color: var(--accent);
        text-decoration: none;
      }}
      .language-switch a[aria-current="true"] {{
        color: var(--fg);
        font-weight: 700;
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
        .topbar {{
          display: block;
        }}
        .language-switch {{
          margin-top: 8px;
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
{redirect_script}
  </head>
  <body>
    <header>
      <div class="topbar">
        <div>
          <div class="site-title">Redis Stream Coordinator</div>
          <div class="site-subtitle">Design docs</div>
        </div>
        {switch}
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

    pages = discover_pages(source)
    if not pages:
        raise SystemExit(f"No Markdown design documents found in {source}")

    output_paths = {page.markdown_path.resolve(): page.output_path for page in pages}
    output_set = {page.output_path for page in pages}
    pages_by_language: dict[str, list[Page]] = {}
    for page in pages:
        pages_by_language.setdefault(page.language, []).append(page)

    def alternate_for(current_output: Path, language: str) -> Path | None:
        candidate = counterpart_output(current_output, language)
        return candidate if candidate in output_set else None

    def render_href_pair(current_output: Path, language: str) -> tuple[str, str | None]:
        current_href = relative_href(current_output, current_output)
        alternate_output = alternate_for(current_output, language)
        alternate_href = relative_href(current_output, alternate_output) if alternate_output else None
        return current_href, alternate_href

    def nav_for(current_output: Path, language: str, active_output: Path | None = None) -> str:
        active = active_output or current_output
        links = []
        for page in pages_by_language.get(language, []):
            current = page.output_path == active
            attrs = ' aria-current="page"' if current else ""
            links.append(
                f'          <a href="{html.escape(relative_href(current_output, page.output_path))}"{attrs}>'
                f'{html.escape(page.title)}</a>'
            )
        return "\n".join(links)

    renderer = markdown.Markdown(extensions=["extra", "tables", "toc", "fenced_code", "sane_lists"])
    for page in pages:
        text = page.markdown_path.read_text(encoding="utf-8")
        body = rewrite_markdown_links(
            renderer.reset().convert(text),
            page.markdown_path.resolve(),
            page.output_path,
            output_paths,
        )
        body = make_tables_responsive(body)
        target = destination / page.output_path
        target.parent.mkdir(parents=True, exist_ok=True)
        current_href, alternate_href = render_href_pair(page.output_path, page.language)
        target.write_text(
            render_page(
                page.title,
                body,
                nav_for(page.output_path, page.language),
                page.language,
                current_href,
                alternate_href,
            ),
            encoding="utf-8",
        )

    if not (destination / "index.html").exists():
        primary_page = next((page for page in pages if page.markdown_path == source / "docs" / "PRD.md"), None)
        korean_primary_page = next((page for page in pages if page.markdown_path == source / "docs" / "ko" / "PRD.md"), None)
        if primary_page is not None:
            text = primary_page.markdown_path.read_text(encoding="utf-8")
            body = rewrite_markdown_links(
                renderer.reset().convert(text),
                primary_page.markdown_path.resolve(),
                Path("index.html"),
                output_paths,
            )
            body = make_tables_responsive(body)
            (destination / "index.html").write_text(
                render_page(
                    primary_page.title,
                    body,
                    nav_for(Path("index.html"), primary_page.language, active_output=primary_page.output_path),
                    primary_page.language,
                    "index.html",
                    "ko/index.html" if korean_primary_page is not None else None,
                ),
                encoding="utf-8",
            )
        else:
            first_page = pages[0].output_path
            (destination / "index.html").write_text(
                render_page(
                    "Redis Stream Coordinator Design Docs",
                    f'<h1>Redis Stream Coordinator Design Docs</h1><p><a href="{html.escape(first_page.as_posix())}">Open the design document</a>.</p>',
                    nav_for(Path("index.html"), "en"),
                ),
                encoding="utf-8",
            )

    korean_primary_page = next((page for page in pages if page.markdown_path == source / "docs" / "ko" / "PRD.md"), None)
    if korean_primary_page is not None:
        korean_index = Path("ko") / "index.html"
        text = korean_primary_page.markdown_path.read_text(encoding="utf-8")
        body = rewrite_markdown_links(
            renderer.reset().convert(text),
            korean_primary_page.markdown_path.resolve(),
            korean_index,
            output_paths,
        )
        body = make_tables_responsive(body)
        target = destination / korean_index
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            render_page(
                korean_primary_page.title,
                body,
                nav_for(korean_index, "ko", active_output=korean_primary_page.output_path),
                "ko",
                "index.html",
                "../index.html",
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
