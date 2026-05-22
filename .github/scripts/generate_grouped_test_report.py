#!/usr/bin/env python3
"""Generate a browsable HTML report from Gradle JUnit XML results."""

from __future__ import annotations

import argparse
import html
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TestCase:
    suite_name: str
    class_name: str
    name: str
    status: str
    time: float
    detail: str
    stdout: str
    stderr: str


@dataclass(frozen=True)
class TestSuite:
    name: str
    tests: int
    failures: int
    errors: int
    skipped: int
    time: float
    stdout: str
    stderr: str
    cases: list[TestCase]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_results_dir", type=Path)
    parser.add_argument("output_file", type=Path)
    parser.add_argument("--gradle-report-path", default="gradle/index.html")
    parser.add_argument("--stdout-log-path", type=Path)
    args = parser.parse_args()

    suites = read_suites(args.test_results_dir)
    build_output = read_optional_text(args.stdout_log_path)
    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(
        render_report(suites, args.gradle_report_path, build_output),
        encoding="utf-8",
    )


def read_optional_text(path: Path | None) -> str:
    if path is None or not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace").strip()


def read_suites(test_results_dir: Path) -> list[TestSuite]:
    if not test_results_dir.exists():
        return []

    suites: list[TestSuite] = []
    for xml_file in sorted(test_results_dir.glob("TEST-*.xml")):
        root = ET.parse(xml_file).getroot()
        suite_name = root.attrib.get("name", xml_file.stem.removeprefix("TEST-"))
        cases = [parse_case(suite_name, element) for element in root.findall("testcase")]
        suites.append(
            TestSuite(
                name=suite_name,
                tests=int(root.attrib.get("tests", len(cases))),
                failures=int(root.attrib.get("failures", 0)),
                errors=int(root.attrib.get("errors", 0)),
                skipped=int(root.attrib.get("skipped", 0)),
                time=float(root.attrib.get("time", 0.0)),
                stdout=child_text(root, "system-out"),
                stderr=child_text(root, "system-err"),
                cases=cases,
            )
        )
    return suites


def parse_case(suite_name: str, element: ET.Element) -> TestCase:
    status = "passed"
    detail = ""
    for child in element:
        if child.tag in {"failure", "error", "skipped"}:
            status = "failed" if child.tag == "failure" else child.tag
            detail = detail_text(child)
            break

    return TestCase(
        suite_name=suite_name,
        class_name=element.attrib.get("classname", suite_name),
        name=element.attrib.get("name", "(unnamed test)"),
        status=status,
        time=float(element.attrib.get("time", 0.0)),
        detail=detail.strip(),
        stdout=child_text(element, "system-out"),
        stderr=child_text(element, "system-err"),
    )


def child_text(element: ET.Element, tag_name: str) -> str:
    child = element.find(tag_name)
    return "" if child is None or child.text is None else child.text.strip()


def detail_text(element: ET.Element) -> str:
    message = element.attrib.get("message", "").strip()
    body = (element.text or "").strip()
    if message and body and message not in body:
        return f"{message}\n{body}"
    return message or body


def render_report(suites: list[TestSuite], gradle_report_path: str, build_output: str = "") -> str:
    totals = {
        "tests": sum(suite.tests for suite in suites),
        "failures": sum(suite.failures for suite in suites),
        "errors": sum(suite.errors for suite in suites),
        "skipped": sum(suite.skipped for suite in suites),
        "time": sum(suite.time for suite in suites),
    }
    passed = max(totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"], 0)
    failed = totals["failures"] + totals["errors"]
    result_class = "success" if failed == 0 else "failed"
    result_label = "Passed" if failed == 0 else "Failed"

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>Coordinator Test Report</title>",
            f"<style>{CSS}</style>",
            "</head>",
            "<body>",
            '<div class="page-shell">',
            '<aside class="sidebar">',
            '<div class="product">Redis Stream Coordinator</div>',
            '<div class="sidebar-title">Test execution</div>',
            '<nav aria-label="Test report navigation">',
            '<a href="#overview">Overview</a>',
            '<a href="#suites">Suites</a>',
            '<a href="#output">Output</a>',
            *sidebar_suite_links(suites),
            "</nav>",
            "</aside>",
            "<main>",
            '<header id="overview" class="hero">',
            f'<div class="result {result_class}">{result_label}</div>',
            "<h1>Coordinator Test Report</h1>",
            '<p class="lede">Expandable suites, scenario groups, individual test results, failure detail, and captured stdout/stderr in one static report.</p>',
            '<div class="hero-actions">',
            f'<a class="button" href="{escape_attr(gradle_report_path)}">Open Gradle default report</a>',
            '<a class="button secondary" href="#output">Jump to stdout</a>',
            "</div>",
            "</header>",
            '<section class="summary-grid" aria-label="Test summary">',
            summary_card("Total tests", totals["tests"]),
            summary_card("Passed", passed, "pass"),
            summary_card("Failed", totals["failures"], "fail"),
            summary_card("Errored", totals["errors"], "fail"),
            summary_card("Skipped", totals["skipped"], "skip"),
            summary_card("Total time", f'{totals["time"]:.3f}s'),
            "</section>",
            render_suites(suites),
            render_output_section(suites, build_output),
            "</main>",
            "</div>",
            "</body>",
            "</html>",
        ]
    )


def sidebar_suite_links(suites: list[TestSuite]) -> list[str]:
    return [
        f'<a class="suite-link" href="#{escape_attr(suite_anchor(suite.name))}">{escape(short_name(suite.name))}</a>'
        for suite in suites
    ]


def render_suites(suites: list[TestSuite]) -> str:
    if not suites:
        return '<section id="suites" class="empty">No JUnit XML results were found.</section>'

    body = ['<section id="suites" class="suite-list" aria-label="Test suites">', '<div class="section-heading"><h2>Suites</h2><p>Click a suite, scenario group, or test case to inspect nested results.</p></div>']
    for suite in suites:
        open_attr = " open" if suite.failures or suite.errors else ""
        body.extend(
            [
                f'<details id="{escape_attr(suite_anchor(suite.name))}" class="suite"{open_attr}>',
                f"<summary>{suite_status(suite)}<span>{escape(short_name(suite.name))}</span>{badges_for(suite.tests, suite.failures, suite.errors, suite.skipped)}</summary>",
                '<div class="suite-body">',
                f'<p class="muted">{escape(suite.name)} · {suite.time:.3f}s</p>',
                render_matrix_cases(suite.cases)
                if suite.name.endswith("CoordinatorOperationalScenarioMatrixTest")
                else render_flat_cases(suite.cases, suite.name),
                render_inline_output("Suite stdout", suite.stdout),
                render_inline_output("Suite stderr", suite.stderr),
                "</div>",
                "</details>",
            ]
        )
    body.append("</section>")
    return "\n".join(body)


def render_matrix_cases(cases: list[TestCase]) -> str:
    grouped: dict[str, dict[str, dict[str, list[TestCase]]]] = {}
    fallback: list[TestCase] = []
    for case in cases:
        parts = [part.strip() for part in case.name.split("|")]
        if len(parts) != 4:
            fallback.append(case)
            continue
        scale, churn, capacity, _size = parts
        grouped.setdefault(scale, {}).setdefault(churn, {}).setdefault(capacity, []).append(case)

    body = ['<div class="nested-groups">']
    for scale, churns in grouped.items():
        scale_cases = [case for capacities in churns.values() for cases_by_capacity in capacities.values() for case in cases_by_capacity]
        body.append(group_details(scale, scale_cases, "group-level-1", render_churns(churns)))
    if fallback:
        body.append(group_details("Other matrix cases", fallback, "group-level-1", render_status_groups(fallback)))
    body.append("</div>")
    return "\n".join(body)


def render_churns(churns: dict[str, dict[str, list[TestCase]]]) -> str:
    body: list[str] = []
    for churn, capacities in churns.items():
        churn_cases = [case for cases_by_capacity in capacities.values() for case in cases_by_capacity]
        capacity_html = []
        for capacity, cases in capacities.items():
            capacity_html.append(group_details(capacity, cases, "group-level-3", render_case_list(cases)))
        body.append(group_details(churn, churn_cases, "group-level-2", "\n".join(capacity_html)))
    return "\n".join(body)


def render_flat_cases(cases: list[TestCase], suite_name: str) -> str:
    categories: dict[str, list[TestCase]] = {}
    for case in cases:
        categories.setdefault(case_category(case, suite_name), []).append(case)

    only_category = next(iter(categories), "Tests")
    if len(categories) == 1 and (
        only_category == "Tests" or normalized_label(only_category) == normalized_label(short_name(suite_name))
    ):
        return render_status_groups(cases)

    if len(categories) > 1 or only_category != "Tests":
        return "\n".join(
            group_details(category, category_cases, "group-level-1", render_status_groups(category_cases))
            for category, category_cases in categories.items()
        )

    return render_status_groups(cases)


def render_status_groups(cases: list[TestCase]) -> str:
    grouped: dict[str, list[TestCase]] = {}
    for case in cases:
        grouped.setdefault(case.status, []).append(case)

    body: list[str] = []
    for status in ["failed", "error", "skipped", "passed"]:
        status_cases = grouped.get(status, [])
        if status_cases:
            body.append(group_details(status.title(), status_cases, "group-level-1", render_case_list(status_cases)))
    return "\n".join(body)


def case_category(case: TestCase, suite_name: str) -> str:
    if "$" in case.class_name:
        return humanize_identifier(case.class_name.rsplit("$", 1)[-1])
    if case.class_name != suite_name:
        return humanize_identifier(short_name(case.class_name))
    return "Tests"


def group_details(title: str, cases: list[TestCase], css_class: str, content: str) -> str:
    open_attr = " open" if any(case.status in {"failed", "error"} for case in cases) else ""
    return "\n".join(
        [
            f'<details class="{css_class}"{open_attr}>',
            f"<summary><span>{escape(title)}</span>{badges_from_cases(cases)}</summary>",
            content,
            "</details>",
        ]
    )


def render_case_list(cases: list[TestCase]) -> str:
    cards = [render_case(case) for case in cases]
    return "\n".join(['<div class="case-list">', *cards, "</div>"])


def render_case(case: TestCase) -> str:
    open_attr = " open" if case.status in {"failed", "error"} else ""
    detail = render_inline_output("Failure detail", case.detail)
    stdout = render_inline_output("Standard output", case.stdout)
    stderr = render_inline_output("Standard error", case.stderr)
    metadata = (
        '<dl class="case-meta">'
        f"<div><dt>Class</dt><dd>{escape(case.class_name)}</dd></div>"
        f"<div><dt>Suite</dt><dd>{escape(case.suite_name)}</dd></div>"
        f"<div><dt>Duration</dt><dd>{case.time:.3f}s</dd></div>"
        "</dl>"
    )
    return "\n".join(
        [
            f'<details class="case-card {escape_attr(case.status)}"{open_attr}>',
            '<summary class="case-summary">',
            f'<span class="status {escape_attr(case.status)}">{escape(case.status)}</span>',
            f'<span class="case-name">{escape(case.name)}</span>',
            f'<span class="case-time">{case.time:.3f}s</span>',
            "</summary>",
            '<div class="case-body">',
            metadata,
            detail,
            stdout,
            stderr,
            "</div>",
            "</details>",
        ]
    )


def render_output_section(suites: list[TestSuite], build_output: str) -> str:
    suite_output = "\n\n".join(
        f"## {suite.name}\n{joined}"
        for suite in suites
        for joined in ["\n".join(piece for piece in [suite.stdout, suite.stderr] if piece)]
        if joined
    )
    return "\n".join(
        [
            '<section id="output" class="output-section">',
            '<div class="section-heading"><h2>Output</h2><p>Gradle stdout is shown first, followed by suite-level JUnit stdout/stderr when available.</p></div>',
            render_inline_output("Gradle stdout", build_output, open_by_default=bool(build_output)),
            render_inline_output("JUnit suite stdout/stderr", suite_output, open_by_default=bool(suite_output) and not build_output),
            "</section>",
        ]
    )


def render_inline_output(title: str, value: str, open_by_default: bool = False) -> str:
    if not value:
        return ""
    open_attr = " open" if open_by_default else ""
    return "\n".join(
        [
            f'<details class="output"{open_attr}>',
            f"<summary>{escape(title)}</summary>",
            f"<pre>{escape(value)}</pre>",
            "</details>",
        ]
    )


def suite_status(suite: TestSuite) -> str:
    if suite.failures or suite.errors:
        return '<span class="suite-dot failed" aria-label="failed"></span>'
    if suite.skipped and suite.skipped == suite.tests:
        return '<span class="suite-dot skipped" aria-label="skipped"></span>'
    return '<span class="suite-dot passed" aria-label="passed"></span>'


def badges_from_cases(cases: list[TestCase]) -> str:
    total = len(cases)
    failures = sum(1 for case in cases if case.status == "failed")
    errors = sum(1 for case in cases if case.status == "error")
    skipped = sum(1 for case in cases if case.status == "skipped")
    return badges_for(total, failures, errors, skipped)


def badges_for(total: int, failures: int, errors: int, skipped: int) -> str:
    passed = max(total - failures - errors - skipped, 0)
    pieces = [f'<span class="badge">{total} total</span>', f'<span class="badge pass">{passed} passed</span>']
    if failures:
        pieces.append(f'<span class="badge fail">{failures} failed</span>')
    if errors:
        pieces.append(f'<span class="badge fail">{errors} errored</span>')
    if skipped:
        pieces.append(f'<span class="badge skip">{skipped} skipped</span>')
    return " ".join(pieces)


def summary_card(label: str, value: object, status: str = "") -> str:
    status_class = f" {status}" if status else ""
    return f'<div class="summary-card{status_class}"><span>{escape(str(label))}</span><strong>{escape(str(value))}</strong></div>'


def suite_anchor(value: str) -> str:
    return "suite-" + re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def short_name(value: str) -> str:
    return humanize_identifier(value.rsplit(".", 1)[-1].rsplit("$", 1)[-1])


def humanize_identifier(value: str) -> str:
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", value)
    spaced = spaced.replace("_", " ").replace("-", " ")
    return " ".join(spaced.split())


def normalized_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def escape(value: str) -> str:
    return html.escape(value, quote=False)


def escape_attr(value: str) -> str:
    return html.escape(value, quote=True)


CSS = """
:root {
  --bg: #f6f7f9;
  --panel: #ffffff;
  --panel-soft: #f9fafb;
  --text: #172033;
  --muted: #667085;
  --line: #d8dee9;
  --accent: #3155d4;
  --accent-soft: #eef2ff;
  --pass: #16803d;
  --pass-soft: #ecfdf3;
  --fail: #b42318;
  --fail-soft: #fef3f2;
  --skip: #946200;
  --skip-soft: #fffaeb;
  --shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
}
* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
  line-height: 1.5;
}
.page-shell { display: grid; grid-template-columns: 260px minmax(0, 1fr); min-height: 100vh; }
.sidebar {
  position: sticky;
  top: 0;
  height: 100vh;
  padding: 22px 16px;
  border-right: 1px solid var(--line);
  background: #111827;
  color: #fff;
  overflow-y: auto;
}
.product { font-size: 12px; color: #c7d2fe; font-weight: 700; letter-spacing: 0.02em; text-transform: uppercase; }
.sidebar-title { margin: 6px 0 18px; font-size: 20px; font-weight: 750; }
nav { display: grid; gap: 4px; }
nav a {
  color: #e5e7eb;
  text-decoration: none;
  padding: 7px 8px;
  border-radius: 6px;
  overflow-wrap: anywhere;
}
nav a:hover { background: rgba(255, 255, 255, 0.08); color: #fff; }
nav .suite-link { padding-left: 16px; color: #cbd5e1; font-size: 12px; }
main { width: min(100%, 1180px); padding: 28px; }
.hero {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 10px;
  box-shadow: var(--shadow);
  padding: 22px;
}
h1, h2 { margin: 0; line-height: 1.2; }
h1 { font-size: 28px; }
h2 { font-size: 20px; }
.lede { margin: 8px 0 0; color: var(--muted); max-width: 820px; }
.result {
  display: inline-block;
  margin-bottom: 12px;
  border-radius: 999px;
  padding: 3px 10px;
  font-size: 12px;
  font-weight: 750;
}
.result.success { color: var(--pass); background: var(--pass-soft); }
.result.failed { color: var(--fail); background: var(--fail-soft); }
.hero-actions { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 18px; }
.button {
  display: inline-flex;
  align-items: center;
  border-radius: 6px;
  background: var(--accent);
  color: #fff;
  padding: 8px 12px;
  font-weight: 700;
  text-decoration: none;
}
.button.secondary { background: var(--accent-soft); color: var(--accent); }
.summary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
  margin: 18px 0;
}
.summary-card {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
  padding: 13px;
}
.summary-card span { display: block; color: var(--muted); font-size: 12px; }
.summary-card strong { display: block; margin-top: 4px; font-size: 22px; }
.summary-card.pass strong { color: var(--pass); }
.summary-card.fail strong { color: var(--fail); }
.summary-card.skip strong { color: var(--skip); }
.section-heading {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 16px;
  margin: 24px 0 10px;
}
.section-heading p { margin: 0; color: var(--muted); }
details {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  margin: 10px 0;
  overflow: hidden;
  box-shadow: var(--shadow);
}
summary {
  cursor: pointer;
  padding: 11px 13px;
  font-weight: 700;
  list-style-position: outside;
}
.suite > summary {
  display: flex;
  align-items: center;
  gap: 8px;
  background: var(--accent-soft);
}
.group-level-1 > summary { background: var(--panel-soft); }
.group-level-2 { margin-left: 14px; }
.group-level-3 { margin-left: 28px; }
.suite-body, .nested-groups { padding: 0 12px 12px; }
.muted { color: var(--muted); margin: 8px 0 10px; }
.suite-dot {
  width: 9px;
  height: 9px;
  border-radius: 999px;
  flex: 0 0 auto;
}
.suite-dot.passed { background: var(--pass); }
.suite-dot.failed { background: var(--fail); }
.suite-dot.skipped { background: var(--skip); }
.badge {
  display: inline-block;
  margin-left: 6px;
  padding: 1px 6px;
  border-radius: 999px;
  border: 1px solid var(--line);
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  vertical-align: 1px;
}
.badge.pass { color: var(--pass); border-color: #abefc6; background: var(--pass-soft); }
.badge.fail { color: var(--fail); border-color: #fecdca; background: var(--fail-soft); }
.badge.skip { color: var(--skip); border-color: #fedf89; background: var(--skip-soft); }
.case-list { display: grid; gap: 8px; margin: 10px 0 12px; }
.case-card {
  margin: 0;
  border-radius: 7px;
  box-shadow: none;
}
.case-card.failed, .case-card.error { border-color: #fecdca; }
.case-summary {
  display: grid;
  grid-template-columns: max-content minmax(0, 1fr) max-content;
  align-items: center;
  gap: 10px;
  padding: 9px 11px;
}
.status {
  display: inline-flex;
  justify-content: center;
  min-width: 68px;
  border-radius: 999px;
  padding: 2px 7px;
  font-size: 12px;
  font-weight: 800;
  text-transform: uppercase;
}
.status.passed { color: var(--pass); background: var(--pass-soft); }
.status.failed, .status.error { color: var(--fail); background: var(--fail-soft); }
.status.skipped { color: var(--skip); background: var(--skip-soft); }
.case-name { overflow-wrap: anywhere; }
.case-time { color: var(--muted); font-size: 12px; }
.case-body { padding: 0 12px 12px; border-top: 1px solid var(--line); }
.case-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px;
  margin: 10px 0;
}
.case-meta div {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--panel-soft);
  padding: 8px;
}
dt { color: var(--muted); font-size: 11px; font-weight: 800; text-transform: uppercase; }
dd { margin: 3px 0 0; overflow-wrap: anywhere; }
.output {
  box-shadow: none;
  background: #0b1020;
  border-color: #1f2937;
}
.output > summary { color: #e5e7eb; background: #111827; }
pre {
  margin: 0;
  max-height: 460px;
  overflow: auto;
  padding: 12px;
  background: #0b1020;
  color: #d1d5db;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 1.45;
  white-space: pre-wrap;
}
.empty {
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--panel);
}
@media (max-width: 900px) {
  .page-shell { display: block; }
  .sidebar { position: static; height: auto; }
  main { padding: 16px; }
}
@media (max-width: 720px) {
  .section-heading { display: block; }
  .group-level-2, .group-level-3 { margin-left: 0; }
  .case-summary { grid-template-columns: 1fr; }
}
"""


if __name__ == "__main__":
    main()
