#!/usr/bin/env python3
"""Generate a grouped, browsable HTML report from Gradle JUnit XML results."""

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


@dataclass(frozen=True)
class TestSuite:
    name: str
    tests: int
    failures: int
    errors: int
    skipped: int
    time: float
    cases: list[TestCase]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_results_dir", type=Path)
    parser.add_argument("output_file", type=Path)
    parser.add_argument("--gradle-report-path", default="gradle/index.html")
    args = parser.parse_args()

    suites = read_suites(args.test_results_dir)
    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(render_report(suites, args.gradle_report_path), encoding="utf-8")


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
            detail = child.attrib.get("message") or (child.text or "")
            break

    return TestCase(
        suite_name=suite_name,
        class_name=element.attrib.get("classname", suite_name),
        name=element.attrib.get("name", "(unnamed test)"),
        status=status,
        time=float(element.attrib.get("time", 0.0)),
        detail=detail.strip(),
    )


def render_report(suites: list[TestSuite], gradle_report_path: str) -> str:
    totals = {
        "tests": sum(suite.tests for suite in suites),
        "failures": sum(suite.failures for suite in suites),
        "errors": sum(suite.errors for suite in suites),
        "skipped": sum(suite.skipped for suite in suites),
        "time": sum(suite.time for suite in suites),
    }
    passed = max(totals["tests"] - totals["failures"] - totals["errors"] - totals["skipped"], 0)

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>Grouped Coordinator Test Report</title>",
            f"<style>{CSS}</style>",
            "</head>",
            "<body>",
            "<main>",
            "<header>",
            "<h1>Grouped Coordinator Test Report</h1>",
            '<p class="lede">Open each section to inspect the child suites, scenario groups, and individual tests.</p>',
            f'<p><a class="link" href="{escape_attr(gradle_report_path)}">Open Gradle default report</a></p>',
            "</header>",
            '<section class="summary-grid" aria-label="Test summary">',
            summary_card("Total", totals["tests"]),
            summary_card("Passed", passed, "pass"),
            summary_card("Failed", totals["failures"], "fail"),
            summary_card("Errored", totals["errors"], "fail"),
            summary_card("Skipped", totals["skipped"], "skip"),
            summary_card("Time", f'{totals["time"]:.3f}s'),
            "</section>",
            render_suites(suites),
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def render_suites(suites: list[TestSuite]) -> str:
    if not suites:
        return '<section class="empty">No JUnit XML results were found.</section>'

    body = ['<section class="suite-list" aria-label="Test suites">']
    for suite in suites:
        open_attr = " open" if suite.failures or suite.errors else ""
        body.extend(
            [
                f'<details class="suite"{open_attr}>',
                f"<summary>{escape(short_name(suite.name))} {badges_for(suite.tests, suite.failures, suite.errors, suite.skipped)}</summary>",
                '<div class="suite-body">',
                f'<p class="muted">{escape(suite.name)} · {suite.time:.3f}s</p>',
                render_matrix_cases(suite.cases)
                if suite.name.endswith("CoordinatorOperationalScenarioMatrixTest")
                else render_flat_cases(suite.cases, suite.name),
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
            capacity_html.append(group_details(capacity, cases, "group-level-3", render_case_table(cases)))
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
            body.append(group_details(status.title(), status_cases, "group-level-1", render_case_table(status_cases)))
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
            f"<summary>{escape(title)} {badges_from_cases(cases)}</summary>",
            content,
            "</details>",
        ]
    )


def render_case_table(cases: list[TestCase]) -> str:
    rows = []
    for case in cases:
        detail = f'<div class="case-detail">{escape(case.detail)}</div>' if case.detail else ""
        rows.append(
            "<tr>"
            f'<td><span class="status {escape_attr(case.status)}">{escape(case.status)}</span></td>'
            f"<td>{escape(case.name)}{detail}</td>"
            f"<td>{case.time:.3f}s</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<table class="case-table">',
            "<thead><tr><th>Status</th><th>Test</th><th>Time</th></tr></thead>",
            "<tbody>",
            *rows,
            "</tbody>",
            "</table>",
        ]
    )


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


CSS = re.sub(
    r"\s+",
    " ",
    """
    :root {
      --bg: #f7f8fb;
      --panel: #ffffff;
      --text: #172033;
      --muted: #667085;
      --line: #d8dee9;
      --accent: #1428a0;
      --pass: #16794c;
      --fail: #b42318;
      --skip: #946200;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 15px;
      line-height: 1.5;
    }
    main { width: min(100%, 1120px); margin: 0 auto; padding: 24px; }
    h1 { margin: 0 0 8px; font-size: 28px; line-height: 1.2; }
    .lede { margin: 0 0 10px; color: var(--muted); }
    .link { color: var(--accent); font-weight: 650; text-decoration: none; }
    .link:hover { text-decoration: underline; }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
      gap: 10px;
      margin: 18px 0;
    }
    .summary-card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
    }
    .summary-card span { display: block; color: var(--muted); font-size: 12px; }
    .summary-card strong { display: block; margin-top: 4px; font-size: 22px; }
    .summary-card.pass strong { color: var(--pass); }
    .summary-card.fail strong { color: var(--fail); }
    .summary-card.skip strong { color: var(--skip); }
    details {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      margin: 10px 0;
      overflow: hidden;
    }
    summary {
      cursor: pointer;
      padding: 11px 13px;
      font-weight: 680;
      list-style-position: outside;
    }
    .suite > summary { background: #eef2ff; }
    .group-level-1 > summary { background: #f8fafc; }
    .group-level-2 { margin-left: 14px; }
    .group-level-3 { margin-left: 28px; }
    .suite-body, .nested-groups { padding: 0 12px 12px; }
    .muted { color: var(--muted); margin: 8px 0 10px; }
    .badge {
      display: inline-block;
      margin-left: 6px;
      padding: 1px 6px;
      border-radius: 999px;
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      vertical-align: 1px;
    }
    .badge.pass { color: var(--pass); border-color: #abefc6; background: #ecfdf3; }
    .badge.fail { color: var(--fail); border-color: #fecdca; background: #fef3f2; }
    .badge.skip { color: var(--skip); border-color: #fedf89; background: #fffaeb; }
    table { width: 100%; border-collapse: collapse; margin: 8px 0 12px; background: #fff; }
    th, td { border-top: 1px solid var(--line); padding: 8px; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-size: 12px; }
    .status { font-weight: 700; }
    .status.passed { color: var(--pass); }
    .status.failed, .status.error { color: var(--fail); }
    .status.skipped { color: var(--skip); }
    .case-detail {
      margin-top: 6px;
      color: var(--fail);
      white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .empty {
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }
    @media (max-width: 720px) {
      main { padding: 16px; }
      .group-level-2, .group-level-3 { margin-left: 0; }
      th:nth-child(3), td:nth-child(3) { display: none; }
    }
    """,
).strip()


if __name__ == "__main__":
    main()
