#!/usr/bin/env python3
"""
Run sqlfluff fix on Databricks SQL notebook files.

Databricks SQL notebooks mix Python/Markdown magic cells with SQL cells, all
separated by '-- COMMAND ----------'. sqlfluff is run on each SQL cell
independently (so multi-statement parse errors across cells are avoided).

Usage:
  python scripts/sqlfluff_fix.py                     # fix all files in databricks/src/
  python scripts/sqlfluff_fix.py databricks/src/tne_attendee.sql
  python scripts/sqlfluff_fix.py databricks/src/ --check   # lint only, no changes
"""

import re
import sys
import pathlib
import subprocess
import tempfile

# Cell separator used in Databricks notebooks
CELL_SEP = "-- COMMAND ----------"

# :snake_case_name  →  "__PARAM_name__"
# Databricks named parameter markers are not valid SQL — substitute before linting.
PARAM_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")
PLACEHOLDER_FMT = '"__param_{name}__"'

# IDENTIFIER(...)  →  dynamic_table_N
# Databricks dynamic table references — substitute before linting to avoid
# CV10 false positives on the double-quoted strings inside the expression.
IDENTIFIER_RE = re.compile(r"IDENTIFIER\s*\(([^)]+)\)", re.IGNORECASE | re.DOTALL)
IDENTIFIER_FMT = "dynamic_table_{n}"

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
SQLFLUFF_CONFIG = PROJECT_ROOT / ".sqlfluff"


def encode(sql: str) -> tuple[str, dict[str, str]]:
    """Replace Databricks-specific syntax with sqlfluff-safe placeholders."""
    mapping: dict[str, str] = {}

    # 1. IDENTIFIER(...) — before param substitution to avoid partial matches
    def _replace_identifier(m: re.Match) -> str:
        n = sum(1 for k in mapping if k.startswith("dynamic_table_"))
        placeholder = IDENTIFIER_FMT.format(n=n)
        mapping[placeholder] = m.group(0)
        return placeholder

    sql = IDENTIFIER_RE.sub(_replace_identifier, sql)

    # 2. :param_name
    def _replace_param(m: re.Match) -> str:
        placeholder = PLACEHOLDER_FMT.format(name=m.group(1))
        mapping[placeholder] = m.group(0)
        return placeholder

    sql = PARAM_RE.sub(_replace_param, sql)
    return sql, mapping


def decode(sql: str, mapping: dict[str, str]) -> str:
    """Restore all original values."""
    for placeholder, original in mapping.items():
        sql = sql.replace(placeholder, original)
    return sql


def run_sqlfluff(path: pathlib.Path, check_only: bool) -> subprocess.CompletedProcess:
    cmd = ["python", "-m", "sqlfluff", "lint" if check_only else "fix", str(path)]
    if SQLFLUFF_CONFIG.exists():
        cmd += ["--config", str(SQLFLUFF_CONFIG)]
    return subprocess.run(cmd, capture_output=True, text=True)


def is_magic_cell(cell: str) -> bool:
    """Return True if the cell is a Python/Markdown magic cell (not SQL)."""
    lines = [l for l in cell.splitlines() if l.strip()]
    if not lines:
        return True
    magic_prefixes = ("-- magic", "-- databricks notebook source", "-- dbtitle")
    return all(l.lower().startswith(magic_prefixes) for l in lines)


def fix_cell(cell: str, check_only: bool, label: str) -> tuple[str, str]:
    """
    Lint or fix a single SQL cell.
    Returns (fixed_cell, lint_output).
    """
    stripped = cell.strip("\n")
    leading = cell[: len(cell) - len(cell.lstrip("\n"))]
    trailing = cell[len(cell.rstrip("\n")):]

    encoded, mapping = encode(stripped)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sql", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(encoded + "\n")
        tmp_path = pathlib.Path(tmp.name)

    try:
        result = run_sqlfluff(tmp_path, check_only)
        output = result.stdout.replace(str(tmp_path), label)

        if check_only:
            return cell, output

        fixed_encoded = tmp_path.read_text(encoding="utf-8").strip("\n")
        return leading + decode(fixed_encoded, mapping) + trailing, output
    finally:
        tmp_path.unlink(missing_ok=True)


def process_file(path: pathlib.Path, check_only: bool) -> None:
    original = path.read_text(encoding="utf-8")
    raw_cells = original.split(CELL_SEP)

    fixed_cells: list[str] = []
    changed = False
    all_output: list[str] = []

    for i, cell in enumerate(raw_cells):
        if is_magic_cell(cell):
            fixed_cells.append(cell)
            continue

        label = f"{path} (cell {i})"
        fixed_cell, output = fix_cell(cell, check_only, label)

        if output.strip():
            all_output.append(output)

        if not check_only and fixed_cell != cell:
            changed = True

        fixed_cells.append(fixed_cell)

    if check_only:
        for out in all_output:
            print(out, end="")
        return

    if changed:
        path.write_text(CELL_SEP.join(fixed_cells), encoding="utf-8")
        print(f"  fixed  {path.name}")
    else:
        print(f"  ok     {path.name}")


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    check_only = "--check" in sys.argv

    if args:
        target = pathlib.Path(args[0])
        files = sorted(target.glob("*.sql")) if target.is_dir() else [target]
    else:
        files = sorted(pathlib.Path("databricks/src").glob("*.sql"))

    for f in files:
        process_file(f, check_only=check_only)


if __name__ == "__main__":
    main()
