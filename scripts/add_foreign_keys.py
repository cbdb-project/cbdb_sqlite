#!/usr/bin/env python3
"""
Add foreign key constraints to a CBDB SQLite database based on foreign_keys_regen.csv.

Reads the FK definitions from a CSV URL, groups them by table, and recreates
each affected table with proper FOREIGN KEY constraints appended to the schema.
Tables that already have FK constraints are skipped (idempotent).

Usage:
    python add_foreign_keys.py [--db DB_PATH] [--csv-url URL]
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sqlite3
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

CSV_URL = (
    "https://raw.githubusercontent.com/cbdb-project/cbdb-user-mdb-tests"
    "/main/reports/foreign_keys_regen.csv"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# (child_col, parent_table, parent_col)
FKDef = Tuple[str, str, str]


def fetch_csv(url: str) -> str:
    logger.info("Fetching FK definitions from %s", url)
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8-sig")  # strip BOM if present


def parse_foreign_keys(csv_content: str) -> Dict[str, List[FKDef]]:
    """
    Parse foreign_keys_regen.csv and return {table_name: [(col, ref_table, ref_col), ...]}.

    Table names are normalised to UPPER CASE.  Duplicate (table, col, ref_table, ref_col)
    combinations are deduplicated while preserving order.
    """
    fk_map: Dict[str, List[FKDef]] = defaultdict(list)
    seen: set = set()

    reader = csv.DictReader(io.StringIO(csv_content))
    for row in reader:
        table = row["AccessTblNm"].strip().upper()
        col = row["AccessFldNm"].strip()
        ref_table = row["ForeignKey"].strip().upper()
        ref_col = row["ForeignKeyBaseField"].strip()

        key = (table, col, ref_table, ref_col)
        if key in seen:
            continue
        seen.add(key)
        fk_map[table].append((col, ref_table, ref_col))

    return dict(fk_map)


def _has_foreign_keys(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute(f'PRAGMA foreign_key_list("{table}")').fetchall())


def _get_create_sql(conn: sqlite3.Connection, table: str) -> Optional[str]:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row[0] if row else None


def _build_create_with_fks(create_sql: str, tmp_name: str, fk_defs: List[FKDef]) -> str:
    """
    Append FOREIGN KEY constraints to an existing CREATE TABLE statement
    and rename the table to tmp_name.

    Uses paren-depth tracking so nested expressions inside CHECK or DEFAULT
    clauses do not confuse the outer closing-paren search.
    """
    depth = 0
    close_pos = -1
    for i, ch in enumerate(create_sql):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                close_pos = i
                break

    if close_pos == -1:
        raise ValueError("Could not locate closing paren in CREATE TABLE SQL.")

    body = create_sql[:close_pos].rstrip().rstrip(",")
    tail = create_sql[close_pos + 1:]

    fk_clauses = []
    for col, ref_table, ref_col in fk_defs:
        fk_clauses.append(
            f'    FOREIGN KEY ("{col}") REFERENCES "{ref_table}" ("{ref_col}")'
        )

    new_sql = body + ",\n" + ",\n".join(fk_clauses) + "\n)" + tail

    # Replace the table name; handle double-quoted, backtick, bracket, or bare identifiers.
    new_sql = re.sub(
        r'(?i)(CREATE\s+TABLE\s+)(?:"[^"]*"|`[^`]*`|\[[^\]]*\]|\S+)',
        rf'\1"{tmp_name}"',
        new_sql,
        count=1,
    )
    return new_sql


def _recreate_with_fks(
    conn: sqlite3.Connection, table: str, fk_defs: List[FKDef]
) -> bool:
    """
    Recreate *table* with FOREIGN KEY constraints appended.  Returns True on success.
    Foreign-key enforcement is disabled for the duration of the operation.
    """
    create_sql = _get_create_sql(conn, table)
    if not create_sql:
        logger.warning("  %s: not found in sqlite_master, skipping.", table)
        return False

    if "VIRTUAL" in create_sql.upper():
        logger.info("  %s: virtual table, skipping.", table)
        return False

    tmp = f"_fk_rebuild_{table}"
    try:
        new_create = _build_create_with_fks(create_sql, tmp, fk_defs)
    except ValueError as exc:
        logger.error("  %s: could not build new CREATE TABLE — %s", table, exc)
        return False

    col_list = ", ".join(
        f'"{row[1]}"'
        for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    )

    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')
        conn.execute(new_create)
        conn.execute(f'INSERT INTO "{tmp}" SELECT {col_list} FROM "{table}"')
        conn.execute(f'DROP TABLE "{table}"')
        conn.execute(f'ALTER TABLE "{tmp}" RENAME TO "{table}"')
        conn.commit()
        fk_summary = ", ".join(f"{col}->{ref_t}.{ref_c}" for col, ref_t, ref_c in fk_defs)
        logger.info("  ✓ %s  (%d FKs: %s)", table, len(fk_defs), fk_summary)
        return True
    except Exception as exc:
        conn.rollback()
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')
        except Exception:
            pass
        logger.error("  ✗ %s: %s", table, exc)
        return False
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def add_foreign_keys(db_path: str | Path, csv_url: str = CSV_URL) -> None:
    """
    Add FOREIGN KEY constraints to all applicable tables in *db_path* based on
    foreign_keys_regen.csv.  Tables that already have FK constraints are skipped.
    """
    content = fetch_csv(csv_url)
    fk_map = parse_foreign_keys(content)
    logger.info("CSV parsed: FK definitions found for %d tables.", len(fk_map))

    conn = sqlite3.connect(str(db_path))
    try:
        # Build a case-insensitive lookup from uppercase name → actual DB name.
        db_table_lookup: Dict[str, str] = {
            row[0].upper(): row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }

        updated = skipped = missing = 0
        for upper_name, fk_defs in fk_map.items():
            actual = db_table_lookup.get(upper_name)
            if actual is None:
                logger.debug("  %s: not in database, skipping.", upper_name)
                missing += 1
                continue
            if _has_foreign_keys(conn, actual):
                skipped += 1
                continue
            if _recreate_with_fks(conn, actual, fk_defs):
                updated += 1

        logger.info(
            "Finished: %d tables updated, %d already had FKs, %d not in database.",
            updated,
            skipped,
            missing,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add foreign key constraints to a CBDB SQLite database."
    )
    parser.add_argument(
        "--db",
        default="latest.db",
        type=Path,
        help="Path to the SQLite database (default: latest.db).",
    )
    parser.add_argument(
        "--csv-url",
        default=CSV_URL,
        metavar="URL",
        help="URL of foreign_keys_regen.csv (default: main branch on GitHub).",
    )
    args = parser.parse_args()
    add_foreign_keys(args.db, args.csv_url)
