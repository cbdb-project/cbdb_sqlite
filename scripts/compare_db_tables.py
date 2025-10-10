#!/usr/bin/env python3
"""
Compare table row counts between two CBDB SQLite databases.

Usage:
    python compare_db_tables.py path/to/old_cbdb.db path/to/new_cbdb.db

The script prints a table with the row counts for every table found in either
database, plus the difference (new - old). Tables that exist only in one
database are annotated accordingly.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Dict


def quote_identifier(identifier: str) -> str:
    """Return identifier quoted with double quotes for SQLite usage."""
    return '"' + identifier.replace('"', '""') + '"'


def load_table_counts(database: Path) -> Dict[str, int]:
    if not database.exists():
        raise FileNotFoundError(f"Database file not found: {database}")

    conn = sqlite3.connect(str(database))
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]

        counts: Dict[str, int] = {}
        for table in tables:
            quoted = quote_identifier(table)
            cursor = conn.execute(f"SELECT COUNT(*) FROM {quoted}")
            counts[table] = cursor.fetchone()[0]
        return counts
    finally:
        conn.close()


def main(old_db: Path, new_db: Path) -> None:
    old_counts = load_table_counts(old_db)
    new_counts = load_table_counts(new_db)

    all_tables = sorted(set(old_counts) | set(new_counts))

    # Prepare column widths for neat printing.
    name_width = max((len(name) for name in all_tables), default=5)
    header = (
        f"{'Table':{name_width}}  "
        f"{'Old Rows':>12}  "
        f"{'New Rows':>12}  "
        f"{'Diff':>12}"
    )
    print(header)
    print("-" * len(header))

    for table in all_tables:
        old = old_counts.get(table)
        new = new_counts.get(table)
        diff_display = "-"
        if old is not None and new is not None:
            diff_display = f"{new - old:+}"

        old_display = str(old) if old is not None else "-"
        new_display = str(new) if new is not None else "-"

        print(
            f"{table:{name_width}}  "
            f"{old_display:>12}  "
            f"{new_display:>12}  "
            f"{diff_display:>12}"
        )

    print()
    only_old = sorted(set(old_counts) - set(new_counts))
    only_new = sorted(set(new_counts) - set(old_counts))

    if only_old:
        print("Tables only in old database:")
        for table in only_old:
            print(f"  - {table}")
    if only_new:
        print("Tables only in new database:")
        for table in only_new:
            print(f"  - {table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare table row counts across two SQLite databases."
    )
    parser.add_argument(
        "old_db", type=Path, help="Path to the reference (old) database."
    )
    parser.add_argument(
        "new_db", type=Path, help="Path to the comparison (new) database."
    )
    args = parser.parse_args()
    main(args.old_db, args.new_db)
