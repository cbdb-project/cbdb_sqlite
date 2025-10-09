#!/usr/bin/env python3
"""
Rebuild SQLite tables that currently lack an explicit primary key so that they
gain one.

The script performs two phases:
1. For every table without a primary key, determine the intended key columns and
   check the data for duplicates. If any duplicates are found, list the
   offending tables and abort without modifying the database.
2. If all checks pass, rebuild each table inside a single transaction by
   creating a new table that defines the primary key, copying the data,
   re-creating indexes/triggers, and dropping the old table.

Usage:
    python add_primary_keys.py --db path/to/cbdb_database.db

This script has been tested with the last two CBDB official releases (20250520,
20240820).
"""

from __future__ import annotations

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

PRIMARY_KEY_INDEX_PATTERNS = ("PrimaryKey", "Primary Key")

# Primary-key column specifications for tables that either lack an explicit
# index or have schema variations between database versions. Each table can
# supply multiple candidates; the first candidate whose columns exist in the
# current schema will be used.
PRIMARY_KEY_CANDIDATES: Dict[str, Tuple[Tuple[str, ...], ...]] = {
    "ADDR_BELONGS_DATA": (
        ("c_addr_id", "c_belongs_to", "c_firstyear", "c_lastyear"),
    ),
    "ADDR_CODES": (("c_addr_id",),),
    "ADDR_PLACE_DATA": (("c_addr_id", "c_place_id"),),
    "ADDR_XY": (("c_addr_id",),),
    # Expected to fail uniqueness checks and get skipped.
    "ADDRESSES": (("c_addr_id",),),
    "ALTNAME_CODES": (("c_name_type_code",),),
    "ALTNAME_DATA": (("c_personid", "c_alt_name_chn", "c_alt_name_type_code"),),
    "APPOINTMENT_CODES": (("c_appt_code",),),
    "APPOINTMENT_CODE_TYPE_REL": (("c_appt_code", "c_appt_type_code"),),
    "APPOINTMENT_TYPES": (("c_appt_type_code",),),
    "APPOINTMENT_TYPE_CODES": (("c_appt_type_code",),),
    "ASSOC_CODES": (("c_assoc_code",),),
    "ASSOC_CODE_TYPE_REL": (
        ("c_assoc_code", "c_assoc_type_code"),
        ("c_assoc_code", "c_assoc_type_id"),
    ),
    "ASSOC_DATA": (
        (
            "c_assoc_code",
            "c_personid",
            "c_kin_code",
            "c_kin_id",
            "c_assoc_id",
            "c_assoc_kin_code",
            "c_assoc_kin_id",
            "c_assoc_first_year",
            "c_text_title",
        ),
        (
            "c_assoc_code",
            "c_personid",
            "c_kin_code",
            "c_kin_id",
            "c_assoc_id",
            "c_assoc_kin_code",
            "c_assoc_kin_id",
            "c_assoc_year",
            "c_text_title",
        ),
    ),
    "ASSOC_TYPES": (("c_assoc_type_code",), ("c_assoc_type_id",)),
    "ASSUME_OFFICE_CODES": (("c_assume_office_code",),),
    "BIOG_ADDR_CODES": (("c_addr_type",),),
    "BIOG_ADDR_DATA": (
        ("c_personid", "c_addr_id", "c_addr_type", "c_sequence"),
    ),
    "BIOG_INST_CODES": (("c_bi_role_code",),),
    "BIOG_INST_DATA": (
        ("c_personid", "c_inst_name_code", "c_inst_code", "c_bi_role_code"),
    ),
    "BIOG_MAIN": (("c_personid",),),
    "BIOG_SOURCE_DATA": (("c_personid", "c_textid", "c_pages"),),
    "BIOG_TEXT_DATA": (("c_textid", "c_personid", "c_role_id"),),
    "CBDB_NAME_LIST": (("c_personid", "name", "source"),),
    "CHORONYM_CODES": (("c_choronym_code",),),
    "COUNTRY_CODES": (("c_country_code",),),
    "Copy Of CopyTables": (("TableName",),),
    "CopyMissingTables": (("ID",),),
    "CopyTables": (("TableName",),),
    "CopyTablesDefault": (("ID",),),
    "DATABASE_LINK_CODES": (("c_db_id",),),
    "DATABASE_LINK_DATA": (("c_person_id", "c_db_id", "c_db_sys_id"),),
    "DYNASTIES": (("c_dy",),),
    "ENTRY_CODES": (("c_entry_code",),),
    "ENTRY_CODE_TYPE_REL": (("c_entry_code", "c_entry_type"),),
    "ENTRY_DATA": (
        (
            "c_personid",
            "c_entry_code",
            "c_sequence",
            "c_kin_code",
            "c_kin_id",
            "c_assoc_code",
            "c_assoc_id",
            "c_year",
            "c_inst_code",
            "c_inst_name_code",
        ),
    ),
    "ENTRY_TYPES": (("c_entry_type",),),
    "ETHNICITY_TRIBE_CODES": (("c_ethnicity_code",),),
    "EVENTS_ADDR": (("c_event_record_id", "c_personid", "c_addr_id"),),
    "EVENTS_DATA": (
        (
            "c_personid",
            "c_sequence",
            "c_event_record_id",
            "c_event_code",
            "c_event",
        ),
    ),
    "EVENT_CODES": (("c_event_code",),),
    "EXTANT_CODES": (("c_extant_code",),),
    "ForeignKeys": (("AccessTblNm", "AccessFldNm"),),
    "FormLabels": (("c_form", "c_label_id"),),
    "GANZHI_CODES": (("c_ganzhi_code",),),
    "HOUSEHOLD_STATUS_CODES": (("c_household_status_code",),),
    "INDEXYEAR_TYPE_CODES": (("c_index_year_type_code",),),
    "KINSHIP_CODES": (("c_kincode",),),
    "KIN_DATA": (("c_personid", "c_kin_id", "c_kin_code"),),
    "KIN_MOURNING_STEPS": (("c_kinrel",),),
    "KIN_Mourning": (("c_kinrel",),),
    "LITERARYGENRE_CODES": (("c_lit_genre_code",),),
    "MEASURE_CODES": (("c_measure_code",),),
    "NIAN_HAO": (("c_nianhao_id",),),
    "OCCASION_CODES": (("c_occasion_code",),),
    "OFFICE_CATEGORIES": (("c_office_category_id",),),
    "OFFICE_CODES": (("c_office_id",),),
    "OFFICE_CODES_CONVERSION": (("c_office_id",),),
    "OFFICE_CODE_TYPE_REL": (("c_office_id", "c_office_tree_id"),),
    "OFFICE_TYPE_TREE": (("c_office_type_node_id",),),
    "OFFICE_TYPE_TREE_backup": (("c_office_type_node_id",),),
    "PARENTAL_STATUS_CODES": (("c_parental_status_code",),),
    "PLACE_CODES": (("c_place_id",),),
    "POSSESSION_ACT_CODES": (("c_possession_act_code",),),
    "POSSESSION_ADDR": (("c_possession_record_id", "c_personid", "c_addr_id"),),
    "POSSESSION_DATA": (("c_possession_record_id",),),
    "POSTED_TO_ADDR_DATA": (("c_posting_id", "c_office_id", "c_addr_id"),),
    "POSTED_TO_OFFICE_DATA": (("c_office_id", "c_posting_id"),),
    "POSTING_DATA": (("c_posting_id",),),
    "SCHOLARLYTOPIC_CODES": (("c_topic_code",),),
    "SOCIAL_INSTITUTION_ADDR": (
        (
            "c_inst_name_code",
            "c_inst_code",
            "c_inst_addr_type_code",
            "c_inst_addr_id",
            "inst_xcoord",
            "inst_ycoord",
        ),
    ),
    "SOCIAL_INSTITUTION_ADDR_TYPES": (("c_inst_addr_type_code",),),
    "SOCIAL_INSTITUTION_ALTNAME_CODES": (("c_inst_altname_type",),),
    "SOCIAL_INSTITUTION_ALTNAME_DATA": (
        (
            "c_inst_name_code",
            "c_inst_code",
            "c_inst_altname_type",
            "c_inst_altname_hz",
        ),
    ),
    "SOCIAL_INSTITUTION_CODES": (("c_inst_name_code", "c_inst_code"),),
    "SOCIAL_INSTITUTION_CODES_CONVERSION": (
        ("c_inst_name_code", "c_inst_code"),
    ),
    "SOCIAL_INSTITUTION_NAME_CODES": (("c_inst_name_code",),),
    "SOCIAL_INSTITUTION_TYPES": (("c_inst_type_code",),),
    "STATUS_CODES": (("c_status_code",),),
    "STATUS_CODE_TYPE_REL": (("c_status_code", "c_status_type_code"),),
    "STATUS_DATA": (("c_personid", "c_sequence", "c_status_code"),),
    "STATUS_TYPES": (("c_status_type_code",),),
    "TEXT_BIBLCAT_CODES": (("c_text_cat_code",),),
    "TEXT_BIBLCAT_CODE_TYPE_REL": (("c_text_cat_code", "c_text_cat_type_id"),),
    "TEXT_BIBLCAT_TYPES": (("c_text_cat_type_id",),),
    "TEXT_CODES": (("c_textid",),),
    "TEXT_INSTANCE_DATA": (
        ("c_textid", "c_text_edition_id", "c_text_instance_id"),
    ),
    "TEXT_ROLE_CODES": (("c_role_id",),),
    "TEXT_TYPE": (("c_text_type_code",),),
    "TMP_INDEX_YEAR": (("c_personid",),),
    "TablesFields": (("AccessTblNm", "AccessFldNm"),),
    "TablesFieldsChanges": (
        ("TableName", "FieldName", "ChangeDate", "Change"),
    ),
    "YEAR_RANGE_CODES": (("c_range_code",),),
}

# ADDRESSES table did not change between 20240820 and 20250520 releases.
SKIP_TABLES = {"ADDRESSES"}


def quote_ident(identifier: str) -> str:
    """Return an SQLite-safe quoted identifier using bracket quoting."""
    escaped = identifier.replace("]", "]]")
    return f"[{escaped}]"


def fetch_all_tables(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    return [row[0] for row in cursor.fetchall()]


def table_has_primary_key(conn: sqlite3.Connection, table: str) -> bool:
    cursor = conn.execute(f"PRAGMA table_info({quote_ident(table)})")
    return any(row[5] for row in cursor.fetchall())


def columns_from_index(conn: sqlite3.Connection, index_name: str) -> List[str]:
    cursor = conn.execute(f"PRAGMA index_info({quote_ident(index_name)})")
    rows = cursor.fetchall()
    rows.sort(key=lambda r: r[1])  # seqno
    return [row[2] for row in rows]


def determine_key_columns(
    conn: sqlite3.Connection, table: str
) -> Sequence[str]:
    if table in PRIMARY_KEY_CANDIDATES:
        existing_columns = {col[1] for col in fetch_table_columns(conn, table)}
        for candidate in PRIMARY_KEY_CANDIDATES[table]:
            if all(column in existing_columns for column in candidate):
                return candidate
        raise RuntimeError(
            f"Cannot match any primary key candidate for table {table!r}. "
            f"Available columns: {sorted(existing_columns)}"
        )

    indexes = conn.execute(
        f"PRAGMA index_list({quote_ident(table)})"
    ).fetchall()
    candidates: List[Tuple[str, Sequence[str]]] = []
    for idx in indexes:
        idx_name = idx[1]
        if idx_name and any(
            pattern in idx_name for pattern in PRIMARY_KEY_INDEX_PATTERNS
        ):
            candidates.append((idx_name, columns_from_index(conn, idx_name)))

    if not candidates:
        raise RuntimeError(
            f"Cannot determine key columns for table {table!r}: no matching index and no manual mapping."
        )
    if len(candidates) > 1:
        raise RuntimeError(
            f"Ambiguous primary key candidates for table {table!r}: {[name for name, _ in candidates]}"
        )

    return candidates[0][1]


def duplicate_group_count(
    conn: sqlite3.Connection, table: str, key_columns: Sequence[str]
) -> int:
    group_by = ", ".join(quote_ident(col) for col in key_columns)
    sql = (
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {quote_ident(table)} "
        f"GROUP BY {group_by} HAVING COUNT(*)>1)"
    )
    cursor = conn.execute(sql)
    return cursor.fetchone()[0]


def fetch_table_columns(
    conn: sqlite3.Connection, table: str
) -> List[sqlite3.Row]:
    cursor = conn.execute(f"PRAGMA table_info({quote_ident(table)})")
    rows = cursor.fetchall()
    if not rows:
        raise RuntimeError(
            f"Failed to fetch column metadata for table {table!r}."
        )
    return rows


def build_create_table_sql(
    table: str, columns: Sequence[sqlite3.Row], pk_columns: Sequence[str]
) -> str:
    column_defs: List[str] = []
    for cid, name, col_type, notnull, dflt_value, _pk in columns:
        pieces = [quote_ident(name)]
        if col_type:
            pieces.append(col_type)
        if notnull:
            pieces.append("NOT NULL")
        if dflt_value is not None:
            pieces.append(f"DEFAULT {dflt_value}")
        column_defs.append(" ".join(pieces))

    pk_clause = (
        f"PRIMARY KEY ({', '.join(quote_ident(col) for col in pk_columns)})"
    )
    inner = ",\n  ".join(column_defs + [pk_clause])
    return f"CREATE TABLE {quote_ident(table)} (\n  {inner}\n);"


def fetch_indexes(
    conn: sqlite3.Connection, table: str
) -> List[Tuple[str, str]]:
    cursor = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,),
    )
    return cursor.fetchall()


def fetch_triggers(
    conn: sqlite3.Connection, table: str
) -> List[Tuple[str, str]]:
    cursor = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? AND sql IS NOT NULL",
        (table,),
    )
    return cursor.fetchall()


def rebuild_table(
    conn: sqlite3.Connection, table: str, pk_columns: Sequence[str]
) -> None:
    columns = fetch_table_columns(conn, table)
    create_sql = build_create_table_sql(table, columns, pk_columns)
    column_names = [quote_ident(row[1]) for row in columns]
    column_list = ", ".join(column_names)
    temp_table = f"{table}__old"

    indexes = fetch_indexes(conn, table)
    # Skip recreating redundant indexes that mirror the primary key.
    indexes_to_recreate = [
        (name, sql)
        for name, sql in indexes
        if not any(pattern in name for pattern in PRIMARY_KEY_INDEX_PATTERNS)
    ]
    triggers = fetch_triggers(conn, table)

    for name, _ in indexes:
        conn.execute(f"DROP INDEX IF EXISTS {quote_ident(name)}")

    for name, _ in triggers:
        conn.execute(f"DROP TRIGGER IF EXISTS {quote_ident(name)}")

    conn.execute(
        f"ALTER TABLE {quote_ident(table)} RENAME TO {quote_ident(temp_table)}"
    )
    conn.execute(create_sql)
    conn.execute(
        f"INSERT INTO {quote_ident(table)} ({column_list}) "
        f"SELECT {column_list} FROM {quote_ident(temp_table)}"
    )

    for name, sql in indexes_to_recreate:
        conn.execute(sql)

    for _, trigger_sql in triggers:
        conn.execute(trigger_sql)

    conn.execute(f"DROP TABLE {quote_ident(temp_table)}")


def main(database: Path) -> None:
    if not database.exists():
        raise SystemExit(f"Database file {database} does not exist.")

    with closing(sqlite3.connect(str(database))) as conn:
        conn.row_factory = sqlite3.Row
        tables = fetch_all_tables(conn)
        targets: Dict[str, Sequence[str]] = {}
        duplicates: Dict[str, Tuple[Sequence[str], int]] = {}

        for table in tables:
            if table in SKIP_TABLES:
                print(f"Skipping table {table} (marked for manual review).")
                continue
            if table_has_primary_key(conn, table):
                continue
            key_columns = determine_key_columns(conn, table)
            dup_count = duplicate_group_count(conn, table, key_columns)
            if dup_count:
                duplicates[table] = (key_columns, dup_count)
                continue
            targets[table] = key_columns

        if duplicates:
            print(
                "Duplicate key values detected. No schema changes were applied."
            )
            for table, (cols, dup_count) in sorted(duplicates.items()):
                col_list = ", ".join(cols)
                print(
                    f"  - {table}: {dup_count} duplicate group(s) for columns ({col_list})"
                )
            raise SystemExit(1)

        if not targets:
            print("All tables already have primary keys. Nothing to do.")
            return

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        try:
            for table, pk_columns in targets.items():
                print(
                    f"Rebuilding {table} with primary key on ({', '.join(pk_columns)})..."
                )
                rebuild_table(conn, table, pk_columns)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

        print("Primary keys successfully added to all target tables.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add primary keys to SQLite tables lacking them."
    )
    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Path to the SQLite database to update.",
    )
    args = parser.parse_args()
    main(args.db)
