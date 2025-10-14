# CBDB Utility Scripts

[中文文档](./README.zh.md)

This directory contains the helper scripts used to download, normalise, and compare CBDB SQLite releases. The scripts themselves live in the project root so they can be executed directly without adjusting `PATH`; refer to their relative locations when running the commands below.

## Available Scripts

- `process_cbdb_dbs.sh`: end-to-end workflow that downloads the latest and historical SQLite dumps, unpacks them, applies the normalisation helpers, vacuums the databases, and generates a schema/data summary comparison.
- `add_primary_keys.py`: rebuilds tables that lack explicit primary keys by creating replacement tables with the desired constraints and copying data across inside a single transaction.
- `compare_db_tables.py`: compares two SQLite databases table-by-table, emitting a report of schema and data discrepancies.

## Prerequisites

The scripts expect the following command line tools:

- `wget`
- `7z`
- `sqlite3`
- `python3`

Install the tools before running the scripts. `process_cbdb_dbs.sh` will perform a sanity check and exit early if any are missing.

## Usage Notes

- Run the shell script from the repository root: `./process_cbdb_dbs.sh`.
- Both Python utilities accept `--help` for detailed argument listings.
- Intermediate downloads are written to a temporary directory and cleaned up automatically; resulting databases are created alongside the scripts.
