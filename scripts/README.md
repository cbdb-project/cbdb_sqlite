# CBDB Utility Scripts

[中文文档](./README.zh.md)

This directory contains helper scripts for downloading, post-processing, and comparing CBDB SQLite releases.

## Available Scripts

### One-stop notebook

- **`setup_cbdb.ipynb`** — Google Colab notebook that runs the full setup pipeline in one click:
  downloads the latest database, adds foreign keys, creates views, and builds the `ADDRESSES` table.
  [![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1fTg-Ng1nRB1jcTe7Ii2mhUqh0OUdPPEO) — click to open directly, then click **Runtime → Run all**.
  Each step can be toggled on or off via boolean flags in the *Configuration* cell.

### Individual scripts

| Script | Description |
|--------|-------------|
| `add_foreign_keys.py` | Fetches `foreign_keys_regen.csv` from GitHub and recreates SQLite tables with proper `FOREIGN KEY` constraints. Skips tables that already have FK constraints (idempotent). |
| `create_views.sh` | Creates 18 convenience SQL views (e.g. `View_PeopleData`, `View_EntryData`, `View_PostingOfficeData`). |
| `create_addresses_table.py` | Builds the `ADDRESSES` table by resolving the full administrative hierarchy for each address across time, preserving gaps in the data. |
| `compare_db_tables.py` | Compares two SQLite databases table-by-table, emitting row-count and schema discrepancies. |
| `process_cbdb_dbs.sh` | End-to-end workflow: downloads the latest and a historical SQLite dump, unpacks them, vacuums both, and runs `compare_db_tables.py`. |

## Prerequisites

### For the Colab notebook (`setup_cbdb.ipynb`)

No local installation needed — just upload to Google Colab.

### For running scripts locally

| Tool | Required by |
|------|-------------|
| `python3` | `add_foreign_keys.py`, `create_addresses_table.py`, `compare_db_tables.py` |
| `sqlite3` CLI | `create_views.sh` |
| `bash` | `create_views.sh`, `process_cbdb_dbs.sh` |
| `wget`, `7z` | `process_cbdb_dbs.sh` |

`process_cbdb_dbs.sh` checks for missing tools at startup and exits early if any are absent.

## Usage

### Add foreign keys

```bash
python scripts/add_foreign_keys.py --db latest.db
```

Pass `--csv-url URL` to use a different branch of `foreign_keys_regen.csv`.

### Create views

```bash
bash scripts/create_views.sh latest.db
```

### Build ADDRESSES table

```bash
python scripts/create_addresses_table.py --db latest.db
```

### Compare two releases

```bash
python scripts/compare_db_tables.py old.db new.db
```

### Download and compare historical releases

```bash
bash scripts/process_cbdb_dbs.sh
```

Intermediate downloads are written to a temporary directory and cleaned up automatically.
