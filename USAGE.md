# CBDB SQLite Usage Guide

[中文文档](./USAGE.zh.md)

This document summarizes the common workflow for obtaining, processing, and querying the CBDB SQLite database.

## Prepare the Environment
- Operating systems: macOS, Linux, and other Unix-like systems such as Windows WSL.
- Required tools: `python3`, `sqlite3`, `wget`, and `7z`. You may also need `unrar` to unpack older releases.

### Installation Examples
- **Debian/Ubuntu and other Debian-based distributions**:
  ```bash
  sudo apt update
  sudo apt install -y sqlite3 python3 wget p7zip-full
  ```
  Install `unrar` if you need to handle archives other than 7z.
- **macOS (via Homebrew)**:
  ```bash
  brew update
  brew install sqlite python@3 wget p7zip
  ```

Use tools such as `pyenv` if you need to maintain multiple Python versions and create project-specific environments.

## Download the Database
1. Fetch the latest database archive `latest.7z` (current dataset release date: 2025-05-20):
   ```bash
   wget https://github.com/cbdb-project/cbdb_sqlite/raw/refs/heads/master/latest.7z
   ```
   Historical releases are available at: https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history
2. Extract the archive with 7-Zip or unrar, depending on the file extension:
   ```bash
   7z x latest.7z
   ```

   After extraction the database is placed in the project root. You can rename it to `CBDB_20250520.db` and remove the archive:
   ```bash
   mv latest.db CBDB_20250520.db
   rm latest.7z
   ```

## Primary Keys
To keep the archive small, the `latest.7z` published by the cbdb_sqlite project does not include primary keys, indexes, or views. Add them as needed for your use cases.

To add primary keys, run the `scripts/add_primary_keys.py` helper:
```bash
python3 scripts/add_primary_keys.py --db CBDB_20250520.db
```

## Query Examples

Use the interactive `sqlite3` shell for quick data inspection:
```bash
sqlite3 CBDB_20250520.db
```

Exit the `sqlite3` shell with `.quit`.

You can also run individual queries directly:
- Table schema: `sqlite3 CBDB_20250520.db '.schema BIOG_MAIN'`
- Person count: `sqlite3 CBDB_20250520.db 'SELECT COUNT(*) FROM BIOG_MAIN;'`
- Fuzzy name search: `sqlite3 CBDB_20250520.db 'SELECT c_personid, c_alt_name, c_alt_name_chn FROM ALTNAME_DATA WHERE c_alt_name_chn LIKE "%王%" LIMIT 20;'`
- Person-to-place relationships: `sqlite3 CBDB_20250520.db 'SELECT * FROM BIOG_ADDR_DATA WHERE c_personid = 100;'`

## Frequently Asked Questions
- **Database is locked**: ensure no lingering `sqlite3` or Python process is holding the file. Kill the offending process if necessary.
- **7-Zip is missing**: the latest release uses 7-Zip compression. Install it with `apt install p7zip-full` on Ubuntu or `brew install p7zip` on macOS.
