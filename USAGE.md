# CBDB SQLite Usage Guide

[中文文档](./USAGE.zh.md)

This document summarizes the common workflow for obtaining, processing, and querying the CBDB SQLite database.

## Prepare the Environment
- Operating systems: macOS, Linux, and other Unix-like systems such as Windows WSL.
- Required tools: `python3`, `sqlite3`, and `wget`. You may also need `unrar` or `7z` to unpack older releases.

### Installation Examples
- **Debian/Ubuntu and other Debian-based distributions**:
  ```bash
  sudo apt update
  sudo apt install -y sqlite3 python3 wget
  ```
- **macOS (via Homebrew)**:
  ```bash
  brew update
  brew install sqlite python@3 wget
  ```

Install `unrar` or `p7zip-full` if you need to handle older version archives.

Use tools such as `pyenv` if you need to maintain multiple Python versions and create project-specific environments.

## Download the Database
1. Fetch `latest.json` to find the current release metadata:
   ```bash
   wget -O latest.json https://github.com/cbdb-project/cbdb_sqlite/raw/refs/heads/master/latest.json
   ```
   `latest.json` contains the release date, filename, SHA-256 checksum, and HuggingFace download URL. Historical releases are available at: https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history
2. Download the database archive using the URL from `latest.json`:
   ```bash
   wget -O cbdb_latest.zip $(python3 -c "import json; print(json.load(open('latest.json'))['huggingface_url'])")
   ```
3. Extract the archive:
   ```bash
   unzip cbdb_latest.zip
   ```

   After extraction the database file is named as shown in `latest.json` under `sqlite_filename` (e.g. `cbdb_20260509.sqlite3`). Remove the archive when done:
   ```bash
   rm cbdb_latest.zip latest.json
   ```

## Query Examples

Use the interactive `sqlite3` shell for quick data inspection:
```bash
sqlite3 cbdb_20260509.sqlite3
```

Exit the `sqlite3` shell with `.quit`.

You can also run individual queries directly:
- Table schema: `sqlite3 cbdb_20260509.sqlite3 '.schema BIOG_MAIN'`
- Person count: `sqlite3 cbdb_20260509.sqlite3 'SELECT COUNT(*) FROM BIOG_MAIN;'`
- Fuzzy name search: `sqlite3 cbdb_20260509.sqlite3 'SELECT c_personid, c_alt_name, c_alt_name_chn FROM ALTNAME_DATA WHERE c_alt_name_chn LIKE "%王%" LIMIT 20;'`
- Person-to-place relationships: `sqlite3 cbdb_20260509.sqlite3 'SELECT * FROM BIOG_ADDR_DATA WHERE c_personid = 100;'`

## Frequently Asked Questions
- **Database is locked**: ensure no lingering `sqlite3` or Python process is holding the file. Kill the offending process if necessary.
- **unzip is missing**: the latest release uses ZIP compression. Install it with `apt install unzip` on Ubuntu or `brew install unzip` on macOS.
