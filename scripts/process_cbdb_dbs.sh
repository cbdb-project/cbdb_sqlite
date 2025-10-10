#!/usr/bin/env bash

set -euo pipefail
IFS=$'\n\t'

readonly REQUIRED_TOOLS=(wget 7z sqlite3 python3)

announce() {
    printf '\n==> %s\n' "$1"
}

missing_tools=()
for tool in "${REQUIRED_TOOLS[@]}"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        missing_tools+=("$tool")
    fi
done

if ((${#missing_tools[@]} > 0)); then
    printf 'Error: missing required tool(s): %s\n' "${missing_tools[*]}" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cbdb-dl.XXXXXX")"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

download_file() {
    local url="$1"
    local destination="$2"

    printf '  %s\n' "$(basename "$destination")"
    wget --quiet --show-progress --progress=bar:force:noscroll -O "$destination" "$url"
}

extract_archive() {
    local archive="$1"
    local output_dir="$2"

    printf 'Extracting %s...\n' "$(basename "$archive")"
    7z x "$archive" -o"$output_dir" -y >/dev/null
}

ensure_absent() {
    local path="$1"
    if [[ -e "$path" ]]; then
        printf 'Error: %s already exists. Move or remove it before running this script.\n' "$path" >&2
        exit 1
    fi
}

TARGET_DBS=("CBDB_20250520.db" "CBDB_20240820.db")

announce "Ensuring output databases are absent"
for db in "${TARGET_DBS[@]}"; do
    ensure_absent "$db"
done

download_release() {
    local db_name="$1"
    shift
    local urls=("$@")
    local label="${db_name%.db}"

    if ((${#urls[@]} == 0)); then
        printf 'Error: no archive URLs provided for %s\n' "$db_name" >&2
        exit 1
    fi

    announce "Downloading ${label} archive"
    local archive_paths=()
    for url in "${urls[@]}"; do
        local destination="$TMP_DIR/${url##*/}"
        download_file "$url" "$destination"
        archive_paths+=("$destination")
    done

    announce "Extracting ${label} archive"
    extract_archive "${archive_paths[0]}" "$SCRIPT_DIR"

    if [[ ! -f "$db_name" ]]; then
        printf 'Error: expected %s after extraction but did not find it.\n' "$db_name" >&2
        exit 1
    fi

    rm -f "${archive_paths[@]}"
}

download_release "CBDB_20250520.db" \
    "https://huggingface.co/datasets/cbdb/cbdb-sqlite/resolve/main/history/CBDB_20250520/CBDB_20250520.7z"

download_release "CBDB_20240820.db" \
    "https://huggingface.co/datasets/cbdb/cbdb-sqlite/resolve/main/history/CBDB_20240820/CBDB_20240820.7z.001" \
    "https://huggingface.co/datasets/cbdb/cbdb-sqlite/resolve/main/history/CBDB_20240820/CBDB_20240820.7z.002"

announce "Adding primary keys"
python3 add_primary_keys.py --db CBDB_20250520.db
python3 add_primary_keys.py --db CBDB_20240820.db

# optional: remove indexes (saves space); uncomment if preferred
# sqlite3 CBDB_20250520.db "SELECT 'DROP INDEX IF EXISTS \"' || name || '\";' FROM sqlite_master WHERE type='index';" | sqlite3 CBDB_20250520.db
# sqlite3 CBDB_20240820.db "SELECT 'DROP INDEX IF EXISTS \"' || name || '\";' FROM sqlite_master WHERE type='index';" | sqlite3 CBDB_20240820.db

announce "Vacuuming databases"
sqlite3 CBDB_20250520.db 'VACUUM;'
sqlite3 CBDB_20240820.db 'VACUUM;'

announce "Comparing databases"
python3 compare_db_tables.py CBDB_20240820.db CBDB_20250520.db

# generate sqldiff file (outputs a 1.4GB sql_diff.sql file)
# sqldiff --primarykey CBDB_20240820.db CBDB_20250520.db > sql_diff.sql
