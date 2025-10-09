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

announce "Ensuring output databases are absent"
ensure_absent "CBDB_20250520.db"
ensure_absent "CBDB_20240820.db"

LATEST_URL="https://huggingface.co/datasets/cbdb/cbdb-sqlite/resolve/main/latest.7z"
HIST_URL_BASE="https://huggingface.co/datasets/cbdb/cbdb-sqlite/resolve/main/history/CBDB_20240820"

latest_archive="$TMP_DIR/latest.7z"
announce "Downloading latest CBDB archive"
download_file "$LATEST_URL" "$latest_archive"

announce "Extracting latest archive"
extract_archive "$latest_archive" "$SCRIPT_DIR" &
extract_latest_pid=$!

announce "Downloading CBDB_20240820 archive parts"
part1="$TMP_DIR/CBDB_20240820.7z.001"
part2="$TMP_DIR/CBDB_20240820.7z.002"
download_file "$HIST_URL_BASE/CBDB_20240820.7z.001" "$part1" &
pid_part1=$!
download_file "$HIST_URL_BASE/CBDB_20240820.7z.002" "$part2" &
pid_part2=$!

wait "$extract_latest_pid"
if [[ ! -f "latest.db" ]]; then
    printf 'Error: expected latest.db after extraction but did not find it.\n' >&2
    exit 1
fi
mv latest.db CBDB_20250520.db

wait "$pid_part1"
wait "$pid_part2"

announce "Extracting CBDB_20240820 archive"
extract_archive "$part1" "$SCRIPT_DIR"

rm -f "$latest_archive" "$part1" "$part2"

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
