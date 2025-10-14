# pip install mysql-connector-python
 
import mysql.connector
import json
import os
import sys
from typing import Dict, List, Tuple

# Tables that should be excluded from the generated mapping because they do not
# appear in the distributed SQLite release (for example Laravel's OAUTH_* tables).
TABLE_BLACKLIST = {
    "MIGRATIONS",
    "OAUTH_ACCESS_TOKENS",
    "OAUTH_AUTH_CODES",
    "OAUTH_CLIENTS",
    "OAUTH_PERSONAL_ACCESS_CLIENTS",
    "OAUTH_REFRESH_TOKENS",
    "OPERATIONS",
    "PINYIN",
    "USERS",
}

def main():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="***",
            password="***",
            database="cbdb_data"
        )
        cursor = conn.cursor()

        # Detect current default database
        cursor.execute("SELECT DATABASE()")
        db_row = cursor.fetchone()
        if not db_row or not db_row[0]:
            print("No default database selected. Please select a database before running this script.")
            sys.exit(1)
        db_name = db_row[0]

        # Query for tables with PRIMARY KEYs and their columns (ordered)
        query = """
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION
        FROM
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
            TABLE_SCHEMA = %s
            AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY
            TABLE_NAME ASC,
            ORDINAL_POSITION ASC
        """
        cursor.execute(query, (db_name,))
        rows = cursor.fetchall()

        # Build mapping: table -> tuple of primary key columns (ordered)
        pk_map: Dict[str, List[str]] = {}
        for table, column, _ in rows:
            table_upper = table.upper()
            if table_upper in TABLE_BLACKLIST:
                continue
            if table_upper not in pk_map:
                pk_map[table_upper] = []
            pk_map[table_upper].append(column)

        # Convert to required structure: Dict[str, Tuple[Tuple[str, ...], ...]]
        # (one tuple of columns per table, but MySQL only allows one PK per table)
        PRIMARY_KEY_CANDIDATES: Dict[str, Tuple[Tuple[str, ...], ...]] = {
            t: (tuple(cols),) for t, cols in pk_map.items()
        }

        # Write to pretty-printed JSON (lists for JSON compatibility)
        output = {
            k: [list(tup) for tup in v]
            for k, v in sorted(PRIMARY_KEY_CANDIDATES.items())
        }
        out_path = os.path.join(os.path.dirname(__file__), "primary_keys.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # Print summary
        table_count = len(output)
        sample_table = next(iter(output)) if output else None
        print(f"Primary key mapping written to: {out_path}")
        print(f"Tables included: {table_count}")
        if sample_table:
            print(f"Sample entry:\n  {sample_table}: {output[sample_table]}")

    except mysql.connector.Error as err:
        print(f"MySQL error: {err}")
        sys.exit(2)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(3)
    finally:
        try:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
