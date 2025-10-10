import sqlite3
import sys
import argparse
import traceback

def remove_primary_keys_and_indexes(db_path):
    summary = []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all user tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row['name'] for row in cur.fetchall()]

        for table in tables:
            actions = []
            # Get original schema
            cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            row = cur.fetchone()
            if not row or not row['sql']:
                actions.append(f"Could not find schema for table {table}")
                summary.append((table, actions))
                continue
            orig_schema = row['sql']

            # Remove PRIMARY KEY constraints from schema
            import re
            # Remove inline PRIMARY KEY (e.g., "id INTEGER PRIMARY KEY")
            schema_no_pk = re.sub(r'\bPRIMARY\s+KEY\b(\s*\([^)]+\))?', '', orig_schema, flags=re.IGNORECASE)
            # Remove trailing commas left by removal
            schema_no_pk = re.sub(r',\s*\)', ')', schema_no_pk)
            # Remove redundant spaces
            schema_no_pk = re.sub(r'\s+', ' ', schema_no_pk)
            # Remove PRIMARY KEY table constraints (e.g., ", PRIMARY KEY (id)")
            schema_no_pk = re.sub(r',\s*PRIMARY\s+KEY\s*\([^)]+\)', '', schema_no_pk, flags=re.IGNORECASE)

            # Get columns
            cur.execute(f"PRAGMA table_info('{table}')")
            columns = [row['name'] for row in cur.fetchall()]

            # Get indexes
            cur.execute(f"PRAGMA index_list('{table}')")
            indexes = [row['name'] for row in cur.fetchall()]

            # Start transaction
            cur.execute("BEGIN")
            tmp_table = f"{table}_tmp_remove_pk"
            try:
                # Create new table without PK
                cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
                cur.execute(schema_no_pk.replace(table, tmp_table, 1))
                actions.append("Created temp table without PRIMARY KEY.")

                # Copy data
                col_str = ', '.join([f'"{col}"' for col in columns])
                cur.execute(f'INSERT INTO {tmp_table} ({col_str}) SELECT {col_str} FROM {table}')
                actions.append("Copied data to temp table.")

                # Drop old table
                cur.execute(f"DROP TABLE {table}")
                actions.append("Dropped original table.")

                # Rename temp table
                cur.execute(f"ALTER TABLE {tmp_table} RENAME TO {table}")
                actions.append("Renamed temp table to original name.")

                # Drop indexes
                for idx in indexes:
                    cur.execute(f"DROP INDEX IF EXISTS \"{idx}\"")
                if indexes:
                    actions.append(f"Dropped indexes: {', '.join(indexes)}")
                else:
                    actions.append("No indexes to drop.")

                conn.commit()
            except Exception as e:
                conn.rollback()
                actions.append(f"Error processing table {table}: {e}")
                actions.append(traceback.format_exc())
            summary.append((table, actions))
        # vacuum to clean up
        conn.execute("VACUUM")
        conn.close()
    except Exception as e:
        summary.append(("DB ERROR", [str(e), traceback.format_exc()]))

    # Print summary
    print("Summary of actions:")
    for table, actions in summary:
        print(f"Table: {table}")
        for act in actions:
            print(f"  - {act}")
        print("-" * 40)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove primary keys and indexes from a SQLite database.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file")
    args = parser.parse_args()
    remove_primary_keys_and_indexes(args.db)