# cbdb_sqlite

You can download the latest SQLite release of the [China Biographical Database](https://cbdb.hsites.harvard.edu/) at the following page.

* https://huggingface.co/datasets/cbdb/cbdb-sqlite/blob/main/latest.zip

Check [**latest.json**](https://github.com/cbdb-project/cbdb_sqlite/blob/master/latest.json) for the current release date, filename, SHA-256 checksum, and direct download URL.

## Post-processing (optional)

The raw database export does not include convenience views or the denormalised `ADDRESSES` table.
Use the scripts in [`scripts/`](./scripts/) to add them, or run the one-click Colab notebook:

| What you want | How to get it |
|---------------|---------------|
| Everything in one click | Open [`scripts/setup_cbdb.ipynb`](./scripts/setup_cbdb.ipynb) in Google Colab |
| Foreign key constraints | `python scripts/add_foreign_keys.py --db latest.db` |
| 18 convenience views | `bash scripts/create_views.sh latest.db` |
| `ADDRESSES` hierarchy table | `python scripts/create_addresses_table.py --db latest.db` |

See [`scripts/README.md`](./scripts/README.md) for full documentation.

## Data Limitations

* The ZZZ releases are now deprecated in favor of views. Use [`create_views.sh`](./scripts/create_views.sh) to create views in the SQLite file.

## Historical releases

For historical SQLite databases:

* [CBDB Hugging Face dataset](https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history)

The released database file in GitHub repository is no longer updated.

* [**latest_ZZZ_tables.7z**](https://huggingface.co/datasets/cbdb/cbdb-sqlite/blob/main/latest_ZZZ_tables.7z) - The latest SQLite version of the CBDB that includes de-normalized tables.
