# cbdb_sqlite

You can download the latest SQLite release of the [China Biographical Database](https://cbdb.hsites.harvard.edu/) at the following page.

* https://huggingface.co/datasets/cbdb/cbdb-sqlite/blob/main/latest.zip

## Data Limitations

* The ZZZ releases are now deprecated in favor of views. Use [`create_views.sh`](./scripts/create_views.sh) to create views in the SQLite file.
* The SQLite releases after 2025-05 may not include up-to-date **Index Year** (`BIOG_MAIN.c_index_year`), **Index Year Type Code** (`BIOG_MAIN.c_index_year_type_code`) and **Index Year Source ID** (`BIOG_MAIN.c_index_year_source_id`) information.

## Historical releases

For historical SQLite databases:

* [CBDB Hugging Face dataset](https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history)

The released database file in GitHub repository is no longer updated.

* [**latest.7z**](./latest.7z) - Last updated on 2026-02-08. 
* [**latest_ZZZ_tables.7z**](https://huggingface.co/datasets/cbdb/cbdb-sqlite/blob/main/latest_ZZZ_tables.7z) - The latest SQLite version of the CBDB that includes de-normalized tables.
