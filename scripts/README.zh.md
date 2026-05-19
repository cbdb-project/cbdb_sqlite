# CBDB 脚本说明

此目录包含用于下载、后处理及比较 CBDB SQLite 发布版本的辅助脚本。

## 脚本一览

### 一键 Notebook

- **`setup_cbdb.ipynb`** — Google Colab Notebook，一键完成完整配置流程：下载最新数据库、添加外键、创建视图、生成 `ADDRESSES` 表。
  上传至 [Google Colab](https://colab.research.google.com/) 后点击 **Runtime → Run all** 即可运行。
  每个步骤均可在 *Configuration* 单元格中通过布尔变量单独开关。

### 独立脚本

| 脚本 | 说明 |
|------|------|
| `add_foreign_keys.py` | 从 GitHub 读取 `foreign_keys_regen.csv`，将缺少外键的 SQLite 表重建并补充 `FOREIGN KEY` 约束。已有外键的表会自动跳过（幂等操作）。 |
| `create_views.sh` | 创建 18 个便于查询的 SQL 视图（如 `View_PeopleData`、`View_EntryData`、`View_PostingOfficeData` 等）。 |
| `create_addresses_table.py` | 通过解析地址在各时间段内的行政区划层级关系，构建 `ADDRESSES` 表，并保留数据中的空缺时段。 |
| `compare_db_tables.py` | 逐表对比两个 SQLite 数据库的行数与结构，输出差异摘要。 |
| `process_cbdb_dbs.sh` | 完整流程脚本：下载最新版和某一历史版 SQLite 数据库，解压后执行 `VACUUM`，并调用 `compare_db_tables.py` 生成对比报告。 |

## 运行前提

### Colab Notebook（`setup_cbdb.ipynb`）

无需本地安装，直接上传至 Google Colab 使用。

### 本地运行脚本

| 工具 | 所需脚本 |
|------|----------|
| `python3` | `add_foreign_keys.py`、`create_addresses_table.py`、`compare_db_tables.py` |
| `sqlite3` CLI | `create_views.sh` |
| `bash` | `create_views.sh`、`process_cbdb_dbs.sh` |
| `wget`、`7z` | `process_cbdb_dbs.sh` |

`process_cbdb_dbs.sh` 启动时会检查依赖，缺少工具时会直接报错退出。

## 使用方法

### 添加外键

```bash
python scripts/add_foreign_keys.py --db latest.db
```

可通过 `--csv-url URL` 指定其他分支的 `foreign_keys_regen.csv`。

### 创建视图

```bash
bash scripts/create_views.sh latest.db
```

### 生成 ADDRESSES 表

```bash
python scripts/create_addresses_table.py --db latest.db
```

### 比较两个发布版本

```bash
python scripts/compare_db_tables.py old.db new.db
```

### 下载历史版本并对比

```bash
bash scripts/process_cbdb_dbs.sh
```

下载文件会写入临时目录，脚本结束后自动清理。
