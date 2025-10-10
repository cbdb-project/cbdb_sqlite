# CBDB 脚本说明

此目录包含用于 CBDB 项目的辅助脚本，脚本本体保留在仓库根目录，便于直接执行。运行时请根据下述说明使用相对路径调用对应文件。

## 脚本一览

- `process_cbdb_dbs.sh`：完整流程脚本，负责下载最新与历史版 SQLite 数据库、解压、运行规范化工具、执行 `VACUUM`，并生成数据库差异报告。
- `add_primary_keys.py`：为缺少显式主键的表重建结构，在单个事务内创建带主键的新表并复制数据。
- `compare_db_tables.py`：逐表对比两个 SQLite 数据库的结构与数据，输出差异摘要。

## 运行前提

请确认已安装以下命令行工具：

- `wget`
- `7z`
- `sqlite3`
- `python3`
- `mysql-connector-python` (create_primary_key_json[maintained by CBDB].py)

`process_cbdb_dbs.sh` 会在启动时检查依赖，缺失工具时会直接报错退出。

## 使用提示

- 从仓库根目录执行：`./process_cbdb_dbs.sh`。
- 两个 Python 工具均可通过 `--help` 查看详细参数。
- 脚本会创建临时目录存放下载文件并在结束时清理，生成的数据库位于脚本所在目录。
