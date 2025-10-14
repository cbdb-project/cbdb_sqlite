# CBDB SQLite 数据库使用指南

本文档简要介绍获取、处理与查询 CBDB SQLite 数据库的常见流程。

## 准备环境
- 操作系统：macOS，Linux 以及 Windows WSL 等类 Unix 操作系统。
- 必备工具：`python3`、`sqlite3`、`wget`、`7z`。解压缩旧版本数据库可能需要`unrar`。 

### 安装方法示例
- **Debian/Ubuntu 等基于 Debian 的发行版**：
  ```bash
  sudo apt update
  sudo apt install -y sqlite3 python3 wget p7zip-full
  ```
  如果需要处理 7z 以外的压缩格式，可额外安装 `unrar` 工具。
- **macOS（通过 Homebrew）**：
  ```bash
  brew update
  brew install sqlite python@3 wget p7zip
  ```

如需在系统中保留多个 Python 版本，可使用 `pyenv` 等工具创建项目隔离环境。

## 获取数据库
1. 下载本项目最新数据库： latest.7z (当前版本数据集发布日期为 2025-05-20)

  ```bash
  wget https://github.com/cbdb-project/cbdb_sqlite/raw/refs/heads/master/latest.7z
  ```

   如需下载历史版本： https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history
2. 根据发布版本的扩展名，使用 7-zip 或 unrar 软件解压缩。

  ```bash
  7z x latest.7z
  ```

完成后，数据库将位于项目根目录。可将数据库文件名改为 `CBDB_20250520.db`，并将压缩文件删掉。

  ```bash
  mv latest.db CBDB_20250520.db
  rm latest.7z
  ```

## 主键
为节省数据集体积， cbdb_sqlite 项目中发布的 latest.7z 不包含主键、索引、视图。用户可根据使用需求添加。

如需要主键，可运行 `scripts/add_primary_keys.py` 脚本添加主键如下：

  ```bash
  python3 scripts/add_primary_keys.py --db CBDB_20250520.db
  ```

## 常见查询示例

使用 `sqlite3` 交互模式快速检查数据：

  ```bash
  sqlite3 CBDB_20250520.db
  ```

退出 `sqlite3` 客户端：输入 `.quit`。

也可直接运行 `sqlite3` 命令进行单个查询：

- 查看表结构：`sqlite3 CBDB_20250520.db '.schema BIOG_MAIN'`
- 统计人物数量：`sqlite3 CBDB_20250520.db 'SELECT COUNT(*) FROM BIOG_MAIN;'`
- 通过姓名模糊查询：`sqlite3 CBDB_20250520.db 'SELECT c_personid, c_alt_name, c_alt_name_chn FROM ALTNAME_DATA WHERE c_alt_name_chn LIKE "%王%" LIMIT 20;'`
- 查询人物与地名关联：`sqlite3 CBDB_20250520.db 'SELECT * FROM BIOG_ADDR_DATA WHERE c_personid = 100;'`


## 常见问题

- **数据库被锁定**：确保没有遗留的 `sqlite3` 或 Python 进程占用数据库文件。必要时可将锁定的进程 kill 掉。
- **缺少 7z**：本项目最新版本数据库已使用 7-Zip 压缩。在 Ubuntu 上可以通过 `apt install p7zip-full` 安装。 在 macOS 上可通过 `brew install p7zip` 安装。
