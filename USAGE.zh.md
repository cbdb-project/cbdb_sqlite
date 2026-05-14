# CBDB SQLite 数据库使用指南

本文档简要介绍获取、处理与查询 CBDB SQLite 数据库的常见流程。

## 准备环境
- 操作系统：macOS，Linux 以及 Windows WSL 等类 Unix 操作系统。
- 必备工具：`python3`、`sqlite3`、`wget`。解压缩旧版本数据库可能需要 `unrar` 或 `7z`。

### 安装方法示例
- **Debian/Ubuntu 等基于 Debian 的发行版**：
  ```bash
  sudo apt update
  sudo apt install -y sqlite3 python3 wget
  ```
- **macOS（通过 Homebrew）**：
  ```bash
  brew update
  brew install sqlite python@3 wget
  ```

如果需要处理旧版压缩格式，可额外安装 `unrar` 或 `p7zip-full` 工具。

如需在系统中保留多个 Python 版本，可使用 `pyenv` 等工具创建项目隔离环境。

## 获取数据库
1. 下载 `latest.json` 获取当前版本的元数据：

  ```bash
  wget -O latest.json https://github.com/cbdb-project/cbdb_sqlite/raw/refs/heads/master/latest.json
  ```

   `latest.json` 包含发布日期、文件名、SHA-256 校验值以及 HuggingFace 下载链接。如需下载历史版本： https://huggingface.co/datasets/cbdb/cbdb-sqlite/tree/main/history

2. 根据 `latest.json` 中的链接下载数据库压缩包：

  ```bash
  wget -O cbdb_latest.zip $(python3 -c "import json; print(json.load(open('latest.json'))['huggingface_url'])")
  ```

3. 解压缩：

  ```bash
  unzip cbdb_latest.zip
  ```

完成后，数据库文件名如 `latest.json` 中 `sqlite_filename` 字段所示（例如 `cbdb_20260509.sqlite3`）。可将压缩包删掉：

  ```bash
  rm cbdb_latest.zip latest.json
  ```

## 常见查询示例

使用 `sqlite3` 交互模式快速检查数据：

  ```bash
  sqlite3 cbdb_20260509.sqlite3
  ```

退出 `sqlite3` 客户端：输入 `.quit`。

也可直接运行 `sqlite3` 命令进行单个查询：

- 查看表结构：`sqlite3 cbdb_20260509.sqlite3 '.schema BIOG_MAIN'`
- 统计人物数量：`sqlite3 cbdb_20260509.sqlite3 'SELECT COUNT(*) FROM BIOG_MAIN;'`
- 通过姓名模糊查询：`sqlite3 cbdb_20260509.sqlite3 'SELECT c_personid, c_alt_name, c_alt_name_chn FROM ALTNAME_DATA WHERE c_alt_name_chn LIKE "%王%" LIMIT 20;'`
- 查询人物与地名关联：`sqlite3 cbdb_20260509.sqlite3 'SELECT * FROM BIOG_ADDR_DATA WHERE c_personid = 100;'`


## 常见问题

- **数据库被锁定**：确保没有遗留的 `sqlite3` 或 Python 进程占用数据库文件。必要时可将锁定的进程 kill 掉。
- **缺少 unzip**：最新版本数据库使用 ZIP 格式压缩。在 Ubuntu 上可通过 `apt install unzip` 安装，在 macOS 上可通过 `brew install unzip` 安装。
