# spark-viewer-tui

A terminal UI for browsing and querying Delta Lake and Parquet tables with Apache Spark.

Built with [Textual](https://textual.textualize.io/) and [PySpark](https://spark.apache.org/docs/latest/api/python/).

## Features

- **Catalog Browser** - Sidebar tree with databases and tables
- **SQL Editor** - Write and execute Spark SQL queries with syntax highlighting
- **Results Table** - View query results with column types and row count
- **Scan Paths** - Auto-register Delta/Parquet folders as Spark tables
- **Rescan** - Refresh tables on demand (folders are live, Ctrl+S rescans)
- **Save/Load Queries** - Persist frequently used queries
- **Themes** - Multiple color themes (Transparent, Dracula, Gruvbox)
- **Maximize** - Focus on editor or results in full screen

## Requirements

- Python 3.12+
- Java 17 (for PySpark)

## Installation

```bash
pip install spark-viewer-tui
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv pip install spark-viewer-tui
```

## Usage

```bash
spark-viewer
```

Or run directly from source:

```bash
uv run python app.py
```

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `F2` | Spark Configuration (metastore, warehouse, scan paths) |
| `F3` | Save current query |
| `F4` | Load saved query |
| `Ctrl+S` | Start Spark session / Rescan paths |
| `Ctrl+E` | Execute SQL query |
| `Ctrl+T` | Change theme |
| `Ctrl+W` | Maximize editor or results |
| `Ctrl+C` | Exit |

## Getting Started

1. Run `spark-viewer`
2. Press `F2` to configure:
   - **Metastore DB Path** - Where Spark stores metadata (e.g. `/tmp/metastore_db`)
   - **Warehouse Dir Path** - Spark warehouse directory (e.g. `/tmp/spark-warehouse`)
   - **Scan Paths** - Folders to scan for Delta/Parquet tables
3. Press `Ctrl+S` to start the Spark session
4. Click a table in the sidebar or write SQL in the editor
5. Press `Ctrl+E` to run the query

## Scan Paths

Scan paths auto-register Delta and Parquet tables from a directory. Each scan path has a **database name** and a **folder path**.

```
db_name: vendas
path:    /data/warehouse
```

Subfolders are registered as tables:
- Subfolder with `_delta_log/` -> Delta table
- Subfolder with `.parquet` files -> Parquet table

Every `Ctrl+S` drops and recreates the databases from scan paths, keeping tables in sync with the filesystem.

## Configuration

Settings are saved in `spark_config.json` in the project directory:

```json
{
  "metastore_db": "/tmp/metastore_db",
  "warehouse_dir": "/tmp/spark-warehouse",
  "scan_paths": [
    { "path": "/data/warehouse", "db_name": "vendas" },
    { "path": "/data/lake", "db_name": "analytics" }
  ]
}
```

## Project Structure

```
spark-viewer-tui/
├── app.py              # Main application
├── config.py           # Configuration management
├── spark_manager.py    # Spark session and table registration
├── queries.py          # Query persistence
├── themes.py           # Theme system
├── screens/
│   ├── spark_config.py    # Spark config modal (F2)
│   ├── save_query.py      # Save query modal (F3)
│   ├── load_query.py      # Load query modal (F4)
│   └── theme_selector.py  # Theme selector modal (Ctrl+T)
└── pyproject.toml
```

## License

MIT
