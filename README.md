# spark-viewer-tui

A terminal UI for browsing and querying Delta Lake and Parquet tables with Apache Spark.

Built with [Textual](https://textual.textualize.io/) and [PySpark](https://spark.apache.org/docs/latest/api/python/).

GitHub: https://github.com/eritondev-stack/spark-viewer-tui

![spark-viewer-tui](spark-view.png)

[video demo]("https://drive.google.com/file/d/16ZDCjVVPLh7t9tRZa_h-kNbk3BJLgfB0/preview")



## Features

- **Catalog Browser** - Sidebar tree with databases and tables
- **SQL Editor** - Write and execute Spark SQL queries with syntax highlighting
- **Results Table** - View query results with column types and row count
- **`print_df`** - Send DataFrames from any script to the TUI in real time (see below)
- **Scan Paths** - Auto-register Delta/Parquet folders as Spark tables
- **Rescan** - Refresh tables on demand (folders are live, Ctrl+R rescans)
- **Save/Load Queries** - Persist frequently used queries
- **Themes** - Multiple color themes (Transparent, Dracula, Gruvbox)
- **Maximize** - Focus on editor or results in full screen

## Requirements

- Python 3.9+
- Java 17 (for PySpark) — must be available via `JAVA_HOME` or `java` in your `PATH`

### Java Setup

**macOS (Homebrew):**
```bash
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

**Linux (Debian/Ubuntu):**
```bash
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Add the `export JAVA_HOME=...` line to your `~/.bashrc` or `~/.zshrc` to make it persistent.

**Verify:**
```bash
java -version
```

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
uv run spark-viewer
```

The Spark session starts automatically on launch. No configuration required to get started — F2 is only needed if you want to connect to an existing metastore or scan paths for Delta/Parquet files.

## Keyboard Shortcuts

| Key | Action |
|---|---|
| `F2` | Spark Configuration (metastore, warehouse, scan paths) |
| `F3` | Save current query |
| `F4` | Load saved query |
| `Ctrl+R` | Rescan configured paths and refresh catalog |
| `Ctrl+E` | Execute SQL query |
| `Ctrl+T` | Change theme |
| `Ctrl+W` | Maximize editor or results |
| `Ctrl+C` | Exit |

## Getting Started

1. Run `spark-viewer` — Spark starts automatically
2. Click a table in the sidebar or write SQL in the editor
3. Press `Ctrl+E` to run the query

To load your own Delta/Parquet files, press `F2` and add scan paths.

---

## `print_df` — Live DataFrame Viewer

Send any Spark or Pandas DataFrame from your script to the running TUI. The DataFrame appears instantly in the sidebar under a database called **`live`** and can be queried with SQL.

### How it works

```
your_script.py  ──print_df()──►  TCP :7891  ──►  live.<table>  in the TUI
```

The TUI runs a lightweight TCP server on `localhost:7891`. `print_df` connects, sends the DataFrame as JSON, and the TUI registers it as an in-memory Spark table (`global_temp.<table>`), displayed as `live.<table>` in the sidebar.

### Usage

```python
from spark_viewer_tui import print_df

# Works with PySpark DataFrames
print_df(spark_df, "my_table")

# Works with Pandas DataFrames
print_df(pandas_df, "my_table")
```

The table appears in the sidebar under `live`. Click it to auto-generate and run `SELECT * FROM global_temp.my_table LIMIT 1000`, or write your own SQL query.

### PySpark example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, round as spark_round, when, col
from spark_viewer_tui import print_df

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.range(1, 101).select(
    col("id"),
    when(col("id") % 2 == 0, "Par").otherwise("Ímpar").alias("tipo"),
    spark_round(rand() * 1000, 2).alias("valor"),
)

print_df(df, "minha_tabela")
```

### Pandas example

```python
import pandas as pd
from spark_viewer_tui import print_df

df = pd.DataFrame({
    "produto": ["A", "B", "C"],
    "receita": [1200.50, 850.00, 3400.75],
    "ativo": [True, False, True],
})

print_df(df, "produtos")
```

### Notes

- The TUI must be running before calling `print_df`
- DataFrames are truncated to **10,000 rows** with a warning if larger
- Calling `print_df` with the same table name replaces the previous data
- Tables in `live` are in-memory only — they are lost when the TUI closes
- Maximum payload size: 256 MB

### Built-in example

Run the included example to see three DataFrames sent to the TUI at once:

```bash
# Terminal 1
spark-viewer

# Terminal 2 (after "Spark iniciado!" appears in the TUI)
spark-viewer-example
```

This sends three tables to the `live` database: `vendas`, `metricas_servidor`, and `resumo_categorias`.

---

## Seed (Example Data)

The package includes a seed command that creates 6 Delta tables with 500 rows each (employees, products, orders, customers, logs, metrics). Useful for testing and exploring the tool.

```bash
# Uses paths from spark_config.json
spark-viewer-seed

# Or specify paths manually
spark-viewer-seed --metastore-db ./metastore_db --warehouse-dir ./spark-warehouse
```

After seeding, run `spark-viewer` and press `Ctrl+R` to load the tables.

## Scan Paths

Scan paths auto-register Delta and Parquet tables from a directory. Each scan path has a **database name** and a **folder path**.

```
db_name: vendas
path:    /data/warehouse
```

Subfolders are registered as tables:
- Subfolder with `_delta_log/` -> Delta table
- Subfolder with `.parquet` files -> Parquet table

Every `Ctrl+R` (Refresh Catalog) drops and recreates the databases from scan paths, keeping tables in sync with the filesystem.

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

All fields are optional. If `metastore_db` and `warehouse_dir` are not set, the TUI uses temporary directories under `/tmp/spark-viewer-tui/` automatically.

Themes are stored in `~/.config/spark-viewer-tui/themes.json`. The file is created automatically on first run with the default themes. Edit it to customize colors or add new themes.

## Project Structure

```
spark-viewer-tui/
├── src/
│   └── spark_viewer_tui/
│       ├── app.py              # Main application
│       ├── client.py           # print_df() client API
│       ├── ipc_server.py       # TCP server for receiving DataFrames
│       ├── examples/
│       │   └── spark_example.py  # spark-viewer-example entry point
│       ├── seed.py             # Seed example Delta tables
│       ├── config.py           # Configuration management
│       ├── spark_manager.py    # Spark session and table registration
│       ├── queries.py          # Query persistence
│       ├── themes.py           # Theme system
│       └── screens/
│           ├── spark_config.py    # Spark config modal (F2)
│           ├── save_query.py      # Save query modal (F3)
│           ├── load_query.py      # Load query modal (F4)
│           └── theme_selector.py  # Theme selector modal (Ctrl+T)
└── pyproject.toml
```

## License

MIT
