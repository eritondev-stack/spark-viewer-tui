import os
from rich.text import Text
from textual import work, events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.widgets import TextArea, DataTable, Static, Tree

from config import load_config, save_config
from queries import load_queries, add_query
from spark_manager import SparkManager
from themes import THEME_NAMES, THEME_COLORS, BASE_THEME_CSS, THEME_CSS
from screens.spark_config import SparkConfigScreen
from screens.save_query import SaveQueryScreen
from screens.load_query import LoadQueryScreen
from screens.theme_selector import ThemeSelectorScreen

# Configure JAVA_HOME for PySpark
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Bindings shown in the status bar
APP_BINDINGS = [
    ("F2", "Spark Config"),
    ("F3", "Save Query"),
    ("F4", "Load Query"),
    ("^S", "Start Spark"),
    ("^E", "Run SQL"),
    ("^T", "Theme"),
]

class StatusBar(Static):
    """Custom footer bar — fully transparent, no Textual Footer limitations."""

    def __init__(self) -> None:
        super().__init__(id="status-bar")
        self._key_color = "#01f649"
        self._text_color = "#e0e0e0"
        self._extra_bindings: list[tuple[str, str]] = []

    def render(self) -> Text:
        t = Text()
        for key, desc in APP_BINDINGS:
            t.append(f" {key} ", style=f"bold {self._key_color}")
            t.append(f"{desc} ", style=self._text_color)
        for key, desc in self._extra_bindings:
            t.append(f" {key} ", style=f"bold {self._key_color}")
            t.append(f"{desc} ", style=self._text_color)
        return t

    def set_extra_bindings(self, bindings: list[tuple[str, str]]) -> None:
        self._extra_bindings = bindings
        self.refresh()

    def set_colors(self, key_color: str, text_color: str) -> None:
        self._key_color = key_color
        self._text_color = text_color
        self.refresh()


class Sidebar(Static):
    """Barra lateral da aplicacao"""

    def compose(self) -> ComposeResult:
        tree: Tree[str] = Tree("Databases", id="db-tree")
        tree.root.expand()
        yield tree


class TextualApp(App):
    """Aplicacao principal Textual"""

    ENABLE_COMMAND_PALETTE = False

    BINDINGS = [
        ("f2", "open_config", "Spark Config"),
        ("f3", "save_query", "Save Query"),
        ("f4", "load_query", "Load Query"),
        ("ctrl+s", "start_spark", "Start Spark"),
        Binding("ctrl+e", "execute_query", "Run SQL", priority=True),
        ("ctrl+t", "cycle_theme", "Change Theme"),
        Binding("ctrl+w", "toggle_maximize", "Maximize", priority=True),
    ]

    CSS = """
    /* ══════════ Base / Transparent Theme ══════════ */
    Screen {
        layout: horizontal;
        background: transparent;
        color: #e0e0e0;
    }

    Sidebar {
        width: 30;
        height: 1fr;
        padding: 1;
        background: transparent;
        color: #e0e0e0;
        border-title-align: left;
    }

    Sidebar > Static {
        background: transparent;
    }

    #title {
        text-style: bold;
        text-align: center;
        background: transparent;
    }

    #separator {
        background: transparent;
    }

    #db-tree {
        background: transparent;
        color: #e0e0e0;
        padding: 0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-size: 1 2;
    }

    #main-container {
        width: 1fr;
        height: 1fr;
        padding: 0;
        background: transparent;
    }

    Vertical {
        background: transparent;
    }

    Container {
        background: transparent;
    }

    TextArea {
        height: 10;
        padding: 0;
        background: transparent;
        color: #e0e0e0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-size: 1 2;
        border-title-align: left;
    }

    TextArea > .text-area--cursor-line {
        background: transparent;
    }

    DataTable {
        height: 1fr;
        background: transparent;
        color: #e0e0e0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-size: 1 2;
        scrollbar-corner-color: transparent;
        border-title-align: left;
    }

    DataTable > .datatable--header {
        background: transparent;
    }

    DataTable > .datatable--header-hover {
        background: transparent;
    }

    DataTable > .datatable--hover {
        background: transparent;
    }

    DataTable > .datatable--cursor {
        background: transparent;
    }

    Static {
        background: transparent;
    }

    LoadingIndicator {
        background: transparent;
    }

    #status-bar {
        dock: bottom;
        height: 1;
        width: 1fr;
        background: transparent;
    }

    ToastRack {
        dock: top;
        align-horizontal: right;
    }
    """ + BASE_THEME_CSS + THEME_CSS

    def __init__(self):
        super().__init__(ansi_color=True)
        self._config = load_config()
        self._spark = SparkManager()
        self._theme_names = THEME_NAMES
        self._theme_colors = THEME_COLORS
        self._current_theme = self._theme_names[0]
        self._maximized_widget = None

    def compose(self) -> ComposeResult:
        yield Sidebar()
        with Container(id="main-container"):
            with Vertical():
                yield TextArea(language="sql", placeholder="Digite sua query SQL aqui...", id="input_text")
                yield DataTable(id="data_table", cursor_type="cell", header_height=3)
        yield StatusBar()

    def on_mount(self) -> None:
        self.query_one("Sidebar").border_title = "Catalog"
        self.query_one("#input_text", TextArea).border_title = "Run Query"

        table = self.query_one("#data_table", DataTable)
        table.border_title = "Results"
        table.add_column(self._make_header("", "No data loaded"), width=30)

        # Apply first theme as default
        self._apply_theme(self._theme_names[0])

        if self._config.get("metastore_db") and self._config.get("warehouse_dir"):
            self.notify("Config loaded. Ctrl+S to start Spark.")
        else:
            self.notify("F2 to configure Spark paths.")

    def _make_header(self, col_type: str, col_name: str) -> Text:
        colors = self._theme_colors[self._current_theme]
        t = Text()
        t.append(col_name, style="bold")
        if col_type:
            t.append("\n")
            t.append(col_type, style=f"dim italic {colors['col_type_color']}")
        t.append("\n")
        t.append("─" * 16, style="dim")
        return t

    # ── Focus tracking ───────────────────────────────────────

    def on_descendant_focus(self, event: events.DescendantFocus) -> None:
        self._update_focus_bindings()

    def on_descendant_blur(self, event: events.DescendantBlur) -> None:
        self._update_focus_bindings()

    def _update_focus_bindings(self) -> None:
        focused = self.screen.focused
        status = self.query_one("#status-bar", StatusBar)
        if isinstance(focused, (TextArea, DataTable)):
            status.set_extra_bindings([("^W", "Maximize")])
        else:
            status.set_extra_bindings([])

    # ── Theme cycling ────────────────────────────────────────

    def _apply_theme(self, theme_name: str) -> None:
        """Apply theme via CSS classes + status bar colors."""
        # Remove all theme classes
        for name in self._theme_names:
            css_class = name.lower().replace(" ", "-")
            self.screen.remove_class(css_class)

        # Add new theme class (first theme uses base CSS, no class needed)
        if theme_name != self._theme_names[0]:
            css_class = theme_name.lower().replace(" ", "-")
            self.screen.add_class(css_class)

        colors = self._theme_colors[theme_name]
        self.query_one("#status-bar", StatusBar).set_colors(
            colors["key_color"], colors["text"]
        )

        self._current_theme = theme_name

    def action_cycle_theme(self) -> None:
        """Open theme selector modal."""
        self.push_screen(
            ThemeSelectorScreen(self._current_theme),
            self._on_theme_selected,
        )

    def _on_theme_selected(self, theme_name: str | None) -> None:
        if theme_name is None or theme_name == self._current_theme:
            return
        self._apply_theme(theme_name)
        self.notify(f"Theme: {theme_name}")

    # ── Maximize toggle ──────────────────────────────────────

    def action_toggle_maximize(self) -> None:
        """Toggle maximize for focused TextArea or DataTable."""
        sidebar = self.query_one("Sidebar")
        text_area = self.query_one("#input_text", TextArea)
        data_table = self.query_one("#data_table", DataTable)

        if self._maximized_widget:
            # Restore all
            sidebar.display = True
            text_area.display = True
            data_table.display = True
            text_area.styles.height = 10
            text_area.styles.border = None
            data_table.styles.border = None
            self._maximized_widget = None
            return

        focused = self.screen.focused
        if isinstance(focused, TextArea):
            sidebar.display = False
            data_table.display = False
            text_area.styles.height = "1fr"
            text_area.styles.border = ("none", "transparent")
            self._maximized_widget = focused
        elif isinstance(focused, DataTable):
            sidebar.display = False
            text_area.display = False
            data_table.styles.border = ("none", "transparent")
            self._maximized_widget = focused

    # ── Query save / load ────────────────────────────────────

    def action_save_query(self) -> None:
        text_area = self.query_one("#input_text", TextArea)
        sql = text_area.text.strip()
        if not sql:
            self.notify("Write a query first.", severity="warning")
            return
        self.push_screen(SaveQueryScreen(sql, self._current_theme), self._on_query_saved)

    def _on_query_saved(self, name: str | None) -> None:
        if name is None:
            return
        sql = self.query_one("#input_text", TextArea).text.strip()
        add_query(name, sql)
        self.notify(f"Query saved: {name}")

    def action_load_query(self) -> None:
        queries = load_queries()
        if not queries:
            self.notify("No saved queries.", severity="warning")
            return
        self.push_screen(LoadQueryScreen(queries, self._current_theme), self._on_query_loaded)

    def _on_query_loaded(self, sql: str | None) -> None:
        if sql is None:
            return
        text_area = self.query_one("#input_text", TextArea)
        text_area.load_text(sql)
        self.notify("Query loaded.")

    # ── Config popup ─────────────────────────────────────────

    def action_open_config(self) -> None:
        """Open Spark configuration modal"""
        try:
            screen = SparkConfigScreen(self._config, self._current_theme)
            self.push_screen(screen, self._on_config_saved)
        except Exception as e:
            self.notify(f"Error opening config: {e}", severity="error")

    def _on_config_saved(self, result: dict | None) -> None:
        if result is not None:
            self._config = result
            save_config(result)
            self.notify("Configuration saved.")
        else:
            self.notify("Configuration cancelled.")

    # ── Spark session ────────────────────────────────────────

    def action_start_spark(self) -> None:
        if self._spark.is_active:
            # Spark already running — rescan paths and refresh tree
            scan_paths = self._config.get("scan_paths", [])
            if not scan_paths:
                self.notify("No scan paths configured (F2).", severity="warning")
                return
            self.notify("Rescanning paths...")
            self.query_one("Sidebar").loading = True
            self._rescan_worker()
            return
        metastore = self._config.get("metastore_db", "")
        warehouse = self._config.get("warehouse_dir", "")
        if not metastore or not warehouse:
            self.notify("Configure Spark paths first (F2).", severity="error")
            return
        self.notify("Starting Spark session...")
        self.query_one("Sidebar").loading = True
        self._start_spark_worker(metastore, warehouse)

    @work(thread=True, exclusive=True)
    def _start_spark_worker(self, metastore: str, warehouse: str) -> None:
        try:
            self._spark.start_session(metastore, warehouse)
            # Scan configured paths for Delta/Parquet tables
            scan_paths = self._config.get("scan_paths", [])
            if scan_paths:
                self._spark.scan_and_register_paths(scan_paths)
            databases = self._spark.list_databases()
            catalog_data = {}
            for db in databases:
                tables = self._spark.list_tables(db)
                catalog_data[db] = tables
            self.app.call_from_thread(self._on_spark_ready, catalog_data)
        except Exception as e:
            import traceback
            error_msg = f"Spark error: {str(e)}"
            self.app.call_from_thread(self._on_spark_error, error_msg)
            print(f"ERROR in Spark worker:\n{traceback.format_exc()}")

    @work(thread=True, exclusive=True)
    def _rescan_worker(self) -> None:
        try:
            scan_paths = self._config.get("scan_paths", [])
            self._spark.scan_and_register_paths(scan_paths)
            databases = self._spark.list_databases()
            catalog_data = {}
            for db in databases:
                tables = self._spark.list_tables(db)
                catalog_data[db] = tables
            self.app.call_from_thread(self._on_spark_ready, catalog_data)
        except Exception as e:
            import traceback
            error_msg = f"Rescan error: {str(e)}"
            self.app.call_from_thread(self._on_spark_error, error_msg)
            print(f"ERROR in rescan worker:\n{traceback.format_exc()}")

    def _on_spark_error(self, error_msg: str) -> None:
        self.query_one("Sidebar").loading = False
        self.notify(error_msg, severity="error")

    def _on_spark_ready(self, catalog_data: dict[str, list[str]]) -> None:
        self.query_one("Sidebar").loading = False
        self.notify("Spark session started!")
        self._populate_tree(catalog_data)

    def _populate_tree(self, catalog_data: dict[str, list[str]]) -> None:
        tree = self.query_one("#db-tree", Tree)
        tree.clear()
        for db_name, tables in catalog_data.items():
            db_node = tree.root.add(db_name, expand=True)
            for table_name in tables:
                db_node.add_leaf(table_name)

    # ── Tree click -> auto query ─────────────────────────────

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if node.is_root or node.children:
            return
        if not self._spark.is_active:
            self.notify("Start Spark session first (Ctrl+S).", severity="error")
            return
        table_name = str(node.label)
        db_name = str(node.parent.label)
        query = f"SELECT * FROM {db_name}.{table_name} LIMIT 1000"
        text_area = self.query_one("#input_text", TextArea)
        text_area.load_text(query)
        self._run_query(query)

    # ── SQL execution ────────────────────────────────────────

    def action_execute_query(self) -> None:
        if not self._spark.is_active:
            self.notify("Start Spark session first (Ctrl+S).", severity="error")
            return
        text_area = self.query_one("#input_text", TextArea)
        query = text_area.text.strip()
        if not query:
            self.notify("Enter a SQL query first.", severity="warning")
            return
        self._run_query(query)

    def _run_query(self, query: str) -> None:
        table = self.query_one("#data_table", DataTable)
        table.loading = True
        self._execute_query_worker(query)

    @work(thread=True, exclusive=True)
    def _execute_query_worker(self, query: str) -> None:
        try:
            schema, rows = self._spark.execute_sql(query)
            self.app.call_from_thread(self._on_query_results, schema, rows)
        except Exception as e:
            self.app.call_from_thread(self._on_query_error, str(e))

    def _on_query_results(self, schema: list[tuple[str, str]], rows: list[list[str]]) -> None:
        table = self.query_one("#data_table", DataTable)
        table.clear(columns=True)
        for col_name, col_type in schema:
            table.add_column(self._make_header(col_type, col_name), width=16)
        for row in rows:
            table.add_row(*row)
        table.loading = False
        table.border_title = f"Results (total: {len(rows)})"

    def _on_query_error(self, error: str) -> None:
        table = self.query_one("#data_table", DataTable)
        table.loading = False
        self.notify(f"Query error: {error}", severity="error")


def main():
    app = TextualApp()
    app.run()


if __name__ == "__main__":
    main()
