import os
from rich.console import COLOR_SYSTEMS
from rich.text import Text
from textual import work, events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.widgets import TextArea, DataTable, Static, Tree

from config import load_config, save_config
from spark_manager import SparkManager
from screens.spark_config import SparkConfigScreen

# Configure JAVA_HOME for PySpark
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

# Theme definitions
THEME_NAMES = ["Transparent", "Dracula", "Solid Dark", "Gruvbox"]

THEME_CSS_CLASS = {
    "Transparent": None,
    "Dracula": "dracula",
    "Solid Dark": "solid-dark",
    "Gruvbox": "gruvbox",
}

THEME_COLORS = {
    "Transparent": {"accent": "#c026d3", "text": "#e0e0e0"},
    "Dracula": {"accent": "#bd93f9", "text": "#f8f8f2"},
    "Solid Dark": {"accent": "#c026d3", "text": "#ffffff"},
    "Gruvbox": {"accent": "#d79921", "text": "#ebdbb2"},
}

ALL_THEME_CLASSES = {"dracula", "solid-dark", "gruvbox"}

# Bindings shown in the status bar
APP_BINDINGS = [
    ("F2", "Spark Config"),
    ("^S", "Start Spark"),
    ("^Enter", "Run SQL"),
    ("^T", "Theme"),
]

COLOR_DEFAULT_TRANSPARENT = {
    "blue": "#4d4dca",
    "aqua": "#45afb3",
    "magenta": "#c848b7",
    "yellow": "#fcff00",
    "lime": "#01f649",
    "neutro": "#676667"
}


class StatusBar(Static):
    """Custom footer bar — fully transparent, no Textual Footer limitations."""

    def __init__(self) -> None:
        super().__init__(id="status-bar")
        self._accent = "#c026d3"
        self._text_color = "#e0e0e0"
        self._extra_bindings: list[tuple[str, str]] = []

    def render(self) -> Text:
        t = Text()
        for key, desc in APP_BINDINGS:
            t.append(f" {key} ", style=f"bold {COLOR_DEFAULT_TRANSPARENT['lime']}")
            t.append(f"{desc} ", style=self._text_color)
        for key, desc in self._extra_bindings:
            t.append(f" {key} ", style=f"bold {COLOR_DEFAULT_TRANSPARENT['lime']}")
            t.append(f"{desc} ", style=self._text_color)
        return t

    def set_extra_bindings(self, bindings: list[tuple[str, str]]) -> None:
        self._extra_bindings = bindings
        self.refresh()

    def set_colors(self, accent: str, text_color: str) -> None:
        self._accent = accent
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
        ("ctrl+s", "start_spark", "Start Spark"),
        Binding("ctrl+enter", "execute_query", "Run SQL", priority=True),
        ("ctrl+t", "cycle_theme", "Change Theme"),
        Binding("ctrl+w", "toggle_maximize", "Maximize", priority=True),
    ]

    CSS = fr"""
    /* ══════════ Base / Transparent Theme ══════════ */
    Screen {{
        layout: horizontal;
        background: transparent;
        color: #e0e0e0;
    }}

    Sidebar {{
        width: 30;
        height: 1fr;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        padding: 1;
        background: transparent;
        color: #e0e0e0;
    }}

    Sidebar > Static {{
        background: transparent;
    }}

    #title {{
        text-style: bold;
        color: #c026d3;
        text-align: center;
        background: transparent;
    }}

    #separator {{
        color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
        background: transparent;
    }}

    #db-tree {{
        background: transparent;
        color: #e0e0e0;
        padding: 0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: magenta;
        scrollbar-color-hover: magenta 80%;
        scrollbar-color-active: magenta 60%;
        scrollbar-size: 1 2;
    }}

    Tree > .tree--cursor {{
        background: {COLOR_DEFAULT_TRANSPARENT['blue']};
    }}

    Tree > .tree--guides {{
        color: {COLOR_DEFAULT_TRANSPARENT['blue']};
    }}

    Tree > .tree--guides-hover {{
        color: {COLOR_DEFAULT_TRANSPARENT['blue']};
    }}

    #main-container {{
        width: 1fr;
        height: 1fr;
        padding: 0;
        background: transparent;
    }}

    Vertical {{
        background: transparent;
    }}

    Container {{
        background: transparent;
    }}

    TextArea {{
        height: 10;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        padding: 0;
        background: transparent;
        color: #e0e0e0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
        scrollbar-color-hover: {COLOR_DEFAULT_TRANSPARENT['neutro']} 80%;
        scrollbar-color-active: {COLOR_DEFAULT_TRANSPARENT['neutro']} 60%;
        scrollbar-size: 1 2;
    }}

    TextArea > .text-area--cursor-line {{
        background: transparent;
    }}

    DataTable {{
        height: 1fr;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        background: transparent;
        color: #e0e0e0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
        scrollbar-color-hover: {COLOR_DEFAULT_TRANSPARENT['neutro']} 80%;
        scrollbar-color-active: {COLOR_DEFAULT_TRANSPARENT['neutro']} 60%;
        scrollbar-size: 1 2;
        scrollbar-corner-color: transparent;
    }}

    DataTable > .datatable--header {{
        background: transparent;
    }}

    DataTable > .datatable--header-hover {{
        background: transparent;
    }}

    DataTable > .datatable--hover {{
        background: transparent;
    }}

    DataTable > .datatable--cursor {{
        background: {COLOR_DEFAULT_TRANSPARENT['blue']};
    }}

    Static {{
        background: transparent;
    }}

    LoadingIndicator {{
        background: transparent;
    }}

    #status-bar {{
        dock: bottom;
        height: 1;
        width: 1fr;
        background: transparent;
    }}

    ToastRack {{
        dock: top;
        align-horizontal: right;
    }}

    /* ══════════ Dracula Theme ══════════ */
    Screen.dracula {{
        background: #282a36;
        color: #f8f8f2;
    }}
    Screen.dracula Sidebar {{
        background: #282a36;
        border: solid #bd93f9;
        color: #f8f8f2;
    }}
    Screen.dracula Sidebar > Static {{
        background: #282a36;
    }}
    Screen.dracula #title {{
        color: #bd93f9;
        background: #282a36;
    }}
    Screen.dracula #separator {{
        color: #bd93f9;
        background: #282a36;
    }}
    Screen.dracula #db-tree {{
        background: #282a36;
        color: #f8f8f2;
    }}
    Screen.dracula #main-container {{
        background: #282a36;
    }}
    Screen.dracula Vertical {{
        background: #282a36;
    }}
    Screen.dracula Container {{
        background: #282a36;
    }}
    Screen.dracula TextArea {{
        background: #282a36;
        border: solid #bd93f9;
        color: #f8f8f2;
    }}
    Screen.dracula DataTable {{
        background: #282a36;
        border: solid #bd93f9;
        color: #f8f8f2;
    }}
    Screen.dracula Static {{
        background: #282a36;
    }}
    Screen.dracula #status-bar {{
        background: #282a36;
    }}

    /* ══════════ Solid Dark Theme ══════════ */
    Screen.solid-dark {{
        background: #0a0a0a;
        color: #ffffff;
    }}
    Screen.solid-dark Sidebar {{
        background: #0a0a0a;
        border: solid #c026d3;
        color: #ffffff;
    }}
    Screen.solid-dark Sidebar > Static {{
        background: #0a0a0a;
    }}
    Screen.solid-dark #title {{
        color: #c026d3;
        background: #0a0a0a;
    }}
    Screen.solid-dark #separator {{
        color: #c026d3;
        background: #0a0a0a;
    }}
    Screen.solid-dark #db-tree {{
        background: #0a0a0a;
        color: #ffffff;
    }}
    Screen.solid-dark #main-container {{
        background: #0a0a0a;
    }}
    Screen.solid-dark Vertical {{
        background: #0a0a0a;
    }}
    Screen.solid-dark Container {{
        background: #0a0a0a;
    }}
    Screen.solid-dark TextArea {{
        background: #0a0a0a;
        border: solid #c026d3;
        color: #ffffff;
    }}
    Screen.solid-dark DataTable {{
        background: #0a0a0a;
        border: solid #c026d3;
        color: #ffffff;
    }}
    Screen.solid-dark Static {{
        background: #0a0a0a;
    }}
    Screen.solid-dark #status-bar {{
        background: #0a0a0a;
    }}

    /* ══════════ Gruvbox Theme ══════════ */
    Screen.gruvbox {{
        background: #282828;
        color: #ebdbb2;
    }}
    Screen.gruvbox Sidebar {{
        background: #282828;
        border: solid #d79921;
        color: #ebdbb2;
    }}
    Screen.gruvbox Sidebar > Static {{
        background: #282828;
    }}
    Screen.gruvbox #title {{
        color: #d79921;
        background: #282828;
    }}
    Screen.gruvbox #separator {{
        color: #d79921;
        background: #282828;
    }}
    Screen.gruvbox #db-tree {{
        background: #282828;
        color: #ebdbb2;
    }}
    Screen.gruvbox #main-container {{
        background: #282828;
    }}
    Screen.gruvbox Vertical {{
        background: #282828;
    }}
    Screen.gruvbox Container {{
        background: #282828;
    }}
    Screen.gruvbox TextArea {{
        background: #282828;
        border: solid #d79921;
        color: #ebdbb2;
    }}
    Screen.gruvbox DataTable {{
        background: #282828;
        border: solid #d79921;
        color: #ebdbb2;
    }}
    Screen.gruvbox Static {{
        background: #282828;
    }}
    Screen.gruvbox #status-bar {{
        background: #282828;
    }}

    /* ══════════ Focus Highlight (after themes to win specificity) ══════════ */
    Screen Sidebar:focus-within {{
        border: solid {COLOR_DEFAULT_TRANSPARENT['magenta']};
    }}
    Screen TextArea:focus {{
        border: solid {COLOR_DEFAULT_TRANSPARENT['magenta']};
    }}
    Screen DataTable:focus {{
        border: solid {COLOR_DEFAULT_TRANSPARENT['magenta']};
    }}
    """

    def __init__(self):
        super().__init__(ansi_color=True)
        self._config = load_config()
        self._spark = SparkManager()
        self._current_theme = "Dracula"
        self._maximized_widget = None

    def compose(self) -> ComposeResult:
        yield Sidebar()
        with Container(id="main-container"):
            with Vertical():
                yield TextArea(placeholder="Digite sua query SQL aqui...", id="input_text")
                yield DataTable(id="data_table", cursor_type="cell", header_height=3)
        yield StatusBar()

    def on_mount(self) -> None:
        table = self.query_one("#data_table", DataTable)
        table.add_column(self._make_header("", "No data loaded"), width=30)

        # Apply Dracula as default theme
        self._apply_theme("Dracula")

        if self._config.get("metastore_db") and self._config.get("warehouse_dir"):
            self.notify("Config loaded. Ctrl+S to start Spark.")
        else:
            self.notify("F2 to configure Spark paths.")

    def _make_header(self, col_type: str, col_name: str) -> Text:
        t = Text()
        t.append(col_name, style="bold")
        if col_type:
            t.append("\n")
            t.append(col_type, style=f"dim italic {COLOR_DEFAULT_TRANSPARENT['yellow']}")
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
        for cls in ALL_THEME_CLASSES:
            self.screen.remove_class(cls)

        css_class = THEME_CSS_CLASS.get(theme_name)
        if css_class:
            self.screen.add_class(css_class)

        colors = THEME_COLORS[theme_name]
        self.query_one("#status-bar", StatusBar).set_colors(
            colors["accent"], colors["text"]
        )

        self._current_theme = theme_name

    def action_cycle_theme(self) -> None:
        """Cycle through available themes"""
        try:
            idx = THEME_NAMES.index(self._current_theme)
        except ValueError:
            idx = 0
        next_idx = (idx + 1) % len(THEME_NAMES)
        next_name = THEME_NAMES[next_idx]
        self._apply_theme(next_name)
        self.notify(f"Theme: {next_name}")

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

    # ── Config popup ─────────────────────────────────────────

    def action_open_config(self) -> None:
        """Open Spark configuration modal"""
        try:
            screen = SparkConfigScreen(self._config)
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
            self.notify("Spark session already active.", severity="warning")
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
        self.notify(f"Query returned {len(rows)} rows.")

    def _on_query_error(self, error: str) -> None:
        table = self.query_one("#data_table", DataTable)
        table.loading = False
        self.notify(f"Query error: {error}", severity="error")


if __name__ == "__main__":
    app = TextualApp()
    app.run()
