from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Label, OptionList
from textual.widgets.option_list import Option

from themes import MODAL_BASE_CSS, MODAL_THEME_CSS, THEME_NAMES


class SparkConfigScreen(ModalScreen[dict | None]):
    """Modal screen for Spark configuration."""

    CSS = MODAL_BASE_CSS + MODAL_THEME_CSS + """
    SparkConfigScreen {
        align: center middle;
        background: transparent;
    }

    #dialog {
        padding: 1 2;
        width: 70;
        height: 30;
        layout: vertical;
    }

    #title {
        height: auto;
        width: 1fr;
        content-align: center middle;
        text-style: bold;
    }

    .label {
        height: auto;
        width: 1fr;
    }

    Input {
        width: 1fr;
    }

    #path-list {
        height: 6;
        margin: 1 0 0 0;
        border: none;
        scrollbar-background: transparent;
        scrollbar-size: 1 2;
    }

    #path-list:focus {
        border: none;
    }

    OptionList > .option-list--option-hover {
        background: transparent;
    }

    #path-row {
        height: auto;
        width: 1fr;
        margin: 0 0 1 0;
    }

    #input-db-name {
        width: 20;
        min-width: 20;
    }

    #input-path {
        width: 1fr;
    }

    #btn-add-path {
        width: 6;
        min-width: 6;
    }

    #btn-remove-path {
        width: 6;
        min-width: 6;
    }

    #button-row {
        height: auto;
        width: 1fr;
        align: center middle;
    }

    Button {
        border: none;
        text-style: bold;
        height: 3;
    }

    #btn-save, #btn-cancel {
        width: 1fr;
        margin: 0 1;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, current_config: dict, theme_name: str = ""):
        super().__init__()
        self._current_config = current_config
        self._theme_name = theme_name
        # scan_paths is list[dict] with {"path": "...", "db_name": "..."}
        raw = current_config.get("scan_paths", [])
        self._scan_paths: list[dict] = []
        for item in raw:
            if isinstance(item, dict):
                self._scan_paths.append(item)
            elif isinstance(item, str):
                # Migrate old format (plain strings) to new dict format
                self._scan_paths.append({"path": item, "db_name": ""})

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_mount(self) -> None:
        if self._theme_name and THEME_NAMES and self._theme_name != THEME_NAMES[0]:
            css_class = self._theme_name.lower().replace(" ", "-")
            self.add_class(css_class)

    def _format_entry(self, entry: dict) -> str:
        return f"{entry['db_name']} \u2192 {entry['path']}"

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label("Spark Configuration", id="title")
            yield Label("Metastore DB Path:", classes="label")
            yield Input(
                value=self._current_config.get("metastore_db", ""),
                placeholder="/path/to/metastore_db",
                id="input-metastore",
            )
            yield Label("Warehouse Dir Path:", classes="label")
            yield Input(
                value=self._current_config.get("warehouse_dir", ""),
                placeholder="/path/to/warehouse",
                id="input-warehouse",
            )
            yield Label("Scan Paths:", classes="label")
            options = [
                Option(self._format_entry(e), id=str(i))
                for i, e in enumerate(self._scan_paths)
            ]
            yield OptionList(*options, id="path-list")
            with Horizontal(id="path-row"):
                yield Input(placeholder="db name", id="input-db-name")
                yield Input(placeholder="/path/to/scan", id="input-path")
                yield Button("+", id="btn-add-path")
                yield Button("-", id="btn-remove-path")
            with Horizontal(id="button-row"):
                yield Button("Save", id="btn-save")
                yield Button("Cancel", id="btn-cancel")

    def _rebuild_option_list(self) -> None:
        option_list = self.query_one("#path-list", OptionList)
        option_list.clear_options()
        for i, entry in enumerate(self._scan_paths):
            option_list.add_option(Option(self._format_entry(entry), id=str(i)))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-add-path":
            db_input = self.query_one("#input-db-name", Input)
            path_input = self.query_one("#input-path", Input)
            db_name = db_input.value.strip()
            path = path_input.value.strip()
            if not db_name or not path:
                self.notify("Enter both db name and path.", severity="warning")
                return
            # Check for duplicate db_name
            for existing in self._scan_paths:
                if existing["db_name"] == db_name:
                    self.notify(f"Database '{db_name}' already added.", severity="warning")
                    return
            self._scan_paths.append({"path": path, "db_name": db_name})
            self._rebuild_option_list()
            db_input.value = ""
            path_input.value = ""
        elif event.button.id == "btn-remove-path":
            option_list = self.query_one("#path-list", OptionList)
            idx = option_list.highlighted
            if idx is None:
                self.notify("Select a path first.", severity="warning")
                return
            self._scan_paths.pop(idx)
            self._rebuild_option_list()
        elif event.button.id == "btn-save":
            metastore = self.query_one("#input-metastore", Input).value
            warehouse = self.query_one("#input-warehouse", Input).value
            self.dismiss({
                "metastore_db": metastore,
                "warehouse_dir": warehouse,
                "scan_paths": self._scan_paths,
            })
        else:
            self.dismiss(None)
