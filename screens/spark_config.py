from textual.app import ComposeResult
from textual.containers import Vertical, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Static, Label


class SparkConfigScreen(ModalScreen[dict | None]):

    CSS = """
    SparkConfigScreen {
        align: center middle;
    }

    #config-dialog {
        width: 70;
        height: auto;
        border: solid $primary;
        background: $surface;
        padding: 1 2;
    }

    #config-title {
        text-style: bold;
        text-align: center;
        width: 100%;
        margin-bottom: 1;
    }

    .config-label {
        margin-top: 1;
    }

    #config-buttons {
        margin-top: 1;
        align: center middle;
    }

    #config-buttons Button {
        margin: 0 1;
    }
    """

    def __init__(self, current_config: dict):
        super().__init__()
        self._current_config = current_config

    def compose(self) -> ComposeResult:
        with Vertical(id="config-dialog"):
            yield Static("Spark Configuration", id="config-title")
            yield Label("Metastore DB Path:")
            yield Input(
                value=self._current_config.get("metastore_db", ""),
                placeholder="/path/to/metastore_db",
                id="input-metastore",
            )
            yield Label("Warehouse Dir Path:")
            yield Input(
                value=self._current_config.get("warehouse_dir", ""),
                placeholder="/path/to/warehouse",
                id="input-warehouse",
            )
            with Horizontal(id="config-buttons"):
                yield Button("Save", variant="primary", id="btn-save")
                yield Button("Cancel", variant="default", id="btn-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-save":
            metastore = self.query_one("#input-metastore", Input).value
            warehouse = self.query_one("#input-warehouse", Input).value
            self.dismiss({
                "metastore_db": metastore,
                "warehouse_dir": warehouse,
            })
        else:
            self.dismiss(None)
