from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Label


class SparkConfigScreen(ModalScreen[dict | None]):
    """Modal screen for Spark configuration."""

    CSS = """
    SparkConfigScreen {
        align: center middle;
    }

    #dialog {
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: auto auto auto auto auto;
        padding: 1 2;
        width: 70;
        height: 21;
        border: thick $background 80%;
        background: $surface;
    }

    #title {
        column-span: 2;
        height: auto;
        width: 1fr;
        content-align: center middle;
        text-style: bold;
    }

    .label {
        column-span: 2;
        height: auto;
        width: 1fr;
    }

    .input {
        column-span: 2;
        height: auto;
        width: 1fr;
    }

    Button {
        width: 100%;
    }
    """

    def __init__(self, current_config: dict):
        super().__init__()
        self._current_config = current_config

    def compose(self) -> ComposeResult:
        yield Grid(
            Label("Spark Configuration", id="title"),
            Label("Metastore DB Path:", classes="label"),
            Input(
                value=self._current_config.get("metastore_db", ""),
                placeholder="/path/to/metastore_db",
                id="input-metastore",
                classes="input",
            ),
            Label("Warehouse Dir Path:", classes="label"),
            Input(
                value=self._current_config.get("warehouse_dir", ""),
                placeholder="/path/to/warehouse",
                id="input-warehouse",
                classes="input",
            ),
            Button("Save", variant="primary", id="btn-save"),
            Button("Cancel", variant="default", id="btn-cancel"),
            id="dialog",
        )

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
