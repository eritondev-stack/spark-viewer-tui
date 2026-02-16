from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Label


COLOR_DEFAULT_TRANSPARENT = {
    "blue": "#4d4dca",
    "aqua": "#45afb3",
    "magenta": "#c848b7",
    "yellow": "#fcff00",
    "lime": "#01f649",
    "neutro": "#676667",
}


class SparkConfigScreen(ModalScreen[dict | None]):
    """Modal screen for Spark configuration."""

    CSS = f"""
    SparkConfigScreen {{
        align: center middle;
        background: transparent;
    }}

    #dialog {{
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: auto auto auto auto auto;
        padding: 1 2;
        width: 70;
        height: 21;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        background: transparent;
        color: #e0e0e0;
    }}

    #title {{
        column-span: 2;
        height: auto;
        width: 1fr;
        content-align: center middle;
        text-style: bold;
        color: {COLOR_DEFAULT_TRANSPARENT['magenta']};
        background: transparent;
    }}

    .label {{
        column-span: 2;
        height: auto;
        width: 1fr;
        background: transparent;
        color: #e0e0e0;
    }}

    .input {{
        column-span: 2;
        height: auto;
        width: 1fr;
    }}

    Input {{
        background: transparent;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        color: #e0e0e0;
    }}

    Input:focus {{
        border: solid {COLOR_DEFAULT_TRANSPARENT['magenta']};
    }}

    Button {{
        width: 100%;
        background: transparent;
        border: none;
        color: #e0e0e0;
        text-style: bold;
        height: 3;
    }}

    Button:hover {{
        background: {COLOR_DEFAULT_TRANSPARENT['neutro']} 30%;
        color: #ffffff;
    }}

    Button:focus {{
        background: {COLOR_DEFAULT_TRANSPARENT['neutro']} 20%;
        color: #ffffff;
    }}

    #btn-save {{
        color: {COLOR_DEFAULT_TRANSPARENT['lime']};
    }}

    #btn-save:hover {{
        background: {COLOR_DEFAULT_TRANSPARENT['lime']} 20%;
    }}

    #btn-cancel {{
        color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
    }}
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
            Button("Save", id="btn-save"),
            Button("Cancel", id="btn-cancel"),
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
