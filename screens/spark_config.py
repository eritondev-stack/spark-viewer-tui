from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Label

from themes import MODAL_BASE_CSS, MODAL_THEME_CSS, THEME_NAMES


class SparkConfigScreen(ModalScreen[dict | None]):
    """Modal screen for Spark configuration."""

    CSS = """
    SparkConfigScreen {
        align: center middle;
        background: transparent;
    }

    #dialog {
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: auto auto auto auto auto;
        padding: 1 2;
        width: 70;
        height: 21;
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
        border: none;
        text-style: bold;
        height: 3;
    }
    """ + MODAL_BASE_CSS + MODAL_THEME_CSS

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, current_config: dict, theme_name: str = ""):
        super().__init__()
        self._current_config = current_config
        self._theme_name = theme_name

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_mount(self) -> None:
        if self._theme_name and THEME_NAMES and self._theme_name != THEME_NAMES[0]:
            css_class = self._theme_name.lower().replace(" ", "-")
            self.add_class(css_class)

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
