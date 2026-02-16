from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Input, Button, Label

from themes import MODAL_BASE_CSS, MODAL_THEME_CSS, THEME_NAMES


class SaveQueryScreen(ModalScreen[str | None]):
    """Modal to name and save a query."""

    CSS = """
    SaveQueryScreen {
        align: center middle;
        background: transparent;
    }

    #dialog {
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: auto auto auto;
        padding: 1 2;
        width: 60;
        height: 13;
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

    def __init__(self, current_sql: str, theme_name: str = ""):
        super().__init__()
        self._current_sql = current_sql
        self._theme_name = theme_name

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_mount(self) -> None:
        if self._theme_name and THEME_NAMES and self._theme_name != THEME_NAMES[0]:
            css_class = self._theme_name.lower().replace(" ", "-")
            self.add_class(css_class)

    def compose(self) -> ComposeResult:
        yield Grid(
            Label("Save Query", id="title"),
            Label("Query name:", classes="label"),
            Input(
                placeholder="e.g. employees by department",
                id="input-name",
                classes="input",
            ),
            Button("Save", id="btn-save"),
            Button("Cancel", id="btn-cancel"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-save":
            name = self.query_one("#input-name", Input).value.strip()
            if not name:
                self.notify("Enter a name for the query.", severity="error")
                return
            self.dismiss(name)
        else:
            self.dismiss(None)
