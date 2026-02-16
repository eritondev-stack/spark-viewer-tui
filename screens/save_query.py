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


class SaveQueryScreen(ModalScreen[str | None]):
    """Modal to name and save a query."""

    CSS = f"""
    SaveQueryScreen {{
        align: center middle;
        background: transparent;
    }}

    #dialog {{
        grid-size: 2;
        grid-gutter: 1 2;
        grid-rows: auto auto auto;
        padding: 1 2;
        width: 60;
        height: 13;
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

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, current_sql: str):
        super().__init__()
        self._current_sql = current_sql

    def action_cancel(self) -> None:
        self.dismiss(None)

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
