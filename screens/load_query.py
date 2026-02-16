from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.screen import ModalScreen
from textual.widgets import OptionList, Button, Label
from textual.widgets.option_list import Option

from queries import delete_query


COLOR_DEFAULT_TRANSPARENT = {
    "blue": "#4d4dca",
    "aqua": "#45afb3",
    "magenta": "#c848b7",
    "yellow": "#fcff00",
    "lime": "#01f649",
    "neutro": "#676667",
}


def _make_option(name: str, sql: str, max_width: int = 60) -> Option:
    """Build an OptionList item: name bold + query dim on same line."""
    t = Text()
    t.append(name, style="bold")
    t.append("  ")
    preview = sql.replace("\n", " ").strip()
    if len(preview) > max_width:
        preview = preview[:max_width] + "..."
    t.append(preview, style="dim italic")
    return Option(t, id=name)


class LoadQueryScreen(ModalScreen[str | None]):
    """Modal to select and load a saved query."""

    CSS = f"""
    LoadQueryScreen {{
        align: center middle;
        background: transparent;
    }}

    #dialog {{
        padding: 1 2;
        width: 90;
        height: 22;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        background: transparent;
        color: #e0e0e0;
        layout: vertical;
    }}

    #title {{
        height: auto;
        width: 1fr;
        content-align: center middle;
        text-style: bold;
        color: {COLOR_DEFAULT_TRANSPARENT['magenta']};
        background: transparent;
    }}

    OptionList {{
        height: 1fr;
        background: transparent;
        border: solid {COLOR_DEFAULT_TRANSPARENT['neutro']};
        color: #e0e0e0;
        scrollbar-background: transparent;
        scrollbar-color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
        scrollbar-size: 1 2;
        margin: 1 0;
    }}

    OptionList > .option-list--option-highlighted {{
        background: {COLOR_DEFAULT_TRANSPARENT['blue']};
    }}

    OptionList > .option-list--option-hover {{
        background: transparent;
    }}

    #button-row {{
        height: auto;
        width: 1fr;
        align: center middle;
    }}

    Button {{
        width: 1fr;
        background: transparent;
        border: none;
        color: #e0e0e0;
        text-style: bold;
        height: 3;
        margin: 0 1;
    }}

    Button:hover {{
        background: {COLOR_DEFAULT_TRANSPARENT['neutro']} 30%;
        color: #ffffff;
    }}

    Button:focus {{
        background: {COLOR_DEFAULT_TRANSPARENT['neutro']} 20%;
        color: #ffffff;
    }}

    #btn-load {{
        color: {COLOR_DEFAULT_TRANSPARENT['lime']};
    }}

    #btn-load:hover {{
        background: {COLOR_DEFAULT_TRANSPARENT['lime']} 20%;
    }}

    #btn-delete {{
        color: {COLOR_DEFAULT_TRANSPARENT['yellow']};
    }}

    #btn-delete:hover {{
        background: {COLOR_DEFAULT_TRANSPARENT['yellow']} 20%;
    }}

    #btn-cancel {{
        color: {COLOR_DEFAULT_TRANSPARENT['neutro']};
    }}
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, queries: dict[str, str]):
        super().__init__()
        self._queries = dict(queries)
        self._names = list(queries.keys())

    def action_cancel(self) -> None:
        self.dismiss(None)

    def compose(self) -> ComposeResult:
        yield Label("Load Query", id="title")
        options = [_make_option(n, self._queries[n]) for n in self._names]
        yield OptionList(*options, id="query-list")
        with Horizontal(id="button-row"):
            yield Button("Load", id="btn-load")
            yield Button("Delete", id="btn-delete")
            yield Button("Cancel", id="btn-cancel")

    def _get_selected_name(self) -> str | None:
        option_list = self.query_one("#query-list", OptionList)
        idx = option_list.highlighted
        if idx is None:
            return None
        return option_list.get_option_at_index(idx).id

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-load":
            name = self._get_selected_name()
            if name is None:
                self.notify("Select a query first.", severity="warning")
                return
            self.dismiss(self._queries[name])
        elif event.button.id == "btn-delete":
            name = self._get_selected_name()
            if name is None:
                self.notify("Select a query first.", severity="warning")
                return
            del self._queries[name]
            self._names.remove(name)
            delete_query(name)
            option_list = self.query_one("#query-list", OptionList)
            option_list.remove_option(name)
            self.notify(f"Deleted: {name}")
            if not self._queries:
                self.dismiss(None)
        else:
            self.dismiss(None)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        name = event.option.id
        if name and name in self._queries:
            self.dismiss(self._queries[name])
