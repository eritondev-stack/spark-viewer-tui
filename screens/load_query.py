from textual.app import ComposeResult
from textual.containers import Grid, Horizontal
from textual.screen import ModalScreen
from textual.widgets import OptionList, Button, Label

from queries import delete_query


COLOR_DEFAULT_TRANSPARENT = {
    "blue": "#4d4dca",
    "aqua": "#45afb3",
    "magenta": "#c848b7",
    "yellow": "#fcff00",
    "lime": "#01f649",
    "neutro": "#676667",
}


class LoadQueryScreen(ModalScreen[str | None]):
    """Modal to select and load a saved query."""

    CSS = f"""
    LoadQueryScreen {{
        align: center middle;
        background: transparent;
    }}

    #dialog {{
        padding: 1 2;
        width: 60;
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

    def __init__(self, queries: dict[str, str]):
        super().__init__()
        self._queries = dict(queries)

    def compose(self) -> ComposeResult:
        yield Label("Load Query", id="title")
        option_list = OptionList(*self._queries.keys(), id="query-list")
        yield option_list
        with Horizontal(id="button-row"):
            yield Button("Load", id="btn-load")
            yield Button("Delete", id="btn-delete")
            yield Button("Cancel", id="btn-cancel")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-load":
            option_list = self.query_one("#query-list", OptionList)
            idx = option_list.highlighted
            if idx is None:
                self.notify("Select a query first.", severity="warning")
                return
            name = str(option_list.get_option_at_index(idx).prompt)
            self.dismiss(self._queries[name])
        elif event.button.id == "btn-delete":
            option_list = self.query_one("#query-list", OptionList)
            idx = option_list.highlighted
            if idx is None:
                self.notify("Select a query first.", severity="warning")
                return
            option = option_list.get_option_at_index(idx)
            name = str(option.prompt)
            del self._queries[name]
            delete_query(name)
            option_list.remove_option(option.id)
            self.notify(f"Deleted: {name}")
            if not self._queries:
                self.dismiss(None)
        else:
            self.dismiss(None)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        name = str(event.option.prompt)
        self.dismiss(self._queries[name])
