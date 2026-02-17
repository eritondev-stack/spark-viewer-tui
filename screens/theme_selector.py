from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import OptionList, Label
from textual.widgets.option_list import Option

from themes import MODAL_BASE_CSS, MODAL_THEME_CSS, THEME_NAMES, THEME_COLORS


def _make_option(name: str, is_current: bool) -> Option:
    """Build an OptionList item with theme name and color preview."""
    colors = THEME_COLORS[name]
    t = Text()
    if is_current:
        t.append(" > ", style=f"bold {colors['key_color']}")
    else:
        t.append("   ")
    t.append(name, style="bold")
    t.append("  ")
    t.append("■ ", style=colors["accent"])
    t.append("■ ", style=colors["key_color"])
    t.append("■ ", style=colors["col_type_color"])
    return Option(t, id=name)


class ThemeSelectorScreen(ModalScreen[str | None]):
    """Modal to select a theme."""

    CSS = MODAL_BASE_CSS + MODAL_THEME_CSS + """
    ThemeSelectorScreen {
        align: center middle;
        background: transparent;
    }

    #dialog {
        padding: 1 2;
        width: 50;
        height: 14;
        layout: vertical;
    }

    #title {
        height: auto;
        width: 1fr;
        content-align: center middle;
        text-style: bold;
    }

    OptionList {
        height: 1fr;
        scrollbar-background: transparent;
        scrollbar-size: 1 2;
        margin: 1 0;
        border: none;
    }

    OptionList:focus {
        border: none;
    }

    OptionList > .option-list--option-hover {
        background: transparent;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, current_theme: str = ""):
        super().__init__()
        self._current_theme = current_theme

    def action_cancel(self) -> None:
        self.dismiss(None)

    def on_mount(self) -> None:
        if self._current_theme and THEME_NAMES and self._current_theme != THEME_NAMES[0]:
            css_class = self._current_theme.lower().replace(" ", "-")
            self.add_class(css_class)

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label("Select Theme", id="title")
            options = [
                _make_option(name, name == self._current_theme)
                for name in THEME_NAMES
            ]
            yield OptionList(*options, id="theme-list")

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        name = event.option.id
        if name:
            self.dismiss(name)
