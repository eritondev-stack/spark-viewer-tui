import json
from pathlib import Path

THEMES_FILE = Path(__file__).parent / "themes.json"

DEFAULT_THEMES = {
    "Transparent": {
        "background": "transparent",
        "text": "#e0e0e0",
        "accent": "#c026d3",
        "border": "#676667",
        "focus": "#c848b7",
        "key_color": "#01f649",
        "scrollbar": "#676667",
        "cursor": "#4d4dca",
        "col_type_color": "#fcff00",
    },
    "Dracula": {
        "background": "#282a36",
        "text": "#f8f8f2",
        "accent": "#bd93f9",
        "focus": "#ff79c6",
        "key_color": "#50fa7b",
        "scrollbar": "#6272a4",
        "cursor": "#44475a",
        "col_type_color": "#f1fa8c",
    },
    "Gruvbox": {
        "background": "#282828",
        "text": "#ebdbb2",
        "accent": "#d79921",
        "focus": "#d3869b",
        "key_color": "#b8bb26",
        "scrollbar": "#665c54",
        "cursor": "#3c3836",
        "col_type_color": "#fabd2f",
    },
}


def load_themes() -> dict[str, dict]:
    """Load themes from JSON. Creates default file if missing."""
    if THEMES_FILE.exists():
        with open(THEMES_FILE, "r") as f:
            return json.load(f)
    with open(THEMES_FILE, "w") as f:
        json.dump(DEFAULT_THEMES, f, indent=2)
    return dict(DEFAULT_THEMES)


def get_theme_names(themes: dict[str, dict]) -> list[str]:
    return list(themes.keys())


def _resolve(theme: dict, key: str, fallback_key: str) -> str:
    """Get theme value with fallback."""
    return theme.get(key, theme.get(fallback_key, theme["accent"]))


def get_theme_colors(themes: dict[str, dict]) -> dict[str, dict[str, str]]:
    """Return full color set per theme for runtime use."""
    result = {}
    for name, t in themes.items():
        result[name] = {
            "accent": t["accent"],
            "text": t["text"],
            "key_color": _resolve(t, "key_color", "accent"),
            "col_type_color": _resolve(t, "col_type_color", "accent"),
        }
    return result


def _css_class_name(theme_name: str) -> str:
    return theme_name.lower().replace(" ", "-")


def generate_base_theme_css(themes: dict[str, dict]) -> str:
    """Generate CSS for the base (first) theme's dynamic values: focus, scrollbar, cursor."""
    names = list(themes.keys())
    if not names:
        return ""
    t = themes[names[0]]
    focus = _resolve(t, "focus", "accent")
    scrollbar = _resolve(t, "scrollbar", "border")
    cursor = _resolve(t, "cursor", "accent")
    border = t.get("border", t["accent"])

    accent = t["accent"]

    return f"""
    Sidebar {{ border: solid {border}; }}
    #title {{ color: {accent}; }}
    #separator {{ color: {border}; }}
    #db-tree {{
        scrollbar-color: {scrollbar};
        scrollbar-color-hover: {scrollbar} 80%;
        scrollbar-color-active: {scrollbar} 60%;
    }}
    Tree > .tree--cursor {{ background: {cursor}; }}
    Tree > .tree--guides {{ color: {cursor}; }}
    Tree > .tree--guides-hover {{ color: {cursor}; }}
    TextArea {{
        border: solid {border};
        scrollbar-color: {scrollbar};
        scrollbar-color-hover: {scrollbar} 80%;
        scrollbar-color-active: {scrollbar} 60%;
    }}
    DataTable {{
        border: solid {border};
        scrollbar-color: {scrollbar};
        scrollbar-color-hover: {scrollbar} 80%;
        scrollbar-color-active: {scrollbar} 60%;
    }}
    DataTable > .datatable--cursor {{ background: {cursor}; }}
    Screen Sidebar:focus-within {{ border: solid {focus}; }}
    Screen TextArea:focus {{ border: solid {focus}; }}
    Screen DataTable:focus {{ border: solid {focus}; }}"""


def generate_theme_css(themes: dict[str, dict]) -> str:
    """Generate CSS override blocks for all themes except the first."""
    names = list(themes.keys())
    if len(names) <= 1:
        return ""

    parts: list[str] = []
    for name in names[1:]:
        t = themes[name]
        cls = _css_class_name(name)
        bg = t["background"]
        text = t["text"]
        accent = t["accent"]
        border = t.get("border", accent)
        focus = _resolve(t, "focus", "accent")
        scrollbar = _resolve(t, "scrollbar", "accent")
        cursor = _resolve(t, "cursor", "accent")

        parts.append(f"""
    /* ══════════ {name} Theme ══════════ */
    Screen.{cls} {{ background: {bg}; color: {text}; }}
    Screen.{cls} Sidebar {{ background: {bg}; border: solid {border}; color: {text}; }}
    Screen.{cls} Sidebar > Static {{ background: {bg}; }}
    Screen.{cls} #title {{ color: {accent}; background: {bg}; }}
    Screen.{cls} #separator {{ color: {accent}; background: {bg}; }}
    Screen.{cls} #db-tree {{ background: {bg}; color: {text}; scrollbar-color: {scrollbar}; scrollbar-color-hover: {scrollbar} 80%; scrollbar-color-active: {scrollbar} 60%; }}
    Screen.{cls} #main-container {{ background: {bg}; }}
    Screen.{cls} Vertical {{ background: {bg}; }}
    Screen.{cls} Container {{ background: {bg}; }}
    Screen.{cls} TextArea {{ background: {bg}; border: solid {border}; color: {text}; scrollbar-color: {scrollbar}; scrollbar-color-hover: {scrollbar} 80%; scrollbar-color-active: {scrollbar} 60%; }}
    Screen.{cls} DataTable {{ background: {bg}; border: solid {border}; color: {text}; scrollbar-color: {scrollbar}; scrollbar-color-hover: {scrollbar} 80%; scrollbar-color-active: {scrollbar} 60%; }}
    Screen.{cls} Static {{ background: {bg}; }}
    Screen.{cls} #status-bar {{ background: {bg}; }}
    Screen.{cls} Tree > .tree--cursor {{ background: {cursor}; }}
    Screen.{cls} Tree > .tree--guides {{ color: {cursor}; }}
    Screen.{cls} Tree > .tree--guides-hover {{ color: {cursor}; }}
    Screen.{cls} DataTable > .datatable--cursor {{ background: {cursor}; }}
    Screen.{cls} Sidebar:focus-within {{ border: solid {focus}; }}
    Screen.{cls} TextArea:focus {{ border: solid {focus}; }}
    Screen.{cls} DataTable:focus {{ border: solid {focus}; }}""")

    return "\n".join(parts)


def generate_modal_base_css(themes: dict[str, dict]) -> str:
    """Generate base CSS for modal screens (first theme's dynamic values)."""
    names = list(themes.keys())
    if not names:
        return ""
    t = themes[names[0]]
    bg = t["background"]
    text = t["text"]
    accent = t["accent"]
    border = t.get("border", accent)
    focus = _resolve(t, "focus", "accent")
    scrollbar = _resolve(t, "scrollbar", "border")
    cursor = _resolve(t, "cursor", "accent")
    key_color = _resolve(t, "key_color", "accent")
    col_type_color = _resolve(t, "col_type_color", "accent")

    return f"""
    #dialog {{ border: solid {border}; background: {bg}; color: {text}; }}
    #title {{ color: {accent}; background: {bg}; }}
    .label {{ background: {bg}; color: {text}; }}
    Input {{ background: {bg}; border: solid {border}; color: {text}; }}
    Input:focus {{ border: solid {focus}; }}
    Button {{ background: {bg}; color: {text}; }}
    Button:hover {{ background: {border} 30%; color: #ffffff; }}
    Button:focus {{ background: {border} 20%; color: #ffffff; }}
    #btn-save {{ color: {key_color}; }}
    #btn-save:hover {{ background: {key_color} 20%; }}
    #btn-load {{ color: {key_color}; }}
    #btn-load:hover {{ background: {key_color} 20%; }}
    #btn-delete {{ color: {col_type_color}; }}
    #btn-delete:hover {{ background: {col_type_color} 20%; }}
    #btn-cancel {{ color: {border}; }}
    #btn-add-path {{ color: {key_color}; }}
    #btn-add-path:hover {{ background: {key_color} 20%; }}
    #btn-remove-path {{ color: {col_type_color}; }}
    #btn-remove-path:hover {{ background: {col_type_color} 20%; }}
    OptionList {{ background: {bg}; border: solid {border}; color: {text}; scrollbar-color: {scrollbar}; scrollbar-color-hover: {scrollbar} 80%; scrollbar-color-active: {scrollbar} 60%; }}
    OptionList > .option-list--option-highlighted {{ background: {cursor}; }}
    OptionList > .option-list--option-hover {{ background: {bg}; }}
    OptionList:focus {{ border: solid {focus}; }}"""


def generate_modal_theme_css(themes: dict[str, dict]) -> str:
    """Generate CSS theme overrides for modal screens (all themes except first)."""
    names = list(themes.keys())
    if len(names) <= 1:
        return ""

    parts: list[str] = []
    for name in names[1:]:
        t = themes[name]
        cls = _css_class_name(name)
        bg = t["background"]
        text = t["text"]
        accent = t["accent"]
        border = t.get("border", accent)
        focus = _resolve(t, "focus", "accent")
        scrollbar = _resolve(t, "scrollbar", "accent")
        cursor = _resolve(t, "cursor", "accent")
        key_color = _resolve(t, "key_color", "accent")
        col_type_color = _resolve(t, "col_type_color", "accent")

        parts.append(f"""
    /* ══════════ {name} Modal Theme ══════════ */
    Screen.{cls} #dialog {{ border: solid {border}; background: {bg}; color: {text}; }}
    Screen.{cls} #title {{ color: {accent}; background: {bg}; }}
    Screen.{cls} .label {{ background: {bg}; color: {text}; }}
    Screen.{cls} Input {{ background: {bg}; border: solid {border}; color: {text}; }}
    Screen.{cls} Input:focus {{ border: solid {focus}; }}
    Screen.{cls} Button {{ background: {bg}; color: {text}; }}
    Screen.{cls} Button:hover {{ background: {border} 30%; color: #ffffff; }}
    Screen.{cls} Button:focus {{ background: {border} 20%; color: #ffffff; }}
    Screen.{cls} #btn-save {{ color: {key_color}; }}
    Screen.{cls} #btn-save:hover {{ background: {key_color} 20%; }}
    Screen.{cls} #btn-load {{ color: {key_color}; }}
    Screen.{cls} #btn-load:hover {{ background: {key_color} 20%; }}
    Screen.{cls} #btn-delete {{ color: {col_type_color}; }}
    Screen.{cls} #btn-delete:hover {{ background: {col_type_color} 20%; }}
    Screen.{cls} #btn-cancel {{ color: {border}; }}
    Screen.{cls} #btn-add-path {{ color: {key_color}; }}
    Screen.{cls} #btn-add-path:hover {{ background: {key_color} 20%; }}
    Screen.{cls} #btn-remove-path {{ color: {col_type_color}; }}
    Screen.{cls} #btn-remove-path:hover {{ background: {col_type_color} 20%; }}
    Screen.{cls} OptionList {{ background: {bg}; border: solid {border}; color: {text}; scrollbar-color: {scrollbar}; scrollbar-color-hover: {scrollbar} 80%; scrollbar-color-active: {scrollbar} 60%; }}
    Screen.{cls} OptionList > .option-list--option-highlighted {{ background: {cursor}; }}
    Screen.{cls} OptionList > .option-list--option-hover {{ background: {bg}; }}
    Screen.{cls} OptionList:focus {{ border: solid {focus}; }}""")

    return "\n".join(parts)


# ── Module-level exports (loaded once at import time) ─────────

_THEMES = load_themes()

THEME_NAMES = get_theme_names(_THEMES)
THEME_COLORS = get_theme_colors(_THEMES)
BASE_THEME_CSS = generate_base_theme_css(_THEMES)
THEME_CSS = generate_theme_css(_THEMES)
MODAL_BASE_CSS = generate_modal_base_css(_THEMES)
MODAL_THEME_CSS = generate_modal_theme_css(_THEMES)
