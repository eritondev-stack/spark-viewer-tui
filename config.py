import json
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "spark_config.json"

DEFAULT_CONFIG = {
    "metastore_db": "",
    "warehouse_dir": "",
}


def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            saved = json.load(f)
            return {**DEFAULT_CONFIG, **saved}
    return DEFAULT_CONFIG.copy()


def save_config(config: dict) -> None:
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
