import json
import tempfile
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "spark_config.json"

DEFAULT_CONFIG = {
    "metastore_db": "",
    "warehouse_dir": "",
    "scan_paths": [],
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


def get_auto_spark_paths() -> dict:
    """Retorna paths de metastore/warehouse em /tmp para auto-start sem F2."""
    base = Path(tempfile.gettempdir()) / "spark-viewer-tui"
    return {
        "metastore_db": str(base / "metastore_db"),
        "warehouse_dir": str(base / "spark-warehouse"),
    }
