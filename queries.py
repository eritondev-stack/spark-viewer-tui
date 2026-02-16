import json
from pathlib import Path

QUERIES_FILE = Path(__file__).parent / "saved_queries.json"


def load_queries() -> dict[str, str]:
    if QUERIES_FILE.exists():
        with open(QUERIES_FILE, "r") as f:
            return json.load(f)
    return {}


def save_queries(queries: dict[str, str]) -> None:
    with open(QUERIES_FILE, "w") as f:
        json.dump(queries, f, indent=2)


def add_query(name: str, sql: str) -> None:
    queries = load_queries()
    queries[name] = sql
    save_queries(queries)


def delete_query(name: str) -> None:
    queries = load_queries()
    queries.pop(name, None)
    save_queries(queries)
