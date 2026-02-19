from __future__ import annotations

import json
import socket
from typing import Any

_IPC_HOST = "127.0.0.1"
_IPC_PORT = 7891
_MAX_ROWS = 10_000


def print_df(df: Any, table_name: str, host: str = _IPC_HOST, port: int = _IPC_PORT) -> None:
    """Envia um DataFrame para o spark-viewer-tui em execução.

    O DataFrame aparece na sidebar como ``live.<table_name>`` e pode ser
    consultado com ``SELECT * FROM global_temp.<table_name>``.

    Args:
        df: pandas.DataFrame ou pyspark.sql.DataFrame
        table_name: nome da tabela que aparecerá no banco ``live``
        host: host do servidor IPC (padrão: 127.0.0.1)
        port: porta do servidor IPC (padrão: 7891)

    Raises:
        ConnectionRefusedError: se o spark-viewer-tui não estiver rodando
        TypeError: se o tipo do DataFrame não for suportado
    """
    payload = _build_payload(df, table_name)
    _send_payload(payload, host, port)


# ── Construção do payload ─────────────────────────────────────────────────────

def _build_payload(df: Any, table_name: str) -> dict:
    module = type(df).__module__
    if module.startswith("pyspark"):
        return _from_pyspark(df, table_name)
    # Tenta pandas (ou qualquer coisa com .dtypes e .itertuples)
    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            return _from_pandas(df, table_name)
    except ImportError:
        pass
    raise TypeError(
        f"Tipo de DataFrame não suportado: {type(df).__module__}.{type(df).__name__}. "
        "Use pandas.DataFrame ou pyspark.sql.DataFrame."
    )


def _from_pandas(df: Any, table_name: str) -> dict:
    row_count = len(df)
    if row_count > _MAX_ROWS:
        print(f"[spark-viewer] Aviso: DataFrame tem {row_count} linhas. Truncando para {_MAX_ROWS}.")
        df = df.iloc[:_MAX_ROWS]

    schema = [
        [str(col), _pandas_dtype_to_spark(str(dtype))]
        for col, dtype in zip(df.columns, df.dtypes)
    ]
    rows = [
        [None if _is_null(v) else str(v) for v in row]
        for row in df.itertuples(index=False, name=None)
    ]
    return {"action": "print_df", "table_name": table_name, "schema": schema, "rows": rows}


def _from_pyspark(df: Any, table_name: str) -> dict:
    count = df.count()
    if count > _MAX_ROWS:
        print(f"[spark-viewer] Aviso: DataFrame tem {count} linhas. Truncando para {_MAX_ROWS}.")
        df = df.limit(_MAX_ROWS)

    schema = [[field.name, field.dataType.simpleString()] for field in df.schema.fields]
    rows = [
        [None if v is None else str(v) for v in row]
        for row in df.collect()
    ]
    return {"action": "print_df", "table_name": table_name, "schema": schema, "rows": rows}


# ── Mapeamento de tipos ───────────────────────────────────────────────────────

_PANDAS_TO_SPARK: dict[str, str] = {
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "int64": "bigint",
    "uint8": "smallint",
    "uint16": "int",
    "uint32": "bigint",
    "uint64": "bigint",
    "float16": "float",
    "float32": "float",
    "float64": "double",
    "bool": "boolean",
    "boolean": "boolean",
    "object": "string",
    "string": "string",
    "category": "string",
}


def _pandas_dtype_to_spark(dtype_str: str) -> str:
    normalized = dtype_str.lower().strip()
    # datetime com ou sem timezone
    if normalized.startswith("datetime64"):
        return "timestamp"
    # tipos inteiros anuláveis do pandas (Int8, Int64, UInt32 etc.)
    if normalized.startswith(("int", "uint")):
        clean = normalized.replace("[pyarrow]", "").strip()
        return _PANDAS_TO_SPARK.get(clean, "bigint")
    return _PANDAS_TO_SPARK.get(normalized, "string")


def _is_null(v: Any) -> bool:
    if v is None:
        return True
    try:
        # NaN é o único valor onde v != v
        return v != v  # noqa: PLR0124
    except Exception:
        return False


# ── Envio via socket ──────────────────────────────────────────────────────────

def _send_payload(payload: dict, host: str, port: int) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    length_header = len(body).to_bytes(4, "big")

    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(length_header + body)
            response_raw = sock.recv(256)
    except ConnectionRefusedError:
        raise ConnectionRefusedError(
            f"Não foi possível conectar ao spark-viewer-tui em {host}:{port}. "
            "Verifique se o TUI está rodando."
        )

    result = json.loads(response_raw)
    if result.get("status") != "ok":
        raise RuntimeError(f"Erro no TUI: {result.get('message', 'desconhecido')}")
