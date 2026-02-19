from __future__ import annotations

import asyncio
import json
from typing import Callable, Coroutine, Any

_MAX_PAYLOAD_BYTES = 256 * 1024 * 1024  # 256 MB guard

IPC_HOST = "127.0.0.1"
IPC_PORT = 7891


class IPCServer:
    """Servidor asyncio TCP que recebe DataFrames enviados por print_df().

    Roda no mesmo event loop do Textual — nenhuma thread extra necessária.
    Protocolo: 4 bytes big-endian (tamanho do body) + JSON UTF-8.
    """

    def __init__(self, on_dataframe_received: Callable[[dict], Coroutine[Any, Any, None]]) -> None:
        self._callback = on_dataframe_received
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        """Inicia o servidor TCP. Levanta OSError se a porta já estiver em uso."""
        self._server = await asyncio.start_server(
            self._handle_client,
            IPC_HOST,
            IPC_PORT,
        )

    async def stop(self) -> None:
        """Para o servidor graciosamente."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            # Lê o header de 4 bytes com o tamanho do payload
            header = await reader.readexactly(4)
            length = int.from_bytes(header, "big")

            if length > _MAX_PAYLOAD_BYTES:
                response = json.dumps({"status": "error", "message": "payload too large"})
                writer.write(response.encode())
                await writer.drain()
                return

            raw = await reader.readexactly(length)
            payload = json.loads(raw.decode("utf-8"))
            await self._dispatch(payload, writer)

        except (asyncio.IncompleteReadError, json.JSONDecodeError, OSError):
            # Conexão encerrada prematuramente ou JSON inválido — ignora silenciosamente
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _dispatch(self, payload: dict, writer: asyncio.StreamWriter) -> None:
        action = payload.get("action")

        if action == "print_df":
            await self._callback(payload)
            writer.write(b'{"status":"ok"}')
        elif action == "ping":
            writer.write(b'{"status":"pong"}')
        else:
            response = json.dumps({"status": "error", "message": f"unknown action: {action}"})
            writer.write(response.encode())

        await writer.drain()
