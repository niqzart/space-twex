import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Self
from unittest.mock import patch

from socketio import AsyncServer, packet  # type: ignore


class AsyncSIOTestClient:
    def __init__(self, server: AsyncServer, eio_sid: str) -> None:
        self.server: AsyncServer = server
        self.eio_sid: str = eio_sid
        self.events: dict[str, list[Any]] = {}
        self.packets: dict[int, list[packet.Packet]] = {}

    @property
    def sid(self) -> str:
        return self.server.manager.sid_from_eio_sid(self.eio_sid, "/")  # type: ignore

    def event_put(self, event: str, data: Any) -> None:
        self.events.setdefault(event, []).append(data)

    def event_pop(self, event: str) -> Any | None:
        result = self.events.get(event, [])
        if len(result) < 2:
            self.events.pop(event, None)
        if len(result) == 0:
            return None
        return result.pop(0)

    def event_count(self, event: str | None = None) -> int:
        if event is None:
            return sum(len(queue) for queue in self.events.values())
        return len(self.events.get(event, []))

    async def emit(self, event: str, *data: Any) -> Any:
        return await self.server._trigger_event(event, "/", self.sid, *data)


class AsyncSIOTestServer:
    def __init__(self, server: AsyncServer) -> None:
        self.server: AsyncServer = server
        self.clients: dict[str, AsyncSIOTestClient] = {}

    async def _send_packet(self, eio_sid: str, pkt: packet.Packet) -> None:
        client: AsyncSIOTestClient | None = self.clients.get(eio_sid)
        if client is None:
            return  # TODO logging?

        if pkt.packet_type == packet.EVENT or pkt.packet_type == packet.BINARY_EVENT:
            client.event_put(event=pkt.data[0], data=pkt.data[1])
        else:
            logging.warning(f"Unknown packet: {pkt.packet_type=} {pkt.data=}")
            client.packets.setdefault(pkt.packet_type, []).append(pkt)

    @contextmanager
    def mock(self) -> Iterator[Self]:
        mock_send = patch.object(self.server, "_send_packet", self._send_packet)
        mock_send.start()
        yield self
        mock_send.stop()

    @asynccontextmanager
    async def client(self) -> AsyncIterator[AsyncSIOTestClient]:
        eio_sid: str = self.server.eio.generate_id()
        client = AsyncSIOTestClient(server=self.server, eio_sid=eio_sid)
        self.clients[eio_sid] = client

        await self.server._handle_eio_connect(eio_sid=eio_sid, environ={})
        await self.server._handle_connect(eio_sid=eio_sid, namespace="/", data=None)

        # TODO check client.packets for the CONNECT-type packet

        yield client

        await self.server._handle_disconnect(eio_sid=eio_sid, namespace="/")
        await self.server._handle_eio_disconnect(eio_sid=eio_sid)

        # TODO check client.packets for the DISCONNECT-type packet
