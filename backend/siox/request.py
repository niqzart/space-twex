from typing import Any

from siox.socket import AsyncServer, AsyncSocket


class RequestData:
    def __init__(
        self,
        server: AsyncServer,
        event_name: str,
        sid: str,
        *arguments: Any,
    ) -> None:
        self.socket = AsyncSocket(server, sid)
        self.event_name = event_name
        self.sid = sid
        self.arguments = arguments
