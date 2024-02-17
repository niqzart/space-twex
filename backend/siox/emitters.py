from typing import Any, ClassVar

from siox.packagers import Packager
from siox.socket import AsyncSocket
from siox.types import CallbackProtocol


class ServerEmitter:
    default_exclude_self: ClassVar[bool] = False

    def __init__(
        self,
        socket: AsyncSocket,
        packager: Packager,
        name: str,
    ) -> None:
        self.socket = socket
        self.packager = packager
        self.name = name

    async def emit(
        self,
        data: Any,
        target: str | None = None,
        skip_sid: str | None = None,
        exclude_self: bool | None = None,
        namespace: str | None = None,
        callback: CallbackProtocol | None = None,
        ignore_queue: bool = False,
    ) -> None:
        if exclude_self is None:
            exclude_self = self.default_exclude_self
        await self.socket.emit(
            event=self.name,
            data=self.packager.pack(data),
            target=target,
            skip_sid=skip_sid,
            exclude_self=exclude_self,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )


class DuplexEmitter(ServerEmitter):
    default_exclude_self: ClassVar[bool] = True
