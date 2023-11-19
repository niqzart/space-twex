from typing import Any, ClassVar, cast

from pydantic import BaseModel

from siox.socket import AsyncSocket
from siox.types import CallbackProtocol, DataType


class ServerEmitter:
    default_exclude_self: ClassVar[bool] = False

    def __init__(
        self,
        socket: AsyncSocket,
        model: type[BaseModel],
        name: str,
    ) -> None:
        self.socket = socket
        self.model = model
        self.name = name

    def _convert_data(self, data: Any) -> DataType | tuple[DataType, ...]:
        return cast(DataType, self.model.model_validate(data).model_dump())

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
            data=self._convert_data(data),
            target=target,
            skip_sid=skip_sid,
            exclude_self=exclude_self,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )


class DuplexEmitter(ServerEmitter):
    default_exclude_self: ClassVar[bool] = True
