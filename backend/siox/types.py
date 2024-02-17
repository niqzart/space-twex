from collections.abc import Callable
from typing import Any, Protocol

import socketio  # type: ignore[import]

LocalNS = dict[str, Any]
AnyCallable = Callable[..., Any]

DataType = None | int | str | bytes | dict["DataType", "DataType"] | list["DataType"]
DataOrTuple = DataType | tuple[DataType, ...]

SocketIOBackend = socketio.AsyncServer | socketio.AsyncNamespace


class CallbackProtocol(Protocol):
    def __call__(self, *args: DataType) -> None:
        pass


class HandlerProtocol(Protocol):
    async def __call__(self, sid: str, *args: DataType) -> DataOrTuple:
        pass
