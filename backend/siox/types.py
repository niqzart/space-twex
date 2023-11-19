from collections.abc import Callable
from typing import Any, Protocol

LocalNS = dict[str, Any]
AnyCallable = Callable[..., Any]

DataType = None | str | bytes | dict["DataType", "DataType"] | list["DataType"]
DataOrTuple = DataType | tuple[DataType, ...]


class CallbackProtocol(Protocol):
    def __call__(self, *args: DataType) -> None:
        pass
