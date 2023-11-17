from collections.abc import Callable
from typing import Any

from socketio import AsyncNamespace  # type: ignore

LocalNS = dict[str, Any]
AnyCallable = Callable[..., Any]


class RequestData:
    def __init__(
        self,
        namespace: AsyncNamespace,  # TODO make SOIX independent of this
        event_name: str,
        sid: str,
        *arguments: Any,
    ) -> None:
        self.namespace = namespace
        self.event_name = event_name
        self.sid = sid
        self.arguments = arguments
