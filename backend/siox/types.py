from collections.abc import Callable
from typing import Any

LocalNS = dict[str, Any]
AnyCallable = Callable[..., Any]


class RequestData:
    def __init__(self, event_name: str, sid: str, *arguments: Any) -> None:
        self.event_name = event_name
        self.sid = sid
        self.arguments = arguments
