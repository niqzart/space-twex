from typing import Any, ClassVar

from pydantic import BaseModel
from socketio import AsyncNamespace, AsyncServer  # type: ignore[import]


class ServerEmitter:
    default_exclude_self: ClassVar[bool] = False

    def __init__(
        self,
        server: AsyncServer | AsyncNamespace,
        model: type[BaseModel],
        name: str,
        sid: str,
    ) -> None:
        self.server = server
        self.model = model
        self.name = name
        self.sid = sid

    def _convert_data(self, data: Any) -> Any:
        return self.model.model_validate(data).model_dump()

    async def emit(
        self,
        data: Any,
        target: str | None,
        exclude_self: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if exclude_self is None:
            exclude_self = self.default_exclude_self
        await self.server.emit(
            event=self.name,
            data=self._convert_data(data),
            to=target,
            skip_sid=kwargs.get("skip_sid") or self.sid if exclude_self else None,
            **kwargs,
        )


class DuplexEmitter(ServerEmitter):
    default_exclude_self: ClassVar[bool] = True
