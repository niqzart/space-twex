from typing import Any, AsyncContextManager, Literal, Protocol, cast

import socketio  # type: ignore[import]

DataType = None | str | bytes | dict["DataType", "DataType"] | list["DataType"]


class CallbackProtocol(Protocol):
    def __call__(self, *args: DataType) -> None:
        pass


class AsyncServer:
    def __init__(self, backend: socketio.AsyncServer | socketio.AsyncNamespace) -> None:
        self.backend = backend

    async def emit(
        self,
        event: str,
        data: DataType | tuple[DataType, ...],
        target: str | None = None,
        skip_sid: str | None = None,
        namespace: str | None = None,
        callback: CallbackProtocol | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.backend.emit(
            event=event,
            data=data,
            to=target,
            skip_sid=skip_sid,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )

    async def send(
        self,
        data: DataType | tuple[DataType, ...],
        target: str | None = None,
        skip_sid: str | None = None,
        namespace: str | None = None,
        callback: CallbackProtocol | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.backend.send(
            data=data,
            to=target,
            skip_sid=skip_sid,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )

    async def call(
        self,
        event: str,
        data: DataType | tuple[DataType, ...],
        sid: str,
        namespace: str | None = None,
        timeout: int = 60,
        ignore_queue: bool = False,
    ) -> DataType | tuple[DataType, ...]:
        return cast(
            DataType | tuple[DataType, ...],
            await self.backend.call(
                event=event,
                data=data,
                sid=sid,
                namespace=namespace,
                timeout=timeout,
                ignore_queue=ignore_queue,
            ),
        )

    async def get_session(
        self,
        sid: str,
        namespace: str | None = None,
    ) -> dict[Any, Any]:
        return cast(
            dict[Any, Any],
            await self.backend.get_session(sid=sid, namespace=namespace),
        )

    async def save_session(
        self,
        sid: str,
        session: dict[Any, Any],
        namespace: str | None = None,
    ) -> None:
        await self.backend.save_session(sid=sid, session=session, namespace=namespace)

    def session(
        self,
        sid: str,
        namespace: str | None = None,
    ) -> AsyncContextManager[dict[Any, Any]]:
        return cast(
            AsyncContextManager[dict[Any, Any]],
            self.backend.session(sid=sid, namespace=namespace),
        )

    def transport(self, sid: str) -> Literal["polling", "webserver"]:
        return cast(Literal["polling", "webserver"], self.backend.transport(sid))

    def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.backend.enter_room(sid=sid, room=room, namespace=namespace)

    def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.backend.leave_room(sid=sid, room=room, namespace=namespace)

    def rooms(self, sid: str, namespace: str | None = None) -> list[str]:
        return cast(list[str], self.backend.rooms(sid=sid, namespace=namespace))

    async def close_room(self, room: str, namespace: str | None = None) -> None:
        await self.backend.close_room(room=room, namespace=namespace)

    async def disconnect(
        self,
        sid: str,
        namespace: str | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.backend.disconnect(
            sid=sid,
            namespace=namespace,
            ignore_queue=ignore_queue,
        )


class AsyncSocket:
    def __init__(
        self,
        backend: socketio.AsyncServer | socketio.AsyncNamespace,
        sid: str,
    ) -> None:
        self.server = AsyncServer(backend)
        self.backend = backend
        self.sid = sid

    async def emit(
        self,
        event: str,
        data: DataType | tuple[DataType, ...],
        target: str | None = None,
        skip_sid: str | None = None,
        exclude_self: bool | None = None,
        namespace: str | None = None,
        callback: CallbackProtocol | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.server.emit(
            event=event,
            data=data,
            target=target,
            skip_sid=skip_sid or self.sid if exclude_self else None,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )

    async def send(
        self,
        data: DataType | tuple[DataType, ...],
        target: str | None = None,
        skip_sid: str | None = None,
        exclude_self: bool | None = None,
        namespace: str | None = None,
        callback: CallbackProtocol | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.server.send(
            data=data,
            target=target,
            skip_sid=skip_sid or self.sid if exclude_self else None,
            namespace=namespace,
            callback=callback,
            ignore_queue=ignore_queue,
        )

    async def call(
        self,
        event: str,
        data: DataType | tuple[DataType, ...],
        namespace: str | None = None,
        timeout: int = 60,
        ignore_queue: bool = False,
    ) -> DataType | tuple[DataType, ...]:
        return await self.server.call(
            event=event,
            data=data,
            sid=self.sid,
            namespace=namespace,
            timeout=timeout,
            ignore_queue=ignore_queue,
        )

    async def get_session(self, namespace: str | None = None) -> dict[Any, Any]:
        return await self.server.get_session(sid=self.sid, namespace=namespace)

    async def save_session(
        self,
        session: dict[Any, Any],
        namespace: str | None = None,
    ) -> None:
        await self.server.save_session(
            sid=self.sid, session=session, namespace=namespace
        )

    def session(
        self,
        namespace: str | None = None,
    ) -> AsyncContextManager[dict[Any, Any]]:
        return self.server.session(sid=self.sid, namespace=namespace)

    def transport(self) -> Literal["polling", "webserver"]:
        return self.server.transport(self.sid)

    def enter_room(self, room: str, namespace: str | None = None) -> None:
        self.server.enter_room(sid=self.sid, room=room, namespace=namespace)

    def leave_room(self, room: str, namespace: str | None = None) -> None:
        self.server.leave_room(sid=self.sid, room=room, namespace=namespace)

    def rooms(self, namespace: str | None = None) -> list[str]:
        return self.server.rooms(sid=self.sid, namespace=namespace)

    async def close_room(self, room: str, namespace: str | None = None) -> None:
        await self.server.close_room(room=room, namespace=namespace)

    async def disconnect(
        self,
        namespace: str | None = None,
        ignore_queue: bool = False,
    ) -> None:
        await self.server.disconnect(
            sid=self.sid,
            namespace=namespace,
            ignore_queue=ignore_queue,
        )
