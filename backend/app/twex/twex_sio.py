from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import Ack
from app.twex.twex_db import Twex, TwexStatus
from siox.markers import Depends, Sid
from siox.parsers import RequestSignature


def twex_with_status(statuses: set[TwexStatus]) -> Callable[..., Awaitable[Twex]]:
    async def twex_with_status_inner(file_id: str) -> Twex:
        return await Twex.find_with_status(file_id=file_id, statuses=statuses)

    return twex_with_status_inner


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> dict[str, Any] | None:
        handler_name = f"on_{event}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return None

        request = RequestSignature(handler, ns=type(self))
        client_event = request.extract()
        result = await client_event.execute(event, *args)
        return None if result is None else result.model_dump()

    async def on_connect(self, sid: Sid) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, args: CreateArgs, /, sid: Sid) -> Ack:
        twex = Twex(file_name=args.file_name)
        await twex.save()

        self.enter_room(sid=sid, room=f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id})

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(BaseModel):
        pass

    async def on_subscribe(
        self,
        args: SubscribeArgs,
        /,
        sid: Sid,
        twex: Annotated[Twex, Depends(twex_with_status({TwexStatus.OPEN}))],
    ) -> Ack:
        await twex.update_status(new_status=TwexStatus.FULL)
        # TODO more control over FULL for non-dialog twexes

        self.enter_room(sid=sid, room=f"{twex.file_id}-subscribers")
        await self.emit(
            event="subscribe",
            data={**args.model_dump(), "file_id": twex.file_id},
            room=f"{twex.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200, data=twex)

    class SendArgs(FileIdArgs):
        chunk: bytes

    async def on_send(self, args: SendArgs, /, sid: Sid) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.FULL, TwexStatus.CONFIRMED},
            new_status=TwexStatus.SENT,
        )

        chunk_id: str = uuid4().hex
        await self.emit(
            event="send",
            data={"chunk_id": chunk_id, **args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200, data={"chunk_id": chunk_id})

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(self, args: ConfirmArgs, /, sid: Sid) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.SENT},
            new_status=TwexStatus.CONFIRMED,
        )

        await self.emit(
            event="confirm",
            data={**args.model_dump()},
            room=f"{args.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200)

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(self, args: FinishArgs, /, sid: Sid) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.CONFIRMED},
            new_status=TwexStatus.FINISHED,
        )

        await self.emit(
            event="finish",
            data={**args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200)
