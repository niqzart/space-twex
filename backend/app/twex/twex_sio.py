from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import Ack
from app.twex.twex_db import Twex, TwexStatus
from siox.emitters import DuplexEmitter
from siox.markers import Depends, Sid
from siox.parsers import RequestSignature
from siox.request import RequestData
from siox.socket import AsyncSocket
from siox.types import DataOrTuple


def twex_with_status(statuses: set[TwexStatus]) -> Callable[..., Awaitable[Twex]]:
    async def twex_with_status_inner(file_id: str) -> Twex:
        return await Twex.find_with_status(file_id=file_id, statuses=statuses)

    return twex_with_status_inner


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> DataOrTuple:
        handler_name = f"on_{event}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return None

        request = RequestSignature(handler, ns=type(self))
        client_event = request.extract()
        result = await client_event.execute(RequestData(self, event, *args))
        if isinstance(result, BaseModel):
            return result.model_dump()
        return result

    async def on_connect(self, sid: Sid) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, args: CreateArgs, /, socket: AsyncSocket) -> Ack:
        twex = Twex(file_name=args.file_name)
        await twex.save()

        socket.enter_room(f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id})

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(BaseModel):
        pass

    class SubscribeResp(FileIdArgs, SubscribeArgs):
        pass

    async def on_subscribe(
        self,
        args: SubscribeArgs,
        /,
        socket: AsyncSocket,
        twex: Annotated[Twex, Depends(twex_with_status({TwexStatus.OPEN}))],
        event: Annotated[DuplexEmitter, SubscribeResp],
    ) -> Ack:
        await twex.update_status(new_status=TwexStatus.FULL)
        # TODO more control over FULL for non-dialog twexes

        socket.enter_room(f"{twex.file_id}-subscribers")
        await event.emit(
            data={**args.model_dump(), "file_id": twex.file_id},
            target=f"{twex.file_id}-publishers",
        )
        return Ack(code=200, data=twex)

    class SendArgs(FileIdArgs):
        chunk: bytes

    class SendResp(SendArgs):
        chunk_id: str

    async def on_send(
        self,
        args: SendArgs,
        /,
        event: Annotated[DuplexEmitter, SendResp],
    ) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.FULL, TwexStatus.CONFIRMED},
            new_status=TwexStatus.SENT,
        )

        chunk_id: str = uuid4().hex
        await event.emit(
            data={"chunk_id": chunk_id, **args.model_dump()},
            target=f"{args.file_id}-subscribers",
        )
        return Ack(code=200, data={"chunk_id": chunk_id})

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(
        self,
        args: ConfirmArgs,
        /,
        event: Annotated[DuplexEmitter, ConfirmArgs],
    ) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.SENT},
            new_status=TwexStatus.CONFIRMED,
        )
        await event.emit(data=args, target=f"{args.file_id}-publishers")
        return Ack(code=200)

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(
        self,
        args: FinishArgs,
        /,
        event: Annotated[DuplexEmitter, FinishArgs],
    ) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.CONFIRMED},
            new_status=TwexStatus.FINISHED,
        )
        await event.emit(data=args, target=f"{args.file_id}-subscribers")
        return Ack(code=200)
