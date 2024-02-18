from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import uuid4

from pydantic import BaseModel
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import AckPacker, NoContentPacker
from app.twex.twex_db import Twex, TwexStatus
from siox.emitters import DuplexEmitter
from siox.markers import Depends, Sid, Socket
from siox.routing import EventRouter


def twex_with_status(statuses: set[TwexStatus]) -> Callable[..., Awaitable[Twex]]:
    async def twex_with_status_inner(file_id: str) -> Twex:
        return await Twex.find_with_status(file_id=file_id, statuses=statuses)

    return twex_with_status_inner


router = EventRouter()


@router.on("connect")
async def accept_connection(sid: Sid) -> None:
    logging.warning(f"Connected to {sid}")


class CreateArgs(BaseModel):
    file_name: str


class FileIdArgs(BaseModel):
    file_id: str


@router.on("create")
async def create_twex(
    args: CreateArgs,
    /,
    socket: Socket,
) -> Annotated[FileIdArgs, AckPacker(FileIdArgs, code=201)]:
    twex = Twex(file_name=args.file_name)
    await twex.save()

    socket.enter_room(f"{twex.file_id}-publishers")
    return FileIdArgs(file_id=twex.file_id)


class SubscribeArgs(BaseModel):
    pass


class SubscribeResp(FileIdArgs, SubscribeArgs, from_attributes=True):
    file_name: str


@router.on("subscribe")
async def create_twex_subscription(
    args: SubscribeArgs,
    /,
    socket: Socket,
    twex: Annotated[Twex, Depends(twex_with_status({TwexStatus.OPEN}))],
    event: Annotated[DuplexEmitter, SubscribeResp],
) -> Annotated[Twex, AckPacker(SubscribeResp)]:
    if args:
        pass

    await twex.update_status(new_status=TwexStatus.FULL)
    # TODO more control over FULL for non-dialog twexes

    socket.enter_room(f"{twex.file_id}-subscribers")
    await event.emit(data=twex, target=f"{twex.file_id}-publishers")
    return twex


class SendArgs(FileIdArgs):
    chunk: str


class SendAck(BaseModel):
    chunk_id: str


class SendResp(SendArgs, SendAck):
    pass


@router.on("send")
async def send_data_to_twex(
    args: SendArgs,
    /,
    event: Annotated[DuplexEmitter, SendResp],
) -> Annotated[SendAck, AckPacker(SendAck)]:
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
    return SendAck(chunk_id=chunk_id)


class ConfirmArgs(FileIdArgs):
    chunk_id: str


@router.on("confirm")
async def confirm_data_receive_from_twex(
    args: ConfirmArgs,
    /,
    event: Annotated[DuplexEmitter, ConfirmArgs],
) -> Annotated[None, NoContentPacker()]:
    await Twex.transfer_status(
        file_id=args.file_id,
        statuses={TwexStatus.SENT},
        new_status=TwexStatus.CONFIRMED,
    )
    await event.emit(data=args, target=f"{args.file_id}-publishers")


class FinishArgs(FileIdArgs):
    pass


@router.on("finish")
async def finish_twex_transmission(
    args: FinishArgs,
    /,
    event: Annotated[DuplexEmitter, FinishArgs],
) -> Annotated[None, NoContentPacker()]:
    await Twex.transfer_status(
        file_id=args.file_id,
        statuses={TwexStatus.CONFIRMED},
        new_status=TwexStatus.FINISHED,
    )
    await event.emit(data=args, target=f"{args.file_id}-subscribers")
