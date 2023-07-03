import logging
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ValidationError
from socketio import ASGIApp, AsyncServer  # type: ignore

sio = AsyncServer(async_mode="asgi")


class Ack(BaseModel):
    code: int
    data: Any | None = None


@sio.on("connect")  # type: ignore
async def hello(sid: str, *_: Any) -> None:
    logging.warning(f"Connected to {sid}")


@sio.on("create")  # type: ignore
async def create(sid: str, _: bytes) -> dict[str, Any]:
    file_id: str = uuid4().hex
    sio.enter_room(sid=sid, room=f"{file_id}-publishers")
    return Ack(code=201, data={"file_id": file_id}).model_dump()


class FileIdArgs(BaseModel):
    file_id: str


class SubscribeArgs(FileIdArgs):
    pass


@sio.on("subscribe")  # type: ignore
async def subscribe(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = SubscribeArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()
    sio.enter_room(sid=sid, room=f"{args.file_id}-subscribers")
    return Ack(code=200).model_dump()


class SendArgs(FileIdArgs):
    chunk: bytes


@sio.on("send")  # type: ignore
async def send(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = SendArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()

    chunk_id: str = uuid4().hex
    await sio.emit(
        event="send",
        data={"chunk_id": chunk_id, **args.model_dump()},
        room=f"{args.file_id}-subscribers",
        skip_sid=sid,
    )
    return Ack(code=200, data={"chunk_id": chunk_id}).model_dump()


class ConfirmArgs(FileIdArgs):
    chunk_id: str


@sio.on("confirm")  # type: ignore
async def confirm(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = ConfirmArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()

    await sio.emit(
        event="confirm",
        data={**args.model_dump()},
        room=f"{args.file_id}-publishers",
        skip_sid=sid,
    )
    return Ack(code=200).model_dump()


class FinishArgs(FileIdArgs):
    pass


@sio.on("finish")  # type: ignore
async def finish(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = FinishArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()

    await sio.emit(
        event="finish",
        data={**args.model_dump()},
        room=f"{args.file_id}-subscribers",
        skip_sid=sid,
    )
    return Ack(code=200).model_dump()


app = ASGIApp(socketio_server=sio)
