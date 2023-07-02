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
async def create(_: str, __: bytes) -> dict[str, Any]:
    return Ack(code=201, data=uuid4().hex).model_dump()


class SubscribeArgs(BaseModel):
    file_id: str


@sio.on("subscribe")  # type: ignore
async def subscribe(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = SubscribeArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()
    sio.enter_room(sid=sid, room=args.file_id)
    return Ack(code=200).model_dump()


class SendArgs(BaseModel):
    file_id: str
    chunk: bytes


@sio.on("send")  # type: ignore
async def send(sid: str, data: Any) -> dict[str, Any]:
    try:
        args = SendArgs.model_validate(data)
    except ValidationError as e:
        return Ack(code=422, data=str(e)).model_dump()
    logging.warning(f"{args.file_id=} {args.chunk=}")
    await sio.emit("send", args.chunk, room=args.file_id, skip_sid=sid)
    return Ack(code=200).model_dump()


app = ASGIApp(socketio_server=sio)
