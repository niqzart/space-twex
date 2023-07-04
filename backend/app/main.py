import logging
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ValidationError
from redis.asyncio import Redis
from socketio import ASGIApp, AsyncNamespace, AsyncServer  # type: ignore

db: Redis = Redis(decode_responses=True)  # type: ignore[type-arg]
sio = AsyncServer(async_mode="asgi")


class TwexStatus(str, Enum):
    OPEN = "open"
    FULL = "full"
    SENT = "sent"
    CONFIRMED = "confirmed"
    FINISHED = "finished"


class Ack(BaseModel):
    code: int
    data: Any | None = None


class MainNamespace(AsyncNamespace):  # type: ignore
    async def on_connect(self, sid: str, *_: Any) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, sid: str, data: Any) -> dict[str, Any]:
        try:
            args = self.CreateArgs.model_validate(data)
        except ValidationError as e:
            return Ack(code=422, data=str(e)).model_dump()

        file_id: str = uuid4().hex
        await db.hset(
            name=file_id,
            mapping={
                "file_name": args.file_name,
                "status": TwexStatus.OPEN.value,
            },
        )

        self.enter_room(sid=sid, room=f"{file_id}-publishers")
        return Ack(code=201, data={"file_id": file_id}).model_dump()

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(FileIdArgs):
        pass

    async def on_subscribe(self, sid: str, data: Any) -> dict[str, Any]:
        try:
            args = self.SubscribeArgs.model_validate(data)
        except ValidationError as e:
            return Ack(code=422, data=str(e)).model_dump()

        twex = await db.hgetall(name=args.file_id)
        if twex is None:
            return Ack(code=404).model_dump()
        if twex["status"] != TwexStatus.OPEN:
            return Ack(code=400, data=f"Wrong status: {twex['status']}").model_dump()
        await db.hset(args.file_id, "status", TwexStatus.FULL.value)
        # TODO more control over FULL for non-dialog twexes

        self.enter_room(sid=sid, room=f"{args.file_id}-subscribers")
        await self.emit(
            event="subscribe",
            data={**args.model_dump()},
            room=f"{args.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200, data=twex).model_dump()

    class SendArgs(FileIdArgs):
        chunk: bytes

    async def on_send(self, sid: str, data: Any) -> dict[str, Any]:
        try:
            args = self.SendArgs.model_validate(data)
        except ValidationError as e:
            return Ack(code=422, data=str(e)).model_dump()

        twex_status = await db.hget(name=args.file_id, key="status")
        if twex_status is None:
            return Ack(code=404).model_dump()
        if twex_status not in {TwexStatus.FULL, TwexStatus.CONFIRMED}:
            return Ack(code=400, data=f"Wrong status: {twex_status}").model_dump()
        await db.hset(args.file_id, "status", TwexStatus.SENT.value)

        chunk_id: str = uuid4().hex
        await self.emit(
            event="send",
            data={"chunk_id": chunk_id, **args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200, data={"chunk_id": chunk_id}).model_dump()

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(self, sid: str, data: Any) -> dict[str, Any]:
        try:
            args = self.ConfirmArgs.model_validate(data)
        except ValidationError as e:
            return Ack(code=422, data=str(e)).model_dump()

        twex_status = await db.hget(name=args.file_id, key="status")
        if twex_status is None:
            return Ack(code=404).model_dump()
        if twex_status != TwexStatus.SENT:
            return Ack(code=400, data=f"Wrong status: {twex_status}").model_dump()
        await db.hset(args.file_id, "status", TwexStatus.CONFIRMED.value)

        await self.emit(
            event="confirm",
            data={**args.model_dump()},
            room=f"{args.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200).model_dump()

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(self, sid: str, data: Any) -> dict[str, Any]:
        try:
            args = self.FinishArgs.model_validate(data)
        except ValidationError as e:
            return Ack(code=422, data=str(e)).model_dump()

        twex_status = await db.hget(name=args.file_id, key="status")
        if twex_status is None:
            return Ack(code=404).model_dump()
        if twex_status != TwexStatus.CONFIRMED:
            return Ack(code=400, data=f"Wrong status: {twex_status}").model_dump()
        await db.delete(args.file_id)

        await self.emit(
            event="finish",
            data={**args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200).model_dump()


sio.register_namespace(MainNamespace("/"))
app = ASGIApp(socketio_server=sio)
