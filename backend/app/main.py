import logging
from enum import Enum
from typing import Any, Self
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError
from redis.asyncio import Redis
from socketio import ASGIApp, AsyncNamespace, AsyncServer  # type: ignore

db: Redis = Redis(decode_responses=True)  # type: ignore[type-arg]
sio = AsyncServer(async_mode="asgi")


class Ack(BaseModel):
    code: int
    data: Any | None = None


class AbortException(Exception):
    def __init__(self, ack: Ack) -> None:
        self.ack: Ack = ack


class TwexStatus(str, Enum):
    OPEN = "open"
    FULL = "full"
    SENT = "sent"
    CONFIRMED = "confirmed"
    FINISHED = "finished"


class Twex(BaseModel):
    file_id: str = Field(default_factory=lambda: uuid4().hex)
    file_name: str
    status: TwexStatus = TwexStatus.OPEN

    @classmethod
    async def find_one(cls, file_id: str) -> Self:
        data = await db.hgetall(name=file_id)
        if data is None:
            raise AbortException(Ack(code=404))
        return cls(**data)

    @classmethod
    async def find_with_status(cls, file_id: str, statuses: set[TwexStatus]) -> Self:
        twex: Self = await cls.find_one(file_id)
        if twex.status not in statuses:
            raise AbortException(Ack(code=400, data=f"Wrong status: {twex.status}"))
        return twex

    async def update_status(self, new_status: TwexStatus) -> None:
        await db.hset(self.file_id, "status", new_status.value)

    @staticmethod
    async def transfer_status(
        file_id: str,
        statuses: set[TwexStatus],
        new_status: TwexStatus,
    ) -> None:
        twex_status = await db.hget(name=file_id, key="status")
        if twex_status is None:
            raise AbortException(Ack(code=404))
        if twex_status not in statuses:
            raise AbortException(Ack(code=400, data=f"Wrong status: {twex_status}"))

        if new_status is TwexStatus.FINISHED:
            await db.delete(file_id)
        else:
            await db.hset(file_id, "status", new_status.value)

    async def save(self) -> None:
        await db.hset(
            name=self.file_id,
            mapping=self.model_dump(),  # type: ignore[arg-type]
        )


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> dict[str, Any]:
        try:
            return await super().trigger_event(event, *args)  # type: ignore
        except ValidationError as e:
            raise AbortException(Ack(code=422, data=str(e)))
        except AbortException as e:
            return e.ack.model_dump()

    async def on_connect(self, sid: str, *_: Any) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, sid: str, data: Any) -> dict[str, Any]:
        args = self.CreateArgs.model_validate(data)

        twex = Twex(file_name=args.file_name)
        await twex.save()

        self.enter_room(sid=sid, room=f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id}).model_dump()

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(FileIdArgs):
        pass

    async def on_subscribe(self, sid: str, data: Any) -> dict[str, Any]:
        args = self.SubscribeArgs.model_validate(data)

        twex: Twex = await Twex.find_with_status(
            file_id=args.file_id, statuses={TwexStatus.OPEN}
        )
        await twex.update_status(new_status=TwexStatus.FULL)
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
        args = self.SendArgs.model_validate(data)

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
        return Ack(code=200, data={"chunk_id": chunk_id}).model_dump()

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(self, sid: str, data: Any) -> dict[str, Any]:
        args = self.ConfirmArgs.model_validate(data)

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
        return Ack(code=200).model_dump()

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(self, sid: str, data: Any) -> dict[str, Any]:
        args = self.FinishArgs.model_validate(data)

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
        return Ack(code=200).model_dump()


sio.register_namespace(MainNamespace("/"))
app = ASGIApp(socketio_server=sio)
