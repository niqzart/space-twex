import logging
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ValidationError
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import AbortException, Ack
from app.twex.twex_db import Twex, TwexStatus


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> dict[str, Any] | None:
        result: Ack | None
        try:
            result = await super().trigger_event(event, *args)
        except ValidationError as e:
            result = Ack(code=422, data=str(e))
        except AbortException as e:
            result = e.ack
        return None if result is None else result.model_dump()

    async def on_connect(self, sid: str, *_: Any) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, sid: str, data: Any) -> Ack:
        args = self.CreateArgs.model_validate(data)

        twex = Twex(file_name=args.file_name)
        await twex.save()

        self.enter_room(sid=sid, room=f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id})

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(FileIdArgs):
        pass

    async def on_subscribe(self, sid: str, data: Any) -> Ack:
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
        return Ack(code=200, data=twex)

    class SendArgs(FileIdArgs):
        chunk: bytes

    async def on_send(self, sid: str, data: Any) -> Ack:
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
        return Ack(code=200, data={"chunk_id": chunk_id})

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(self, sid: str, data: Any) -> Ack:
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
        return Ack(code=200)

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(self, sid: str, data: Any) -> Ack:
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
        return Ack(code=200)
