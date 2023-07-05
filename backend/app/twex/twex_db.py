from enum import Enum
from typing import Self
from uuid import uuid4

from pydantic import BaseModel, Field

from app.common.config import db
from app.common.sockets import AbortException, Ack


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
