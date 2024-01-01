from enum import Enum
from typing import Self
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from app.common.config import db
from siox.exceptions import EventException


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
        try:
            return cls(**data, file_id=file_id)
        except ValidationError:
            raise EventException(code=404, reason="Not found")

    @classmethod
    async def find_with_status(cls, file_id: str, statuses: set[TwexStatus]) -> Self:
        twex: Self = await cls.find_one(file_id)
        if twex.status not in statuses:
            raise EventException(code=400, reason=f"Wrong status: {twex.status.value}")
        return twex

    async def update_status(self, new_status: TwexStatus) -> None:
        self.status = new_status
        await db.hset(self.file_id, "status", new_status.value)

    @staticmethod
    async def transfer_status(
        file_id: str,
        statuses: set[TwexStatus],
        new_status: TwexStatus,
    ) -> None:
        twex_status = await db.hget(name=file_id, key="status")
        if twex_status is None:
            raise EventException(code=404, reason="Not found")
        if twex_status not in statuses:
            raise EventException(code=400, reason=f"Wrong status: {twex_status}")

        if new_status is TwexStatus.FINISHED:
            await db.delete(file_id)
        else:
            await db.hset(file_id, "status", new_status.value)

    async def save(self) -> None:
        await db.hset(
            name=self.file_id,
            mapping=self.model_dump(exclude="file_id"),  # type: ignore[arg-type]
        )
