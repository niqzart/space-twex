from __future__ import annotations

from typing import Any, cast

from pydantic import BaseModel

from siox.packager import Packager, PydanticPackager
from siox.types import DataOrTuple


class Ack(BaseModel):
    code: int
    data: Any | None = None


class AbortException(Exception):
    def __init__(self, ack: Ack) -> None:
        self.ack: Ack = ack


class AckPacker(PydanticPackager):
    def __init__(self, model: type[BaseModel], code: int = 200):
        super().__init__(model)
        self.code = code

    def pack(self, data: Any) -> DataOrTuple:
        result = self.model.model_validate(data).model_dump()
        return cast(DataOrTuple, {**result, "code": self.code})


class NoContentPacker(Packager):
    def pack(self, data: Any) -> DataOrTuple:
        return {"code": 204}
