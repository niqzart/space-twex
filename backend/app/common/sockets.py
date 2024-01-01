from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from siox.packagers import Packager, PydanticPackager
from siox.types import DataOrTuple


class AckPacker(PydanticPackager):
    def __init__(self, model: type[BaseModel], code: int = 200):
        super().__init__(model)
        self.code = code

    def pack_to_any(self, data: Any) -> Any:
        return {**self.model.model_validate(data).model_dump(), "code": self.code}


class NoContentPacker(Packager):
    def pack(self, data: Any) -> DataOrTuple:
        return {"code": 204}
