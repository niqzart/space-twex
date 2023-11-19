from typing import Any, cast

from pydantic import BaseModel

from siox.types import DataOrTuple


class Packager:
    def pack(self, data: Any) -> DataOrTuple:
        raise NotImplementedError


class NoopPackager(Packager):
    def pack(self, data: Any) -> DataOrTuple:
        return cast(DataOrTuple, data)


class PydanticPackager(Packager):
    def __init__(self, model: type[BaseModel]) -> None:
        self.model = model

    def pack(self, data: Any) -> DataOrTuple:
        return cast(DataOrTuple, self.model.model_validate(data).model_dump())
