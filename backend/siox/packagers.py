from typing import Any, cast

from pydantic import BaseModel

from siox.types import DataOrTuple


class Packager:
    def pack(self, data: Any) -> DataOrTuple:
        raise NotImplementedError


class CastedPackager(Packager):
    def pack_to_any(self, data: Any) -> Any:
        raise NotImplementedError

    def pack(self, data: Any) -> DataOrTuple:
        return cast(DataOrTuple, self.pack_to_any(data))


class NoopPackager(CastedPackager):
    def pack_to_any(self, data: Any) -> Any:
        return data


class PydanticPackager(CastedPackager):
    def __init__(self, model: type[BaseModel]) -> None:
        self.model = model

    def pack_to_any(self, data: Any) -> Any:
        return self.model.model_validate(data).model_dump(mode="json")
