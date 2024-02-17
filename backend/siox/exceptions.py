from dataclasses import dataclass

from siox.types import DataType


@dataclass()
class EventException(Exception):
    code: int
    reason: str
    detail: DataType = None
