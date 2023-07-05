from typing import Any

from pydantic import BaseModel


class Ack(BaseModel):
    code: int
    data: Any | None = None


class AbortException(Exception):
    def __init__(self, ack: Ack) -> None:
        self.ack: Ack = ack
