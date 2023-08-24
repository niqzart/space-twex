from typing import Annotated

from siox.types import AnyCallable


class SessionID:
    pass


class Depends:
    def __init__(self, dependency: AnyCallable) -> None:
        self.dependency = dependency


Sid = Annotated[str, SessionID()]
