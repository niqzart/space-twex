from dataclasses import dataclass
from typing import Annotated, Generic, TypeVar

from pydantic import BaseModel
from socketio import AsyncNamespace  # type: ignore

from siox.emitters import DuplexEmitter, ServerEmitter
from siox.types import AnyCallable, RequestData

T = TypeVar("T")


class Depends:
    def __init__(self, dependency: AnyCallable) -> None:
        self.dependency = dependency


class Marker(Generic[T]):
    def extract(self, request: RequestData) -> T:
        raise NotImplementedError


class RequestMarker(Marker[RequestData]):
    def extract(self, request: RequestData) -> RequestData:
        return request


class NamespaceMarker(Marker[AsyncNamespace]):
    def extract(self, request: RequestData) -> AsyncNamespace:
        return request.namespace


class EventNameMarker(Marker[str]):
    def extract(self, request: RequestData) -> str:
        return request.event_name


class SessionIDMarker(Marker[str]):
    def extract(self, request: RequestData) -> str:
        return request.sid


@dataclass(frozen=True)
class ServerEmitterMarker(Marker[ServerEmitter]):
    name: str
    model: type[BaseModel]

    def extract(self, request: RequestData) -> ServerEmitter:
        return ServerEmitter(request.namespace, self.model, self.name, request.sid)


@dataclass(frozen=True)
class DuplexEmitterMarker(Marker[DuplexEmitter]):
    model: type[BaseModel]

    def extract(self, request: RequestData) -> DuplexEmitter:
        return DuplexEmitter(
            request.namespace, self.model, request.event_name, request.sid
        )


Sid = Annotated[str, SessionIDMarker()]
EventName = Annotated[str, EventNameMarker()]
Request = Annotated[RequestData, RequestMarker()]
