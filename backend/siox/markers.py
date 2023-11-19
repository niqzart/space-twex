from dataclasses import dataclass
from typing import Annotated, Generic, TypeVar

from siox.emitters import DuplexEmitter, ServerEmitter
from siox.packager import Packager
from siox.request import RequestData
from siox.socket import AsyncServer, AsyncSocket
from siox.types import AnyCallable

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


class EventNameMarker(Marker[str]):
    def extract(self, request: RequestData) -> str:
        return request.event_name


class SessionIDMarker(Marker[str]):
    def extract(self, request: RequestData) -> str:
        return request.sid


@dataclass(frozen=True)
class ServerEmitterMarker(Marker[ServerEmitter]):
    name: str
    packager: Packager

    def extract(self, request: RequestData) -> ServerEmitter:
        return ServerEmitter(request.socket, self.packager, self.name)


@dataclass(frozen=True)
class DuplexEmitterMarker(Marker[DuplexEmitter]):
    packager: Packager

    def extract(self, request: RequestData) -> DuplexEmitter:
        return DuplexEmitter(request.socket, self.packager, request.event_name)


class AsyncServerMarker(Marker[AsyncServer]):
    def extract(self, request: RequestData) -> AsyncServer:
        return request.socket.server


class AsyncSocketMarker(Marker[AsyncSocket]):
    def extract(self, request: RequestData) -> AsyncSocket:
        return request.socket


Sid = Annotated[str, SessionIDMarker()]
EventName = Annotated[str, EventNameMarker()]
Request = Annotated[RequestData, RequestMarker()]
