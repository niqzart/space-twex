from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from inspect import iscoroutinefunction, signature
from typing import Annotated, Any, TypeVar, get_args, get_origin
from uuid import uuid4

from pydantic import BaseModel, ValidationError, create_model
from pydantic._internal._typing_extra import eval_type_lenient
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import AbortException, Ack
from app.twex.twex_db import Twex, TwexStatus

T = TypeVar("T")


async def call_or_await(
    handler: Callable[..., T | Awaitable[T]],
    *args: Any,
    **kwargs: Any,
) -> T:
    if iscoroutinefunction(handler) is True:
        return await handler(*args, **kwargs)  # type: ignore[no-any-return, misc]
    elif callable(handler):
        return handler(*args, **kwargs)  # type: ignore[return-value]
    raise Exception("Handler is not callable")


def decode_annotation(annotation: Any) -> Any | None:
    if get_origin(annotation) is Annotated:
        annotation_args = get_args(annotation)
        if len(annotation_args) == 2:
            return annotation_args[1]
    return None


class SessionID:
    pass


class Depends:
    def __init__(self, dependency: Callable[..., Any]) -> None:
        self.dependency = dependency


class Dependency:
    def __init__(self, depends: Depends, request: Request):
        self.qualname: str = depends.dependency.__qualname__
        self.dependency = depends.dependency

        self.kwargs: dict[str, Any] = {}  # param_name to value (kwargs)
        self.unresolved: dict[str, str] = {}  # qualname to param_name

        for param in signature(depends.dependency).parameters.values():
            ann: Any = param.annotation
            if isinstance(ann, str):
                global_ns = getattr(self.dependency, "__globals__", {})
                ann = eval_type_lenient(ann, global_ns, request.local_ns)

            if isinstance(ann, type) and issubclass(ann, Request):
                self.kwargs[param.name] = request
            else:
                decoded = decode_annotation(ann)
                if isinstance(decoded, Depends):
                    self.unresolved[decoded.dependency.__qualname__] = param.name
                elif isinstance(decoded, SessionID):
                    self.kwargs[param.name] = request.sid
                else:
                    raise NotImplementedError(f"Parameter {param} {ann} not supported")

    async def execute(self) -> Any:
        if len(self.unresolved) != 0:
            raise Exception("Not all sub-dependencies were resolved")

        return await call_or_await(self.dependency, **self.kwargs)


class Request:
    def __init__(
        self,
        event_name: str,
        sid: str,
        *arguments: Any,
        ns: type | None = None,
    ) -> None:
        self.event_name: str = event_name
        self.sid: str = sid
        self.arguments: tuple[Any, ...] = arguments
        self.local_ns: dict[str, Any] = {} if ns is None else dict(ns.__dict__)

    async def execute(self, handler: Callable[..., Ack]) -> Ack | None:
        kwargs: dict[str, Any] = {}  # qualname to value

        # parsing the signature
        arg_names: list[str] = []
        arg_types: list[type] = []
        dependencies: dict[str, Dependency] = {}
        for param in signature(handler).parameters.values():
            ann: Any = param.annotation
            if isinstance(ann, str):
                global_ns = getattr(handler, "__globals__", {})
                ann = eval_type_lenient(ann, global_ns, self.local_ns)

            if isinstance(ann, type):
                if issubclass(ann, Request):
                    kwargs[param.name] = self
                else:
                    arg_names.append(param.name)
                    arg_types.append(ann)
            else:
                decoded = decode_annotation(ann)
                if isinstance(decoded, Depends):
                    dependencies[param.name] = Dependency(decoded, self)
                elif isinstance(decoded, SessionID):
                    kwargs[param.name] = self.sid
                else:
                    raise NotImplementedError(f"Parameter {param} {ann} not supported")

        # checking client arguments (positional-only because socketio)
        if len(arg_types) != len(self.arguments):
            return Ack(
                code=422,
                data=f"Required {len(arg_types)}, but {len(self.arguments)} given",
            )
        # RootModel(tuple(arg_types))
        arg_model = create_model(  # type: ignore[call-overload]
            "InputModel", **{key: (ann, ...) for key, ann in zip(arg_names, arg_types)}
        )

        try:
            args: BaseModel = arg_model.model_validate(
                dict(zip(arg_names, self.arguments))
            )
            kwargs.update({name: getattr(args, name) for name in arg_names})
        except (ValidationError, AttributeError) as e:
            return Ack(code=422, data=str(e))

        # resolving dependencies
        while len(dependencies) != 0:
            layer: list[str] = []  # qualnames of the layer
            for name, dependency in dependencies.items():
                for resolved in layer:
                    param_name = dependency.unresolved.pop(resolved, None)
                    if param_name is not None:
                        dependency.kwargs[param_name] = kwargs[resolved]
                layer = []
                if len(dependency.unresolved) == 0:
                    layer.append(dependency.qualname)
                    kwargs[name] = await dependency.execute()

        # call the function
        try:
            return await call_or_await(handler, **kwargs)
        except AbortException as e:
            return e.ack


Sid = Annotated[str, SessionID()]


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> dict[str, Any] | None:
        handler_name = f"on_{event}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return None

        request = Request(event, *args, ns=type(self))
        result = await request.execute(handler)
        return None if result is None else result.model_dump()

    async def on_connect(self, sid: Sid) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, sid: Sid, args: CreateArgs) -> Ack:
        twex = Twex(file_name=args.file_name)
        await twex.save()

        self.enter_room(sid=sid, room=f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id})

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(FileIdArgs):
        pass

    async def on_subscribe(self, sid: Sid, args: SubscribeArgs) -> Ack:
        twex: Twex = await Twex.find_with_status(
            file_id=args.file_id, statuses={TwexStatus.OPEN}
        )
        await twex.update_status(new_status=TwexStatus.FULL)
        # TODO more control over FULL for non-dialog twexes

        self.enter_room(sid=sid, room=f"{args.file_id}-subscribers")
        await self.emit(
            event="subscribe",
            data={**args.model_dump()},
            room=f"{args.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200, data=twex)

    class SendArgs(FileIdArgs):
        chunk: bytes

    async def on_send(self, sid: Sid, args: SendArgs) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.FULL, TwexStatus.CONFIRMED},
            new_status=TwexStatus.SENT,
        )

        chunk_id: str = uuid4().hex
        await self.emit(
            event="send",
            data={"chunk_id": chunk_id, **args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200, data={"chunk_id": chunk_id})

    class ConfirmArgs(FileIdArgs):
        chunk_id: str

    async def on_confirm(self, sid: Sid, args: ConfirmArgs) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.SENT},
            new_status=TwexStatus.CONFIRMED,
        )

        await self.emit(
            event="confirm",
            data={**args.model_dump()},
            room=f"{args.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200)

    class FinishArgs(FileIdArgs):
        pass

    async def on_finish(self, sid: Sid, args: FinishArgs) -> Ack:
        await Twex.transfer_status(
            file_id=args.file_id,
            statuses={TwexStatus.CONFIRMED},
            new_status=TwexStatus.FINISHED,
        )

        await self.emit(
            event="finish",
            data={**args.model_dump()},
            room=f"{args.file_id}-subscribers",
            skip_sid=sid,
        )
        return Ack(code=200)
