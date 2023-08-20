from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from typing import Annotated, Any, TypeVar, get_args, get_origin
from uuid import uuid4

from pydantic import BaseModel, ValidationError, create_model
from pydantic._internal._typing_extra import eval_type_lenient
from socketio import AsyncNamespace  # type: ignore

from app.common.sockets import AbortException, Ack
from app.twex.twex_db import Twex, TwexStatus

T = TypeVar("T")
AnyCallable = Callable[..., Any]
LocalNS = dict[str, Any]


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


def decode_annotation(annotation: Any) -> tuple[Any, Any] | tuple[None, None]:
    if get_origin(annotation) is Annotated:
        annotation_args = get_args(annotation)
        if len(annotation_args) == 2:
            return annotation_args  # type: ignore[return-value]
    return None, None


class ExpandableArgument:
    def __init__(self, base: type[BaseModel]) -> None:
        self.base = base
        self.fields: dict[str, tuple[type, Any]] = {}
        self.depends: dict[str, set[Dependency | None]] = {}

    def add_field(
        self, name: str, type_: Any, default: Any, dependency: Dependency | None = None
    ) -> None:
        # TODO validate repeat's type
        if default is Parameter.empty:
            default = ...
        self.fields[name] = (type_, default)
        self.depends.setdefault(name, set()).add(dependency)

    def convert(self) -> type[BaseModel]:
        return create_model(  # type: ignore[call-overload, no-any-return]
            f"{self.base.__qualname__}.Expanded",
            __base__=self.base,
            **self.fields,
        )

    def clean(self, result: BaseModel) -> BaseModel:
        return self.base.model_validate(
            result.model_dump(include=set(self.base.model_fields.keys()))
        )


class SessionID:
    pass


class Depends:
    def __init__(self, dependency: AnyCallable) -> None:
        self.dependency = dependency


class SignatureParser:
    def __init__(self, func: AnyCallable, local_ns: LocalNS | None = None) -> None:
        self.func: AnyCallable = func
        self.signature: Signature = signature(func)
        self.local_ns: LocalNS = local_ns or {}

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        pass

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        pass

    def parse_annotated_kwarg(self, param: Parameter, *args: Any) -> None:
        pass

    def parse(self) -> None:
        for param in self.signature.parameters.values():
            annotation: Any = param.annotation  # TODO type the annotation
            if isinstance(annotation, str):
                global_ns = getattr(self.func, "__globals__", {})
                annotation = eval_type_lenient(annotation, global_ns, self.local_ns)

            if param.kind == param.POSITIONAL_ONLY:
                self.parse_positional_only(param, annotation)
            elif isinstance(annotation, type):
                self.parse_typed_kwarg(param, annotation)
            elif get_origin(annotation) is Annotated:
                self.parse_annotated_kwarg(param, *get_args(annotation))
            else:
                raise NotImplementedError  # TODO errors


class Dependency(SignatureParser):
    def __init__(
        self,
        func: AnyCallable,
        local_ns: dict[str, Any],
        arg_types: list[type | ExpandableArgument],
        first_expandable_argument: ExpandableArgument | None,
        request_positions: list[tuple[AnyCallable, str]],
        sid_positions: list[tuple[AnyCallable, str]],
    ) -> None:
        super().__init__(func, local_ns)
        self.unresolved: dict[AnyCallable, str] = {}  # callable to param_name

        self.arg_types: list[type | ExpandableArgument] = arg_types
        self.first_expandable_argument: ExpandableArgument | None = (
            first_expandable_argument
        )
        self.request_positions: list[tuple[AnyCallable, str]] = request_positions
        self.sid_positions: list[tuple[AnyCallable, str]] = sid_positions

        # TODO postpone this more
        self.kwargs: dict[str, Any] = {}  # param_name to value (kwargs)

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise Exception("No positional args allowed for dependencies")  # TODO errors

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        if issubclass(type_, Request):
            self.request_positions.append((self.func, param.name))
        elif self.first_expandable_argument is None:
            # TODO better error message or auto-creation of first expandable
            raise Exception("No expandable arguments found")
        else:
            self.first_expandable_argument.add_field(
                param.name, type_, param.default, self
            )

    def parse_double_annotated_kwarg(
        self, param: Parameter, type_: Any, decoded: Any
    ) -> None:
        if isinstance(decoded, Depends):
            self.unresolved[decoded.dependency] = param.name
        elif isinstance(decoded, SessionID):
            self.sid_positions.append((self.func, param.name))
        elif isinstance(decoded, int):
            if len(self.arg_types) <= decoded:
                raise Exception(
                    f"Param {param} can't be saved to [{decoded}]: "
                    f"only {len(self.arg_types)} are present"
                )
            argument_type = self.arg_types[decoded]
            if isinstance(argument_type, ExpandableArgument):
                argument_type.add_field(param.name, type_, param.default, self)
            else:
                raise Exception(
                    f"Param {param} can't be saved to "
                    f"{argument_type} at [{decoded}]"
                )
        else:
            raise NotImplementedError(f"Parameter {type_} {decoded} not supported")

    def parse_annotated_kwarg(self, param: Parameter, *args: Any) -> None:
        if len(args) != 2:
            raise Exception("Annotated supported with 2 args only")
        self.parse_double_annotated_kwarg(param, *args)

    async def resolve(self, stack: AsyncExitStack) -> Any:  # TODO move out
        if len(self.unresolved) != 0:
            raise Exception("Not all sub-dependencies were resolved")

        if isasyncgenfunction(self.func):
            return await stack.enter_async_context(
                asynccontextmanager(self.func)(**self.kwargs)
            )
        elif isgeneratorfunction(self.func):
            return stack.enter_context(contextmanager(self.func)(**self.kwargs))
        return await call_or_await(self.func, **self.kwargs)


class Request(Dependency):
    def __init__(self, handler: Callable[..., Ack], ns: type | None = None):
        super().__init__(
            func=handler,
            arg_types=[],
            first_expandable_argument=None,
            request_positions=[],
            sid_positions=[],
            local_ns={} if ns is None else dict(ns.__dict__),
        )

        # TODO use self.unresolved
        self.dependencies: dict[str, Dependency] = {}  # TODO keys as qualnames
        self.func_to_dep: dict[AnyCallable, Dependency] = {
            self.func: self,
        }

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        if issubclass(ann, BaseModel):
            expandable_argument = ExpandableArgument(ann)
            if self.first_expandable_argument is None:
                self.first_expandable_argument = expandable_argument
            self.arg_types.append(expandable_argument)
        else:
            self.arg_types.append(ann)

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        if issubclass(type_, Request):
            self.request_positions.append((self.func, param.name))
        elif self.first_expandable_argument is None:
            raise Exception("No expandable arguments found")
        else:
            self.first_expandable_argument.add_field(param.name, type_, param.default)

    def parse_double_annotated_kwarg(
        self, param: Parameter, type_: Any, decoded: Any
    ) -> None:
        if isinstance(decoded, Depends):
            self.dependencies[param.name] = Dependency(
                decoded.dependency,
                self.local_ns,
                self.arg_types,
                self.first_expandable_argument,
                self.request_positions,
                self.sid_positions,
            )
            self.dependencies[param.name].parse()
            self.func_to_dep[decoded.dependency] = self.dependencies[param.name]
        elif isinstance(decoded, SessionID):
            self.sid_positions.append((self.func, param.name))
        elif isinstance(decoded, int):
            if len(self.arg_types) <= decoded:
                raise Exception(
                    f"Param {param} can't be saved to [{decoded}]: "
                    f"only {len(self.arg_types)} are present"
                )
            argument_type = self.arg_types[decoded]
            if isinstance(argument_type, ExpandableArgument):
                argument_type.add_field(param.name, type_, param.default)
            else:
                raise Exception(
                    f"Param {param} can't be saved to "
                    f"{argument_type} at [{decoded}]"
                )
        else:
            raise NotImplementedError(f"Parameter {type_} {decoded} not supported")

    async def execute(
        self, event_name: str, sid: str, *arguments: Any
    ) -> Ack | None:  # TODO move out
        # TODO use *actual* Request data-object
        # checking client arguments (positional-only because socketio)
        if len(self.arg_types) != len(arguments):
            return Ack(
                code=422,
                data=f"Required {len(self.arg_types)}, but {len(arguments)} given",
            )

        field_types = [
            arg_type.convert() if isinstance(arg_type, ExpandableArgument) else arg_type
            for arg_type in self.arg_types
        ]
        # RootModel(tuple(arg_types))
        arg_model = create_model(  # type: ignore[call-overload]
            "InputModel", **{str(i): (ann, ...) for i, ann in enumerate(field_types)}
        )

        try:
            converted = arg_model.model_validate(
                {str(i): ann for i, ann in enumerate(arguments)}
            )
            args: list[Any] = []
            kwargs: dict[str, Any] = {}  # qualname to value
            for i, arg_type in enumerate(self.arg_types):
                if isinstance(arg_type, ExpandableArgument):
                    result: BaseModel = getattr(converted, str(i))
                    args.append(arg_type.clean(result))
                    for field_name in arg_type.fields:
                        value = getattr(result, field_name)
                        for dependency in arg_type.depends[field_name]:
                            if dependency is None:
                                kwargs[field_name] = value
                            else:
                                dependency.kwargs[field_name] = value
                else:
                    args.append(arg_type)
        except (ValidationError, AttributeError) as e:
            return Ack(code=422, data=str(e))

        for func, field_name in self.request_positions:
            self.func_to_dep[func].kwargs[field_name] = self

        for func, field_name in self.sid_positions:
            self.func_to_dep[func].kwargs[field_name] = sid

        kwargs.update(self.kwargs)  # TODO remove after postponing

        try:
            async with AsyncExitStack() as stack:
                # resolving dependencies
                while len(self.dependencies) != 0:
                    layer: list[
                        tuple[AnyCallable, str]
                    ] = []  # callables and param_names of the layer
                    for name, dependency in self.dependencies.items():
                        for resolved, param_name in layer:
                            dep_param_name = dependency.unresolved.pop(resolved, None)
                            if dep_param_name is not None:
                                dependency.kwargs[dep_param_name] = kwargs[param_name]
                        layer = []
                        if len(dependency.unresolved) == 0:
                            layer.append((dependency.func, name))
                            kwargs[name] = await dependency.resolve(stack)
                    for _, name in layer:
                        self.dependencies.pop(name)

                # call the function
                return await call_or_await(self.func, *args, **kwargs)
            # this code is, in fact, reachable
            # noinspection PyUnreachableCode
            return None  # TODO `with` above can lead to no return
        except AbortException as e:
            return e.ack


Sid = Annotated[str, SessionID()]


def twex_with_status(statuses: set[TwexStatus]) -> Callable[..., Awaitable[Twex]]:
    async def twex_with_status_inner(file_id: str) -> Twex:
        return await Twex.find_with_status(file_id=file_id, statuses=statuses)

    return twex_with_status_inner


class MainNamespace(AsyncNamespace):  # type: ignore
    async def trigger_event(self, event: str, *args: Any) -> dict[str, Any] | None:
        handler_name = f"on_{event}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            return None

        request = Request(handler, ns=type(self))
        request.parse()
        result = await request.execute(event, *args)
        return None if result is None else result.model_dump()

    async def on_connect(self, sid: Sid) -> None:
        logging.warning(f"Connected to {sid}")

    class CreateArgs(BaseModel):
        file_name: str

    async def on_create(self, args: CreateArgs, /, sid: Sid) -> Ack:
        twex = Twex(file_name=args.file_name)
        await twex.save()

        self.enter_room(sid=sid, room=f"{twex.file_id}-publishers")
        return Ack(code=201, data={"file_id": twex.file_id})

    class FileIdArgs(BaseModel):
        file_id: str

    class SubscribeArgs(BaseModel):
        pass

    async def on_subscribe(
        self,
        args: SubscribeArgs,
        /,
        sid: Sid,
        twex: Annotated[Twex, Depends(twex_with_status({TwexStatus.OPEN}))],
    ) -> Ack:
        await twex.update_status(new_status=TwexStatus.FULL)
        # TODO more control over FULL for non-dialog twexes

        self.enter_room(sid=sid, room=f"{twex.file_id}-subscribers")
        await self.emit(
            event="subscribe",
            data={**args.model_dump(), "file_id": twex.file_id},
            room=f"{twex.file_id}-publishers",
            skip_sid=sid,
        )
        return Ack(code=200, data=twex)

    class SendArgs(FileIdArgs):
        chunk: bytes

    async def on_send(self, args: SendArgs, /, sid: Sid) -> Ack:
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

    async def on_confirm(self, args: ConfirmArgs, /, sid: Sid) -> Ack:
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

    async def on_finish(self, args: FinishArgs, /, sid: Sid) -> Ack:
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
