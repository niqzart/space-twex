from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterator
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
from siox.markers import Depends, SessionID, Sid
from siox.types import AnyCallable, LocalNS

T = TypeVar("T")


class ExpandableArgument:
    def __init__(self, base: type[BaseModel]) -> None:
        self.base = base
        self.fields: dict[str, tuple[type, Any]] = {}
        self.destinations: dict[str, list[Runnable]] = {}

    def add_field(
        self, name: str, type_: Any, default: Any, destination: Runnable
    ) -> None:
        if default is Parameter.empty:
            default = ...
        passed_field = type_, default
        existing_field = self.fields.get(name)
        if existing_field is None:
            self.fields[name] = passed_field
        elif existing_field != passed_field:
            raise NotImplementedError("Duplicate with a different type")  # TODO errors
        self.destinations.setdefault(name, []).append(destination)

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


class Runnable:
    def __init__(self, func: Callable[..., T | Awaitable[T]]) -> None:
        self.func = func
        self.args: tuple[Any, ...] = ()
        self.kwargs: dict[str, Any] = {}

    async def run(self) -> T:
        if iscoroutinefunction(self.func):
            return await self.func(*self.args, **self.kwargs)  # type: ignore[no-any-return]
        elif callable(self.func):
            return self.func(*self.args, **self.kwargs)  # type: ignore[return-value]
        raise Exception("Handler is not callable")


class Dependency(Runnable):
    def __init__(self, func: Callable[..., T | Awaitable[T]]) -> None:
        super().__init__(func)
        self.destinations: dict[Runnable, list[str]] = {}

    async def resolve(self, stack: AsyncExitStack) -> Any:  # TODO move out
        if isasyncgenfunction(self.func):
            return await stack.enter_async_context(
                asynccontextmanager(self.func)(*self.args, **self.kwargs)
            )
        elif isgeneratorfunction(self.func):
            return stack.enter_context(
                contextmanager(self.func)(*self.args, **self.kwargs)
            )
        return await self.run()


class ClientEvent:
    def __init__(
        self,
        context: SPContext,
        arg_model: type[BaseModel],
        arg_count: int,
        dependency_order: list[Dependency],
        destination: Runnable,
    ):
        self.context = context
        self.arg_model = arg_model
        self.arg_count = arg_count
        self.dependency_order = dependency_order
        self.destination = destination

    def parse_arguments(self, arguments: tuple[Any, ...]) -> Iterator[Any]:
        # TODO argument parser
        converted = self.arg_model.model_validate(
            {str(i): ann for i, ann in enumerate(arguments)}
        )
        for i, arg_type in enumerate(self.context.arg_types):
            result: Any = getattr(converted, str(i))
            if isinstance(arg_type, ExpandableArgument):
                yield arg_type.clean(result)
                for field_name, destinations in arg_type.destinations.items():
                    value = getattr(result, field_name)
                    for destination in destinations:
                        destination.kwargs[field_name] = value
            else:
                yield result

    async def execute(self, event_name: str, sid: str, *arguments: Any) -> Ack | None:
        # TODO use *actual* Request data-object
        # TODO split into files
        if len(arguments) != self.arg_count:
            return Ack(
                code=422,
                data=f"Required {self.arg_count}, but {len(arguments)} given",
            )

        try:
            self.destination.args = tuple(self.parse_arguments(arguments))
        except (ValidationError, AttributeError) as e:
            return Ack(code=422, data=str(e))

        for value, destinations in (  # TODO more expandability
            (self, self.context.request_positions),
            (sid, self.context.sid_positions),
        ):
            for destination, field_name in destinations:
                destination.kwargs[field_name] = value

        try:
            async with AsyncExitStack() as stack:
                for dependency in self.dependency_order:
                    value = await dependency.resolve(stack)
                    for destination, field_names in dependency.destinations.items():
                        for field_name in field_names:
                            destination.kwargs[field_name] = value

                # call the function
                return await self.destination.run()
            # this code is, in fact, reachable
            # noinspection PyUnreachableCode
            return None  # TODO `with` above can lead to no return
        except AbortException as e:
            return e.ack


class SPContext(BaseModel, arbitrary_types_allowed=True):
    arg_types: list[type | ExpandableArgument] = []
    first_expandable_argument: ExpandableArgument | None = None
    request_positions: list[tuple[Runnable, str]] = []  # TODO move out
    sid_positions: list[tuple[Runnable, str]] = []  # TODO move out
    signatures: dict[AnyCallable, SignatureParser] = {}


class SignatureParser:
    def __init__(
        self,
        func: Callable[..., T | Awaitable[T]],
        context: SPContext,
        local_ns: LocalNS | None = None,
        destination: Runnable | None = None,
    ) -> None:
        self.func = func
        self.signature: Signature = signature(func)
        self.local_ns: LocalNS = local_ns or {}
        self.destination: Runnable = destination or Runnable(func)
        self.context: SPContext = context
        self.unresolved: set[AnyCallable] = set()

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise NotImplementedError

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        if issubclass(type_, RequestSignature):
            self.context.request_positions.append((self.destination, param.name))
        elif self.context.first_expandable_argument is None:
            # TODO better error message or auto-creation of first expandable
            raise Exception("No expandable arguments found")
        else:
            self.context.first_expandable_argument.add_field(
                param.name, type_, param.default, self.destination
            )

    def parse_double_annotated_kwarg(
        self, param: Parameter, type_: Any, decoded: Any
    ) -> None:
        if isinstance(decoded, Depends):
            dependency = self.context.signatures.get(decoded.dependency)
            if dependency is None:
                dependency = DependencySignature(
                    decoded.dependency,
                    self.local_ns,
                    self.context,
                )
                dependency.parse()
                self.context.signatures[decoded.dependency] = dependency
            elif not isinstance(dependency, DependencySignature):
                raise Exception(
                    f"Can't add destination to {type(dependency)}"
                )  # TODO errors
            dependency.the_dep.destinations.setdefault(self.destination, []).append(
                param.name
            )
            self.unresolved.add(decoded.dependency)
        elif isinstance(decoded, SessionID):
            self.context.sid_positions.append((self.destination, param.name))
        elif isinstance(decoded, int):
            if len(self.context.arg_types) <= decoded:
                raise Exception(
                    f"Param {param} can't be saved to [{decoded}]: "
                    f"only {len(self.context.arg_types)} are present"
                )
            argument_type = self.context.arg_types[decoded]
            if isinstance(argument_type, ExpandableArgument):
                argument_type.add_field(
                    param.name, type_, param.default, self.destination
                )
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


class DependencySignature(SignatureParser):
    def __init__(
        self,
        func: AnyCallable,
        local_ns: dict[str, Any],
        context: SPContext,
    ) -> None:
        self.the_dep: Dependency = Dependency(func)  # TODO naming
        super().__init__(func, context, local_ns, destination=self.the_dep)

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise Exception("No positional args allowed for dependencies")  # TODO errors


class RequestSignature(SignatureParser):
    def __init__(self, handler: Callable[..., Ack], ns: type | None = None):
        super().__init__(
            func=handler,
            context=SPContext(signatures={handler: self}),
            local_ns=ns and ns.__dict__,  # type: ignore[arg-type]  # assume bool(type) is True
        )

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        if issubclass(ann, BaseModel):
            expandable_argument = ExpandableArgument(ann)
            if self.context.first_expandable_argument is None:
                self.context.first_expandable_argument = expandable_argument
            self.context.arg_types.append(expandable_argument)
        else:
            self.context.arg_types.append(ann)

    def generate_positional_fields(self) -> Iterator[type]:
        for arg_type in self.context.arg_types:
            if isinstance(arg_type, ExpandableArgument):
                yield arg_type.convert()
            else:  # TODO remove the isinstance check
                yield arg_type

    def resolve_dependencies(self) -> Iterator[Dependency]:
        layer: list[SignatureParser]
        while len(self.unresolved) != 0:  # TODO errors for cycles in DR
            layer = [
                dependency
                for dependency in self.context.signatures.values()
                if len(dependency.unresolved) == 0
            ]
            yield from (
                dependency.the_dep
                for dependency in layer
                if isinstance(dependency, DependencySignature)
            )
            for signature in self.context.signatures.values():
                for resolved in layer:
                    signature.unresolved.discard(resolved.func)

    def extract(self) -> ClientEvent:
        self.parse()
        return ClientEvent(
            context=self.context,
            arg_model=create_model(  # type: ignore[call-overload]
                "InputModel",  # TODO model name from event & namespace(?)
                **{
                    str(i): (ann, ...)
                    for i, ann in enumerate(self.generate_positional_fields())
                },
            ),
            arg_count=len(self.context.arg_types),
            dependency_order=list(self.resolve_dependencies()),
            destination=self.destination,
        )


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

        request = RequestSignature(handler, ns=type(self))
        client_event = request.extract()
        result = await client_event.execute(event, *args)
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
