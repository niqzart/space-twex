from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from inspect import (
    Parameter,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
)
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError, create_model

from siox.exceptions import EventException
from siox.markers import Marker
from siox.packagers import ErrorPackager, Packager
from siox.request import RequestData
from siox.types import DataOrTuple

T = TypeVar("T")


class ExpandableArgument:  # TODO split into this & argument parser
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

    async def resolve(self, stack: AsyncExitStack) -> Any:
        if isasyncgenfunction(self.func):
            return await stack.enter_async_context(
                asynccontextmanager(self.func)(*self.args, **self.kwargs)
            )
        elif isgeneratorfunction(self.func):
            return stack.enter_context(
                contextmanager(self.func)(*self.args, **self.kwargs)
            )
        return await self.run()


class MarkerDestinations:
    def __init__(self) -> None:
        self.destinations: dict[Marker[Any], list[tuple[Runnable, str]]] = {}

    def add_destination(
        self,
        marker: Marker[Any],
        destination: Runnable,
        field_name: str,
    ) -> None:
        destinations = self.destinations.setdefault(marker, [])
        destinations.append((destination, field_name))

    def fill_all(self, request: RequestData) -> None:
        for marker, destinations in self.destinations.items():
            value: Any = marker.extract(request)
            for destination, field_name in destinations:
                destination.kwargs[field_name] = value


class ClientHandler:
    def __init__(
        self,
        marker_destinations: MarkerDestinations,
        arg_model: type[BaseModel],
        arg_types: list[type | ExpandableArgument],
        arg_count: int,
        dependency_order: list[Dependency],
        runnable: Runnable,
        result_packager: Packager,
        error_packager: ErrorPackager,
    ):
        self.marker_destinations = marker_destinations
        self.arg_model = arg_model
        self.arg_types = arg_types
        self.arg_count = arg_count
        self.dependency_order = dependency_order
        self.runnable = runnable
        self.result_packager = result_packager
        self.error_packager = error_packager

    def parse_arguments(self, arguments: tuple[Any, ...]) -> Iterator[Any]:
        converted = self.arg_model.model_validate(
            {str(i): ann for i, ann in enumerate(arguments)}
        )
        for i, arg_type in enumerate(self.arg_types):
            result: Any = getattr(converted, str(i))
            if isinstance(arg_type, ExpandableArgument):
                yield arg_type.clean(result)
                for field_name, destinations in arg_type.destinations.items():
                    value = getattr(result, field_name)
                    for destination in destinations:
                        destination.kwargs[field_name] = value
            else:
                yield result

    async def handle(self, request: RequestData) -> DataOrTuple:
        if len(request.arguments) != self.arg_count:
            return self.error_packager.pack_error(
                EventException(
                    422,
                    f"Event requires exactly {self.arg_count} arguments, "
                    f"but {len(request.arguments)} arguments were received",
                )
            )

        try:
            self.runnable.args = tuple(self.parse_arguments(request.arguments))
        except (ValidationError, AttributeError) as e:
            return self.error_packager.pack_error(EventException(422, str(e)))

        self.marker_destinations.fill_all(request)

        try:
            async with AsyncExitStack() as stack:
                for dependency in self.dependency_order:
                    value = await dependency.resolve(stack)
                    for destination, field_names in dependency.destinations.items():
                        for field_name in field_names:
                            destination.kwargs[field_name] = value

                # call the function
                return self.result_packager.pack(await self.runnable.run())
            # this code is, in fact, reachable
            # noinspection PyUnreachableCode
            return None  # TODO `with` above can lead to no return
        except EventException as e:
            return self.error_packager.pack_error(e)
