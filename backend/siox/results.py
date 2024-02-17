from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from siox.exceptions import EventException
from siox.markers import Marker
from siox.packagers import ErrorPackager, Packager
from siox.request import RequestData
from siox.types import DataOrTuple

T = TypeVar("T")


class ExpandedArgument:
    def __init__(
        self, base: type[BaseModel], destinations: dict[str, list[Runnable]]
    ) -> None:
        self.base = base
        self.destinations = destinations

    def clean(self, result: BaseModel) -> BaseModel:
        return self.base.model_validate(
            result.model_dump(include=set(self.base.model_fields.keys()))
        )

    def fulfill_destinations(self, result: BaseModel) -> None:
        for field_name, destinations in self.destinations.items():
            value = getattr(result, field_name)
            for destination in destinations:
                destination.kwargs[field_name] = value


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
        arg_types: list[type | ExpandedArgument],
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
            # TODO remove instance check & properly support non-pydantic arguments
            if isinstance(arg_type, ExpandedArgument):
                yield arg_type.clean(result)
                arg_type.fulfill_destinations(result)
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
