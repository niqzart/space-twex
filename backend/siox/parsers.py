from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from inspect import Parameter, Signature, signature
from typing import Annotated, Any, TypeVar, get_args, get_origin

from pydantic import BaseModel, create_model
from pydantic._internal._typing_extra import eval_type_lenient

from app.common.sockets import Ack
from siox.emitters import DuplexEmitter, ServerEmitter
from siox.markers import (
    AsyncServerMarker,
    AsyncSocketMarker,
    Depends,
    DuplexEmitterMarker,
    Marker,
    ServerEmitterMarker,
)
from siox.results import (
    ClientEvent,
    Dependency,
    ExpandableArgument,
    MarkerDestinations,
    Runnable,
)
from siox.socket import AsyncServer, AsyncSocket
from siox.types import AnyCallable, LocalNS

T = TypeVar("T")


class SPContext(BaseModel, arbitrary_types_allowed=True):
    arg_types: list[type | ExpandableArgument] = []
    first_expandable_argument: ExpandableArgument | None = None
    signatures: dict[AnyCallable, SignatureParser] = {}


class SignatureParser:
    def __init__(
        self,
        func: Callable[..., T | Awaitable[T]],
        context: SPContext,
        marker_destinations: MarkerDestinations,
        local_ns: LocalNS | None = None,
        destination: Runnable | None = None,
    ) -> None:
        self.func = func
        self.signature: Signature = signature(func)
        self.local_ns: LocalNS = local_ns or {}
        self.destination: Runnable = destination or Runnable(func)
        self.context: SPContext = context
        self.marker_destinations = marker_destinations
        self.unresolved: set[AnyCallable] = set()

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise NotImplementedError

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        if issubclass(type_, AsyncSocket):
            self.marker_destinations.add_destination(
                AsyncSocketMarker(), self.destination, param.name
            )
        elif issubclass(type_, AsyncServer):
            self.marker_destinations.add_destination(
                AsyncServerMarker(), self.destination, param.name
            )
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
                    self.context,
                    self.marker_destinations,
                    self.local_ns,
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
        elif isinstance(decoded, Marker):
            self.marker_destinations.add_destination(
                decoded, self.destination, param.name
            )
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
        if len(args) == 0 or not isinstance(args[0], type):
            raise Exception("Incorrect Annotated, first arg must be a type")
        if (
            issubclass(args[0], DuplexEmitter)
            and len(args) == 2
            and isinstance(args[1], type)
            and issubclass(args[1], BaseModel)
        ):
            args = args[0], DuplexEmitterMarker(args[1])
        elif (
            issubclass(args[0], ServerEmitter)
            and len(args) == 3
            and isinstance(args[1], type)
            and issubclass(args[1], BaseModel)
            and isinstance(args[2], str)
        ):
            args = args[0], ServerEmitterMarker(model=args[1], name=args[2])
        if len(args) == 2:
            self.parse_double_annotated_kwarg(param, *args)
        else:
            raise NotImplementedError(f"Parameter Annotated[{args}] not supported")

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
        context: SPContext,
        marker_destinations: MarkerDestinations,
        local_ns: dict[str, Any],
    ) -> None:
        self.the_dep: Dependency = Dependency(func)  # TODO naming
        super().__init__(
            func, context, marker_destinations, local_ns, destination=self.the_dep
        )

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise Exception("No positional args allowed for dependencies")  # TODO errors


class RequestSignature(SignatureParser):
    def __init__(self, handler: Callable[..., Ack], ns: type | None = None):
        super().__init__(
            func=handler,
            context=SPContext(signatures={handler: self}),
            marker_destinations=MarkerDestinations(),
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
            marker_destinations=self.marker_destinations,
            arg_model=create_model(  # type: ignore[call-overload]
                "InputModel",  # TODO model name from event & namespace(?)
                **{
                    str(i): (ann, ...)
                    for i, ann in enumerate(self.generate_positional_fields())
                },
            ),
            arg_types=self.context.arg_types,
            arg_count=len(self.context.arg_types),
            dependency_order=list(self.resolve_dependencies()),
            destination=self.destination,
        )
