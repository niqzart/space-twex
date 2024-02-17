from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from inspect import Parameter, Signature, signature
from typing import Annotated, Any, TypeVar, get_args, get_origin

from pydantic import BaseModel, create_model
from pydantic._internal._typing_extra import eval_type_lenient

from siox.emitters import DuplexEmitter, ServerEmitter
from siox.markers import (
    AsyncServerMarker,
    AsyncSocketMarker,
    Depends,
    DuplexEmitterMarker,
    Marker,
    ServerEmitterMarker,
)
from siox.packagers import BasicErrorPackager, NoopPackager, Packager, PydanticPackager
from siox.results import (
    ClientHandler,
    Dependency,
    ExpandedArgument,
    MarkerDestinations,
    Runnable,
)
from siox.socket import AsyncServer, AsyncSocket
from siox.types import AnyCallable, LocalNS

T = TypeVar("T")


class ExpandablePydanticModel:
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

    def extract(self) -> ExpandedArgument:
        return ExpandedArgument(
            base=self.base,
            destinations=self.destinations,
        )


class SPContext(BaseModel, arbitrary_types_allowed=True):
    arg_types: list[type | ExpandablePydanticModel] = []
    first_expandable_argument: ExpandablePydanticModel | None = None
    signatures: dict[AnyCallable, SignatureParser] = {}


class SignatureParser:
    def __init__(
        self,
        func: Callable[..., T | Awaitable[T]],
        context: SPContext,
        marker_destinations: MarkerDestinations,
        local_ns: LocalNS | None = None,
        runnable: Runnable | None = None,
    ) -> None:
        self.func = func
        self.signature: Signature = signature(func)
        self.local_ns: LocalNS = local_ns or {}
        self.runnable: Runnable = runnable or Runnable(func)
        self.context: SPContext = context
        self.marker_destinations = marker_destinations
        self.unresolved: set[AnyCallable] = set()

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise NotImplementedError

    def parse_typed_kwarg(self, param: Parameter, type_: type) -> None:
        if issubclass(type_, AsyncSocket):
            self.marker_destinations.add_destination(
                AsyncSocketMarker(), self.runnable, param.name
            )
        elif issubclass(type_, AsyncServer):
            self.marker_destinations.add_destination(
                AsyncServerMarker(), self.runnable, param.name
            )
        elif self.context.first_expandable_argument is None:
            # TODO better error message or auto-creation of first expandable
            raise Exception("No expandable arguments found")
        else:
            self.context.first_expandable_argument.add_field(
                param.name, type_, param.default, self.runnable
            )

    def parse_double_annotated_kwarg(
        self, param: Parameter, type_: Any, decoded: Any
    ) -> None:
        if isinstance(decoded, Depends):
            dependency_signature = self.context.signatures.get(decoded.dependency)
            if dependency_signature is None:
                dependency_signature = DependencySignatureParser(
                    decoded.dependency,
                    self.context,
                    self.marker_destinations,
                    self.local_ns,
                )
                dependency_signature.parse()
                self.context.signatures[decoded.dependency] = dependency_signature
            elif not isinstance(dependency_signature, DependencySignatureParser):
                raise Exception(
                    f"Can't add destination to {type(dependency_signature)}"
                )  # TODO errors
            dependency_signature.dependency.destinations.setdefault(
                self.runnable, []
            ).append(param.name)
            self.unresolved.add(decoded.dependency)
        elif isinstance(decoded, Marker):
            self.marker_destinations.add_destination(decoded, self.runnable, param.name)
        elif isinstance(decoded, int):
            if len(self.context.arg_types) <= decoded:
                raise Exception(
                    f"Param {param} can't be saved to [{decoded}]: "
                    f"only {len(self.context.arg_types)} are present"
                )
            argument_type = self.context.arg_types[decoded]
            if isinstance(argument_type, ExpandablePydanticModel):
                argument_type.add_field(param.name, type_, param.default, self.runnable)
            else:
                raise Exception(
                    f"Param {param} can't be saved to "
                    f"{argument_type} at [{decoded}]"
                )
        else:
            raise NotImplementedError(f"Parameter {type_} {decoded} not supported")

    def parse_packaged(self, arg: Any) -> Packager | None:
        if isinstance(arg, Packager):
            return arg
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return PydanticPackager(arg)
        return None

    def parse_annotated_kwarg(self, param: Parameter, *args: Any) -> None:
        if len(args) == 0 or not isinstance(args[0], type):
            raise Exception("Incorrect Annotated, first arg must be a type")
        if issubclass(args[0], DuplexEmitter) and len(args) == 2:
            packager = self.parse_packaged(args[1])
            if packager is not None:
                args = args[0], DuplexEmitterMarker(packager=packager)
        elif (
            issubclass(args[0], ServerEmitter)
            and len(args) == 3
            and isinstance(args[2], str)
        ):
            packager = self.parse_packaged(args[1])
            if packager is not None:
                args = args[0], ServerEmitterMarker(packager=packager, name=args[2])
        if len(args) == 2:
            self.parse_double_annotated_kwarg(param, *args)
        else:
            raise NotImplementedError(f"Parameter Annotated[{args}] not supported")

    def parse(self) -> None:
        for param in self.signature.parameters.values():
            annotation: Any = param.annotation
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


class DependencySignatureParser(SignatureParser):
    def __init__(
        self,
        func: AnyCallable,
        context: SPContext,
        marker_destinations: MarkerDestinations,
        local_ns: dict[str, Any],
    ) -> None:
        self.dependency: Dependency = Dependency(func)
        super().__init__(
            func, context, marker_destinations, local_ns, runnable=self.dependency
        )

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        raise Exception("No positional args allowed for dependencies")  # TODO errors


class RequestSignatureParser(SignatureParser):
    def __init__(self, handler: Callable[..., Any], ns: type | None = None):
        super().__init__(
            func=handler,
            context=SPContext(signatures={handler: self}),
            marker_destinations=MarkerDestinations(),
            local_ns=ns and ns.__dict__,  # type: ignore[arg-type]  # assume bool(type) is True
        )
        self.result_packager: Packager | None = None

    def parse_positional_only(self, param: Parameter, ann: Any) -> None:
        if issubclass(ann, BaseModel):
            expandable_argument = ExpandablePydanticModel(ann)
            if self.context.first_expandable_argument is None:
                self.context.first_expandable_argument = expandable_argument
            self.context.arg_types.append(expandable_argument)
        else:
            self.context.arg_types.append(ann)

    def generate_positional_fields(self) -> Iterator[type]:
        for arg_type in self.context.arg_types:
            if isinstance(arg_type, ExpandablePydanticModel):
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
                dependency.dependency
                for dependency in layer
                if isinstance(dependency, DependencySignatureParser)
            )
            for signature in self.context.signatures.values():
                for resolved in layer:
                    signature.unresolved.discard(resolved.func)

    def parse(self) -> None:
        super().parse()
        annotation: Any = self.signature.return_annotation  # TODO type the annotation
        if isinstance(annotation, str):
            global_ns = getattr(self.func, "__globals__", {})
            annotation = eval_type_lenient(annotation, global_ns, self.local_ns)

        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            if len(args) == 2:
                self.result_packager = self.parse_packaged(args[1])

    def extract(self) -> ClientHandler:
        self.parse()
        return ClientHandler(
            marker_destinations=self.marker_destinations,
            arg_model=create_model(  # type: ignore[call-overload]
                "InputModel",  # TODO model name from event & namespace(?)
                **{
                    str(i): (ann, ...)
                    for i, ann in enumerate(self.generate_positional_fields())
                },
            ),
            arg_types=[
                argument_type.extract()
                if isinstance(argument_type, ExpandablePydanticModel)
                else argument_type
                for argument_type in self.context.arg_types
            ],
            arg_count=len(self.context.arg_types),
            dependency_order=list(self.resolve_dependencies()),
            runnable=self.runnable,
            result_packager=self.result_packager or NoopPackager(),
            error_packager=BasicErrorPackager(),
        )
