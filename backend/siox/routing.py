from collections.abc import Callable
from typing import Any

from socketio import AsyncManager, AsyncServer  # type: ignore[import]

from siox.parsers import RequestSignatureParser
from siox.results import ClientHandler


class EventRouter:
    def __init__(self, namespace: str | None = None) -> None:
        self.namespace = namespace
        self.client_handlers: dict[str, ClientHandler] = {}

    def on(
        self,
        event_name: str,
        # TODO summary: str | None = None,
        # TODO description: str | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def on_inner(function: Callable[..., Any]) -> Callable[..., Any]:
            request_signature_parser = RequestSignatureParser(function)
            self.client_handlers[event_name] = request_signature_parser.extract()
            return function

        return on_inner


class TMEXIO:
    def __init__(
        self,
        client_manager: AsyncManager | None = None,
        logger: bool = False,
        json: Any = None,
        always_connect: bool = False,
        namespaces: list[str] | None = None,
        **kwargs: Any
    ) -> None:
        super().__init__()
        self.backend = AsyncServer(
            client_manager=client_manager,
            logger=logger,
            json=json,
            always_connect=always_connect,
            namespaces=namespaces,
            **kwargs,
        )

    def include_router(self, router: EventRouter, namespace: str | None = None) -> None:
        namespace = namespace or router.namespace
        for event_name, handler in router.client_handlers.items():
            self.backend.on(
                event=event_name,
                handler=handler.build_handler(self.backend, event_name),
                namespace=namespace,
            )
