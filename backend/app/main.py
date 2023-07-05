from socketio import ASGIApp, AsyncNamespace, AsyncServer  # type: ignore

from app.twex.twex_sio import MainNamespace

sio = AsyncServer(async_mode="asgi")
sio.register_namespace(MainNamespace("/"))
app = ASGIApp(socketio_server=sio)
