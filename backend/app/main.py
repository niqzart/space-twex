from socketio import ASGIApp  # type: ignore

from app.twex import twex_sio
from siox.routing import TMEXIO

tmex = TMEXIO(async_mode="asgi")
tmex.include_router(twex_sio.router)

app = ASGIApp(socketio_server=tmex.backend)
