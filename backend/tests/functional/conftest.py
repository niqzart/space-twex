import pytest

from app.main import sio
from app.twex.twex_db import Twex
from tests.testing import AsyncSIOTestClient


@pytest.fixture()
async def source_twex(file_name: str) -> Twex:
    twex = Twex(file_name=file_name)
    await twex.save()
    return twex


@pytest.fixture()
async def roomed_sender(
    sender: AsyncSIOTestClient,
    source_twex: Twex,
) -> AsyncSIOTestClient:
    sio.enter_room(sender.sid, f"{source_twex.file_id}-publishers")
    return sender
