from base64 import b64encode

import pytest
from faker import Faker

from app.main import tmex
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
    tmex.backend.enter_room(sender.sid, f"{source_twex.file_id}-publishers")
    return sender


@pytest.fixture()
async def roomed_receiver(
    receiver: AsyncSIOTestClient,
    source_twex: Twex,
) -> AsyncSIOTestClient:
    tmex.backend.enter_room(receiver.sid, f"{source_twex.file_id}-subscribers")
    return receiver


@pytest.fixture()
async def chunk(faker: Faker) -> str:
    return b64encode(faker.binary(length=24)).decode("utf-8")
