from collections.abc import AsyncIterator

import pytest
from faker import Faker

from app.main import sio as sio_server
from tests.testing import AsyncSIOTestClient, AsyncSIOTestServer


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(scope="session")
async def server() -> AsyncIterator[AsyncSIOTestServer]:
    with AsyncSIOTestServer(server=sio_server).mock() as server:
        yield server


@pytest.fixture(scope="session")
async def sender(server: AsyncSIOTestServer) -> AsyncIterator[AsyncSIOTestClient]:
    async with server.client() as client:
        yield client


@pytest.fixture(scope="session")
async def receiver(server: AsyncSIOTestServer) -> AsyncIterator[AsyncSIOTestClient]:
    async with server.client() as client:
        yield client


@pytest.fixture()
def file_name(faker: Faker) -> str:
    return faker.file_name()  # type: ignore
