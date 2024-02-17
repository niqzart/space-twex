import pytest

from app.twex.twex_db import Twex, TwexStatus
from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_successful_create(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    file_name: str,
) -> None:
    code_create, ack_create = await sender.emit("create", {"file_name": file_name})
    assert code_create == 201
    assert isinstance(file_id := ack_create.get("file_id"), str)

    twex = await Twex.find_one(file_id)
    assert twex.file_id == file_id
    assert twex.file_name == file_name
    assert twex.status == TwexStatus.OPEN

    assert sender.event_count() == 0
    assert receiver.event_count() == 0


@pytest.mark.anyio
async def test_bad_data_create(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
) -> None:
    code_create, _ = await sender.emit("create", {})
    assert code_create == 422

    assert sender.event_count() == 0
    assert receiver.event_count() == 0
