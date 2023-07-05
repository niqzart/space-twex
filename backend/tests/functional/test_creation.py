import pytest

from app.twex.twex_db import Twex, TwexStatus
from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_successful_create(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    file_name: str,
) -> None:
    ack_create = await sender.emit("create", {"file_name": file_name})
    assert ack_create.get("code") == 201
    assert isinstance(ack_create_data := ack_create.get("data"), dict)
    assert isinstance(file_id := ack_create_data.get("file_id"), str)

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
    ack_create = await sender.emit("create", {})
    assert ack_create.get("code") == 422

    assert sender.event_count() == 0
    assert receiver.event_count() == 0
