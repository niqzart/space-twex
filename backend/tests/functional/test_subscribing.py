from typing import Any

import pytest

from app.twex.twex_db import Twex, TwexStatus
from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_successful_subscribe(
    roomed_sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    source_twex: Twex,
) -> None:
    ack_subscribe = await receiver.emit("subscribe", {"file_id": source_twex.file_id})
    assert ack_subscribe.get("code") == 200
    assert isinstance(ack_subscribe_data := ack_subscribe.get("data", None), dict)
    assert ack_subscribe_data.get("file_name") == source_twex.file_name

    event_subscribe = roomed_sender.event_pop("subscribe")
    assert isinstance(event_subscribe, dict)
    assert event_subscribe.get("file_id") == source_twex.file_id

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex.file_id == source_twex.file_id
    assert result_twex.file_name == source_twex.file_name
    assert result_twex.status == TwexStatus.FULL

    assert roomed_sender.event_count() == 0
    assert receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("data", "code"),
    [
        pytest.param({}, 422, id="bad_data"),
        pytest.param({"file_id": ""}, 404, id="not_found"),
    ],
)
async def test_bad_data_subscribe(
    roomed_sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    source_twex: Twex,
    data: dict[str, Any],
    code: int,
) -> None:
    ack_subscribe = await receiver.emit("subscribe", data)
    assert ack_subscribe.get("code") == code

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    "status",
    [
        TwexStatus.FULL,
        TwexStatus.SENT,
        TwexStatus.CONFIRMED,
        TwexStatus.FINISHED,
    ],
)
async def test_wrong_status_subscribe(
    roomed_sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    source_twex: Twex,
    status: TwexStatus,
) -> None:
    await source_twex.update_status(status)

    ack_subscribe = await receiver.emit("subscribe", {"file_id": source_twex.file_id})
    assert ack_subscribe.get("code") == 400
    assert ack_subscribe.get("data") == f"Wrong status: {status.value}"

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert receiver.event_count() == 0
