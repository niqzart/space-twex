from typing import Any

import pytest

from app.twex.twex_db import Twex, TwexStatus
from siox.exceptions import EventException
from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_successful_finish(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    chunk: str,
) -> None:
    await source_twex.update_status(TwexStatus.CONFIRMED)

    ack_finish = await roomed_sender.emit("finish", {"file_id": source_twex.file_id})
    assert ack_finish.get("code") == 204

    event_finish = roomed_receiver.event_pop("finish")
    assert isinstance(event_finish, dict)
    assert event_finish.get("file_id") == source_twex.file_id

    with pytest.raises(EventException):
        await Twex.find_one(source_twex.file_id)

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("data", "code"),
    [
        pytest.param({}, 422, id="bad_data"),
        pytest.param({"file_id": ""}, 404, id="not_found"),
    ],
)
async def test_bad_data_finish(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    data: dict[str, Any],
    code: int,
) -> None:
    ack_subscribe = await roomed_sender.emit("finish", data)
    assert ack_subscribe.get("code") == code

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    "status",
    [
        TwexStatus.OPEN,
        TwexStatus.FULL,
        TwexStatus.SENT,
        TwexStatus.FINISHED,
    ],
)
async def test_wrong_status_finish(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    status: TwexStatus,
) -> None:
    await source_twex.update_status(status)

    ack_send = await roomed_sender.emit("finish", {"file_id": source_twex.file_id})
    assert ack_send.get("code") == 400
    assert ack_send.get("reason") == f"Wrong status: {status.value}"

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0
