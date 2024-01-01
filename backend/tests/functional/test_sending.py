from typing import Any

import pytest

from app.twex.twex_db import Twex, TwexStatus
from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
@pytest.mark.parametrize("stating_status", [TwexStatus.FULL, TwexStatus.CONFIRMED])
async def test_successful_send(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    chunk: str,
    stating_status: TwexStatus,
) -> None:
    await source_twex.update_status(stating_status)

    ack_send = await roomed_sender.emit(
        "send", {"file_id": source_twex.file_id, "chunk": chunk}
    )
    assert ack_send.get("code") == 200
    assert (chunk_id := ack_send.get("chunk_id")) is not None

    event_send = roomed_receiver.event_pop("send")
    assert isinstance(event_send, dict)
    assert event_send.get("chunk") == chunk
    assert event_send.get("chunk_id") == chunk_id
    assert event_send.get("file_id") == source_twex.file_id

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex.file_id == source_twex.file_id
    assert result_twex.file_name == source_twex.file_name
    assert result_twex.status == TwexStatus.SENT

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("data", "code"),
    [
        pytest.param({}, 422, id="no_data"),
        pytest.param({"chunk": b""}, 422, id="only_chunk"),
        pytest.param({"file_id": ""}, 422, id="only_file_id"),
        pytest.param({"file_id": "", "chunk": b""}, 404, id="not_found"),
    ],
)
async def test_bad_data_send(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    data: dict[str, Any],
    code: int,
) -> None:
    ack_subscribe = await roomed_sender.emit("send", data)
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
        TwexStatus.SENT,
        TwexStatus.FINISHED,
    ],
)
async def test_wrong_status_send(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    chunk: str,
    status: TwexStatus,
) -> None:
    await source_twex.update_status(status)

    ack_send = await roomed_sender.emit(
        "send", {"file_id": source_twex.file_id, "chunk": chunk}
    )
    assert ack_send.get("code") == 400
    assert ack_send.get("reason") == f"Wrong status: {status.value}"

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0
