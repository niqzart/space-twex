from typing import Any
from uuid import uuid4

import pytest

from app.twex.twex_db import Twex, TwexStatus
from tests.testing import AsyncSIOTestClient


@pytest.fixture()
def chunk_id() -> str:
    return uuid4().hex


@pytest.mark.anyio
async def test_successful_confirm(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    chunk_id: str,
    chunk: str,
) -> None:
    await source_twex.update_status(TwexStatus.SENT)

    code_confirm, ack_confirm = await roomed_receiver.emit(
        "confirm", {"file_id": source_twex.file_id, "chunk_id": chunk_id}
    )
    assert code_confirm == 204
    assert ack_confirm is None

    event_confirm = roomed_sender.event_pop("confirm")
    assert isinstance(event_confirm, dict)
    assert event_confirm.get("chunk_id") == chunk_id
    assert event_confirm.get("file_id") == source_twex.file_id

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex.file_id == source_twex.file_id
    assert result_twex.file_name == source_twex.file_name
    assert result_twex.status == TwexStatus.CONFIRMED

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("data", "code"),
    [
        pytest.param({}, 422, id="no_data"),
        pytest.param({"chunk_id": ""}, 422, id="no_file_id"),
        pytest.param({"file_id": ""}, 422, id="no_chunk_id"),
        pytest.param({"file_id": "", "chunk_id": ""}, 404, id="not_found"),
    ],
)
async def test_bad_data_confirm(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    data: dict[str, Any],
    code: int,
) -> None:
    code_subscribe, _ = await roomed_receiver.emit("confirm", data)
    assert code_subscribe == code

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
        TwexStatus.CONFIRMED,
        TwexStatus.FINISHED,
    ],
)
async def test_wrong_status_send(
    roomed_sender: AsyncSIOTestClient,
    roomed_receiver: AsyncSIOTestClient,
    source_twex: Twex,
    chunk_id: str,
    status: TwexStatus,
) -> None:
    await source_twex.update_status(status)

    code_send, ack_send = await roomed_receiver.emit(
        "confirm", {"file_id": source_twex.file_id, "chunk_id": chunk_id}
    )
    assert code_send == 400
    assert ack_send.get("reason") == f"Wrong status: {status.value}"

    result_twex = await Twex.find_one(source_twex.file_id)
    assert result_twex == source_twex

    assert roomed_sender.event_count() == 0
    assert roomed_receiver.event_count() == 0
