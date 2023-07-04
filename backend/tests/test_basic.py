import pytest
from faker import Faker

from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_basic(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    faker: Faker,
) -> None:
    file_name: str = faker.file_name()

    ack_create = await sender.emit("create", {"file_name": file_name})
    assert ack_create.get("code") == 201
    assert (file_id := ack_create.get("data", {}).get("file_id")) is not None

    ack_subscribe = await receiver.emit("subscribe", {"file_id": file_id})
    assert ack_subscribe.get("code") == 200
    assert isinstance(ack_subscribe_data := ack_subscribe.get("data", None), dict)
    assert ack_subscribe_data.get("file_name") == file_name

    event_subscribe = sender.event_pop("subscribe")
    assert isinstance(event_subscribe, dict)
    assert event_subscribe.get("file_id") == file_id

    chunk: bytes = faker.binary(length=24)
    ack_send = await sender.emit("send", {"file_id": file_id, "chunk": chunk})
    assert ack_send.get("code") == 200
    assert isinstance(ack_send_data := ack_send.get("data", None), dict)
    assert (chunk_id := ack_send_data.get("chunk_id")) is not None

    event_send = receiver.event_pop("send")
    assert isinstance(event_send, dict)
    assert event_send.get("chunk") == chunk
    assert event_send.get("chunk_id") == chunk_id
    assert event_send.get("file_id") == file_id

    ack_confirm = await receiver.emit(
        "confirm", {"file_id": file_id, "chunk_id": chunk_id}
    )
    assert ack_confirm.get("code") == 200

    event_confirm = sender.event_pop("confirm")
    assert isinstance(event_confirm, dict)
    assert event_confirm.get("chunk_id") == chunk_id
    assert event_confirm.get("file_id") == file_id

    ack_finish = await sender.emit("finish", {"file_id": file_id})
    assert ack_finish.get("code") == 200

    event_finish = receiver.event_pop("finish")
    assert isinstance(event_finish, dict)
    assert event_finish.get("file_id") == file_id

    assert sender.event_count() == 0
    assert receiver.event_count() == 0
