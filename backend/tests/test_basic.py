import pytest
from faker import Faker

from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_basic(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    faker: Faker,
) -> None:
    ack_create = await sender.emit("create", None)
    assert ack_create.get("code") == 201
    assert (file_id := ack_create.get("data"))

    ack_subscribe = await receiver.emit("subscribe", {"file_id": file_id})
    assert ack_subscribe.get("code") == 200

    chunk: bytes = faker.binary(length=24)
    ack_send = await sender.emit("send", {"file_id": file_id, "chunk": chunk})
    assert ack_send.get("code") == 200

    event_send = receiver.event_pop("send")
    assert event_send == chunk

    assert sender.event_count() == 0
    assert receiver.event_count() == 0
