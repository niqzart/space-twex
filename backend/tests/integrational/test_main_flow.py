from base64 import b64encode

import pytest
from faker import Faker

from tests.testing import AsyncSIOTestClient


@pytest.mark.anyio
async def test_main_flow(
    sender: AsyncSIOTestClient,
    receiver: AsyncSIOTestClient,
    file_name: str,
    faker: Faker,
) -> None:
    chunk_1: str = b64encode(faker.binary(length=24)).decode("utf-8")
    chunk_2: str = b64encode(faker.binary(length=24)).decode("utf-8")

    # producer creates the file
    code_create, ack_create = await sender.emit("create", {"file_name": file_name})
    assert code_create == 201
    assert isinstance(file_id := ack_create.get("file_id"), str)

    # producer fails to send to no clients
    code_send_early, ack_send_early = await sender.emit(
        "send", {"file_id": file_id, "chunk": chunk_1}
    )
    assert code_send_early == 400
    assert ack_send_early.get("reason") == "Wrong status: open"

    assert sender.event_count() == 0
    assert receiver.event_count() == 0

    # consumer subscribes
    code_subscribe, ack_subscribe = await receiver.emit(
        "subscribe", {"file_id": file_id}
    )
    assert code_subscribe == 200
    assert ack_subscribe.get("file_name") == file_name

    event_subscribe = sender.event_pop("subscribe")
    assert isinstance(event_subscribe, dict)
    assert event_subscribe.get("file_id") == file_id

    # producer sends the first chunk
    code_send, ack_send = await sender.emit(
        "send", {"file_id": file_id, "chunk": chunk_1}
    )
    assert code_send == 200
    assert (chunk_id := ack_send.get("chunk_id")) is not None

    event_send = receiver.event_pop("send")
    assert isinstance(event_send, dict)
    assert event_send.get("chunk") == chunk_1
    assert event_send.get("chunk_id") == chunk_id
    assert event_send.get("file_id") == file_id

    # producer fails to send before confirmation
    code_send_early, ack_send_early = await sender.emit(
        "send", {"file_id": file_id, "chunk": chunk_2}
    )
    assert code_send_early == 400
    assert ack_send_early.get("reason") == "Wrong status: sent"

    assert sender.event_count() == 0
    assert receiver.event_count() == 0

    # consumer confirms the first chunk
    code_confirm, ack_confirm = await receiver.emit(
        "confirm", {"file_id": file_id, "chunk_id": chunk_id}
    )
    assert code_confirm == 204
    assert ack_confirm is None

    event_confirm = sender.event_pop("confirm")
    assert isinstance(event_confirm, dict)
    assert event_confirm.get("chunk_id") == chunk_id
    assert event_confirm.get("file_id") == file_id

    # producer sends the second chunk
    code_send, ack_send = await sender.emit(
        "send", {"file_id": file_id, "chunk": chunk_2}
    )
    assert code_send == 200
    assert (chunk_id := ack_send.get("chunk_id")) is not None

    event_send = receiver.event_pop("send")
    assert isinstance(event_send, dict)
    assert event_send.get("chunk") == chunk_2
    assert event_send.get("chunk_id") == chunk_id
    assert event_send.get("file_id") == file_id

    # producer fails to close the file before confirmation
    code_finish, ack_finish = await sender.emit("finish", {"file_id": file_id})
    assert code_finish == 400
    assert ack_finish.get("reason") == "Wrong status: sent"

    assert sender.event_count() == 0
    assert receiver.event_count() == 0

    # consumer confirms the second chunk
    code_confirm, ack_confirm = await receiver.emit(
        "confirm", {"file_id": file_id, "chunk_id": chunk_id}
    )
    assert code_confirm == 204
    assert ack_confirm is None

    event_confirm = sender.event_pop("confirm")
    assert isinstance(event_confirm, dict)
    assert event_confirm.get("chunk_id") == chunk_id
    assert event_confirm.get("file_id") == file_id

    # producer closes the file
    code_finish, ack_finish = await sender.emit("finish", {"file_id": file_id})
    assert code_finish == 204
    assert ack_finish is None

    event_finish = receiver.event_pop("finish")
    assert isinstance(event_finish, dict)
    assert event_finish.get("file_id") == file_id

    # no residual events
    assert sender.event_count() == 0
    assert receiver.event_count() == 0
