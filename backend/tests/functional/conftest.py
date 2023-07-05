import pytest

from app.twex.twex_db import Twex


@pytest.fixture()
async def source_twex(file_name: str) -> Twex:
    twex = Twex(file_name=file_name)
    await twex.save()
    return twex
