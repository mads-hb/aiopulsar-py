import pytest
import aiopulsar
from aiopulsar.client import Client
from aiopulsar.utils import _ClientContextManager


class TestClient:
    @pytest.mark.asyncio
    async def test_connect(self):
        conn = aiopulsar.connect("localhost")
        assert isinstance(conn, _ClientContextManager)
        client = await conn.__aenter__()
        assert isinstance(client, Client)
        assert not client.closed
        await conn.__aexit__(None, None, None)
        assert client.closed
