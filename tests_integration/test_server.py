# -*- coding: utf-8 -*-
import pytest

from cabbage import AmqpConnection, AsyncAmqpRpc

pytestmark = pytest.mark.asyncio


async def test_ok():
    connection = AmqpConnection()
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()
