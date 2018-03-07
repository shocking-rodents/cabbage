# -*- coding: utf-8 -*-
import pytest

import cabbage

pytestmark = pytest.mark.asyncio


class TestSendRpc:
    async def test_ok(self, event_loop):
        connection = cabbage.AmqpConnection(loop=event_loop)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
