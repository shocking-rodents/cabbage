# -*- coding: utf-8 -*-
from asynctest import patch, CoroutineMock

import pytest

import cabbage
from tests.conftest import MockTransport, MockProtocol

pytestmark = pytest.mark.asyncio


class TestConnect:
    async def test_ok(self, event_loop):
        connection = cabbage.AmqpConnection(host='wtf', loop=event_loop)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        with patch('aioamqp.connect', new_callable=CoroutineMock) as mock_connect:
            mock_connect.return_value = (MockTransport(), MockProtocol())
            await rpc.connect()
