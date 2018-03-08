# -*- coding: utf-8 -*-
import aioamqp

import pytest
from asynctest import patch

import cabbage
from tests.conftest import MockTransport, MockProtocol

pytestmark = pytest.mark.asyncio
HOST = 'fake_amqp_host'
SUBSCRIPTION_QUEUE = 'rpc_subscription_queue'


class TestConnect:
    """AsyncAmqpRpc.connect"""

    async def test_ok(self, event_loop):
        connection = cabbage.AmqpConnection(host=HOST, loop=event_loop)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        with patch('aioamqp.connect') as mock_connect:
            mock_connect.return_value = (MockTransport(), MockProtocol())
            await rpc.connect()
        mock_connect.assert_called_once()
        # check that client is set up correctly:
        assert isinstance(rpc.channel, aioamqp.channel.Channel)  # actually it's a mock pretending to be a Channel
        rpc.channel.queue_declare.assert_called_with(exclusive=True)
        rpc.channel.basic_consume.assert_called_once_with(callback=rpc.on_response,
                                                          queue_name=rpc.callback_queue)


class TestSubscribe:
    """AsyncAmqpRpc.subscribe"""

    async def test_ok(self, connection):
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(queue=SUBSCRIPTION_QUEUE, queue_params={})
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(prefetch_count=rpc.prefetch_count,
                                                      prefetch_size=0,
                                                      connection_global=False)
        rpc.channel.queue_declare.assert_called_with(queue_name=SUBSCRIPTION_QUEUE)
        rpc.channel.basic_consume.assert_called_with(callback=rpc.on_request,
                                                     queue_name=SUBSCRIPTION_QUEUE)
