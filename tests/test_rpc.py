# -*- coding: utf-8 -*-
import aioamqp

import pytest
from asynctest import patch

import cabbage
from tests.conftest import MockTransport, MockProtocol, SUBSCRIPTION_QUEUE, TEST_EXCHANGE, SUBSCRIPTION_KEY, \
    RANDOM_QUEUE, HOST, MockEnvelope, MockProperties, TEST_DESTINATION

pytestmark = pytest.mark.asyncio


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
        rpc = cabbage.AsyncAmqpRpc(connection=connection, request_handler=lambda x: x)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(exchange=TEST_EXCHANGE, queue=SUBSCRIPTION_QUEUE, routing_key=SUBSCRIPTION_KEY,
                            queue_params=dict(passive=False, durable=True, exclusive=True, auto_delete=True),
                            exchange_params=dict(type_name='fanout', passive=False, durable=True, auto_delete=True))
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(
            prefetch_count=rpc.prefetch_count, prefetch_size=0, connection_global=False)
        rpc.channel.exchange_declare.assert_called_once_with(
            exchange_name=TEST_EXCHANGE, type_name='fanout', passive=False, durable=True, auto_delete=True)
        rpc.channel.queue_bind.assert_called_once_with(
            exchange_name=TEST_EXCHANGE, queue_name=SUBSCRIPTION_QUEUE, routing_key=SUBSCRIPTION_KEY)
        rpc.channel.queue_declare.assert_called_with(
            queue_name=SUBSCRIPTION_QUEUE, auto_delete=True, durable=True, exclusive=True, passive=False)
        rpc.channel.basic_consume.assert_called_with(
            callback=rpc.on_request, queue_name=SUBSCRIPTION_QUEUE)

    async def test_defaults(self, connection):
        rpc = cabbage.AsyncAmqpRpc(connection=connection, request_handler=lambda x: x)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(queue=SUBSCRIPTION_QUEUE)
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(
            prefetch_count=rpc.prefetch_count, prefetch_size=0, connection_global=False)
        rpc.channel.queue_declare.assert_called_with(
            queue_name=SUBSCRIPTION_QUEUE, durable=True, arguments={'x-dead-letter-exchange': 'DLX',
                                                                    'x-dead-letter-routing-key': 'dlx_rpc'})
        rpc.channel.basic_consume.assert_called_with(
            callback=rpc.on_request, queue_name=SUBSCRIPTION_QUEUE)
        # It is an error to attempt declaring or binding on default exchange:
        rpc.channel.exchange_declare.assert_not_called()
        rpc.channel.queue_bind.assert_not_called()

    async def test_amqp_defaults(self, connection):
        """Test that broker defaults (empty queue, empty exchange) are handled well."""
        rpc = cabbage.AsyncAmqpRpc(connection=connection, request_handler=lambda x: x)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(exchange='', queue='')
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(
            prefetch_count=rpc.prefetch_count, prefetch_size=0, connection_global=False)
        rpc.channel.queue_declare.assert_called_with(
            queue_name='', durable=True, arguments={'x-dead-letter-exchange': 'DLX',
                                                    'x-dead-letter-routing-key': 'dlx_rpc'})
        rpc.channel.basic_consume.assert_called_with(
            callback=rpc.on_request, queue_name=RANDOM_QUEUE)
        # We are still on default exchange:
        rpc.channel.exchange_declare.assert_not_called()
        rpc.channel.queue_bind.assert_not_called()


class TestHandleRpc:
    """AsyncAmqpRpc.handle_rpc: low-level handler called by aioamqp"""

    @pytest.mark.parametrize('body, expected', [
        (b'', ''),
        (b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82', 'Test message body. Тест'),
    ])
    async def test_ok(self, connection, body, expected):
        async def request_handler(request: str) -> str:
            assert request == expected
            return request

        rpc = cabbage.AsyncAmqpRpc(connection=connection, request_handler=request_handler)
        await rpc.connect()
        await rpc.handle_rpc(channel=rpc.channel, body=body, envelope=MockEnvelope(), properties=MockProperties())


class TestSendRpc:
    """AsyncAmqpRpc.send_rpc"""

    @staticmethod
    async def request_handler(request: str) -> str:
        return request

    @pytest.mark.parametrize('data', ['', 'Test message body. Тест'])
    async def test_ok(self, connection, data):
        rpc = cabbage.AsyncAmqpRpc(connection=connection, request_handler=self.request_handler)
        await rpc.connect()
        await rpc.send_rpc(destination=TEST_DESTINATION, data=data, await_response=False)
