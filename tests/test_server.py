# -*- coding: utf-8 -*-
import aioamqp
import asyncio

import pytest
from asynctest import patch, MagicMock

import cabbage
from tests.conftest import MockTransport, MockProtocol, SUBSCRIPTION_QUEUE, TEST_EXCHANGE, SUBSCRIPTION_KEY, \
    RANDOM_QUEUE, HOST, MockEnvelope, MockProperties, CONSUMER_TAG, DELIVERY_TAG, RESPONSE_CORR_ID

pytestmark = pytest.mark.asyncio
TEST_DELAY = 0.2


class TestConnect:
    """AsyncAmqpRpc.connect"""

    async def test_ok(self, event_loop):
        connection = cabbage.AmqpConnection(hosts=[(HOST, 5672)], loop=event_loop)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        with patch('aioamqp.connect') as mock_connect:
            mock_connect.return_value = (MockTransport(), MockProtocol())
            await rpc.connect()
        mock_connect.assert_called_once()
        # check that client is set up correctly:
        assert isinstance(rpc.channel, aioamqp.channel.Channel)  # actually it's a mock pretending to be a Channel
        rpc.channel.queue_declare.assert_called_with(exclusive=True)
        rpc.channel.basic_consume.assert_called_once_with(callback=rpc._on_response,
                                                          queue_name=rpc.callback_queue)


class TestSubscribe:
    """AsyncAmqpRpc.subscribe"""

    async def test_ok(self, connection):
        def request_handler(request):
            return request

        rpc = cabbage.AsyncAmqpRpc(
            connection=connection,
            queue_params=dict(passive=False, durable=True, exclusive=True, auto_delete=True),
            exchange_params=dict(type_name='fanout', passive=False, durable=True, auto_delete=True))
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(exchange=TEST_EXCHANGE, queue=SUBSCRIPTION_QUEUE, routing_key=SUBSCRIPTION_KEY,
                            request_handler=request_handler)
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
        # rpc.channel.basic_consume.assert_called_with(  # partial() != partial()
        #     callback=partial(rpc._on_request, request_handler=request_handler), queue_name=SUBSCRIPTION_QUEUE)

    async def test_defaults(self, connection):
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(request_handler=lambda x: x, queue=SUBSCRIPTION_QUEUE)
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(
            prefetch_count=rpc.prefetch_count, prefetch_size=0, connection_global=False)
        rpc.channel.queue_declare.assert_called_with(
            queue_name=SUBSCRIPTION_QUEUE, durable=True, arguments={'x-dead-letter-exchange': 'DLX',
                                                                    'x-dead-letter-routing-key': 'dlx_rpc'})
        # rpc.channel.basic_consume.assert_called_with(
        #     callback=rpc._on_request, queue_name=SUBSCRIPTION_QUEUE)
        # It is an error to attempt declaring or binding on default exchange:
        rpc.channel.exchange_declare.assert_not_called()
        rpc.channel.queue_bind.assert_not_called()

    async def test_amqp_defaults(self, connection):
        """Test that broker defaults (empty queue, empty exchange) are handled well."""
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        assert rpc.channel.queue_declare.call_count == 1
        assert rpc.channel.basic_consume.call_count == 1

        await rpc.subscribe(request_handler=lambda x: x, exchange='', queue='')
        assert rpc.channel.queue_declare.call_count == 2
        assert rpc.channel.basic_consume.call_count == 2
        rpc.channel.basic_qos.assert_called_once_with(
            prefetch_count=rpc.prefetch_count, prefetch_size=0, connection_global=False)
        rpc.channel.queue_declare.assert_called_with(
            queue_name='', durable=True, arguments={'x-dead-letter-exchange': 'DLX',
                                                    'x-dead-letter-routing-key': 'dlx_rpc'})
        # rpc.channel.basic_consume.assert_called_with(
        #     callback=rpc._on_request, queue_name=RANDOM_QUEUE)
        # We are still on default exchange:
        rpc.channel.exchange_declare.assert_not_called()
        rpc.channel.queue_bind.assert_not_called()


class TestUnsubscribe:
    """AsyncAmqpRpc.unsubscribe"""

    async def test_ok(self, connection):
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        await rpc.unsubscribe(consumer_tag=CONSUMER_TAG)
        rpc.channel.basic_cancel.assert_called_once_with(consumer_tag=CONSUMER_TAG)


class TestHandleRpc:
    """AsyncAmqpRpc.handle_rpc: low-level handler called by aioamqp"""

    @staticmethod
    def request_handler_factory(async_, responding=True, fail=False):
        """Creates a request_handler the server can call on the payload."""
        if fail:
            return MagicMock(side_effect=Exception('Handler error'))
        if async_:
            async def request_handler(request):
                return request if responding else None
        else:
            def request_handler(request):
                return request if responding else None
        return MagicMock(side_effect=request_handler)

    @pytest.mark.parametrize('is_async', [True, False])
    @pytest.mark.parametrize('body, expected', [
        (b'', ''),
        (b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82', 'Test message body. Тест'),
    ])
    async def test_responding(self, connection, body, expected, is_async):
        handler = self.request_handler_factory(is_async, responding=True, fail=False)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        await rpc.handle_rpc(channel=rpc.channel, body=body, envelope=MockEnvelope(), properties=MockProperties(),
                             request_handler=handler)
        handler.assert_called_once_with(expected)
        rpc.channel.basic_client_nack.assert_not_called()
        rpc.channel.basic_client_ack.assert_called_once_with(delivery_tag=DELIVERY_TAG)
        rpc.channel.basic_publish.assert_called_once_with(
            exchange_name='', payload=body, properties={'correlation_id': RESPONSE_CORR_ID}, routing_key=RANDOM_QUEUE)

    @pytest.mark.parametrize('is_async', [True, False])
    @pytest.mark.parametrize('body, expected', [
        (b'', ''),
        (b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82', 'Test message body. Тест'),
    ])
    async def test_not_responding(self, connection, body, expected, is_async):
        """Handler returns None instead of str/bytes => no response needed."""
        handler = self.request_handler_factory(is_async, responding=False, fail=False)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        await rpc.handle_rpc(channel=rpc.channel, body=body, envelope=MockEnvelope(), properties=MockProperties(),
                             request_handler=handler)
        handler.assert_called_once_with(expected)
        rpc.channel.basic_client_nack.assert_not_called()
        rpc.channel.basic_client_ack.assert_called_once_with(delivery_tag=DELIVERY_TAG)
        rpc.channel.basic_publish.assert_not_called()

    @pytest.mark.parametrize('is_async', [True, False])
    @pytest.mark.parametrize('body, expected', [
        (b'', ''),
        (b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82', 'Test message body. Тест'),
    ])
    async def test_exception(self, connection, body, expected, is_async):
        handler = self.request_handler_factory(is_async, fail=True)
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        await rpc.handle_rpc(channel=rpc.channel, body=body, envelope=MockEnvelope(), properties=MockProperties(),
                             request_handler=handler)
        handler.assert_called_once_with(expected)
        rpc.channel.basic_client_nack.assert_called_once_with(delivery_tag=DELIVERY_TAG)
        rpc.channel.basic_client_ack.assert_not_called()

    @pytest.mark.parametrize('is_connected', [True, False])
    @pytest.mark.parametrize('channel', [True, False])
    async def test_wait_connection(self, is_connected, channel):
        """
        Checking for execution time of cabbage.AsyncAmqpRpc.wait_connected()
        In case of True value of both variables 'connection.is_connected' and 'channel'
        the function should terminate immediately. Else the function should check
        the state of these variables with an interval determined in variable 'connection_delay' (in seconds)
        until both variables won't have True value.
        """

        class FakeSelf:

            class FakeConnection:
                def __init__(self, is_connected_):
                    self.is_connected = is_connected_

            def __init__(self, is_connected_, channel_):
                self.connection = self.FakeConnection(is_connected_)
                self.channel = channel_
                self.connection_delay = TEST_DELAY

        fake_self = FakeSelf(is_connected, channel)
        future = asyncio.ensure_future(cabbage.AsyncAmqpRpc.wait_connected(fake_self))
        await asyncio.sleep(TEST_DELAY)
        if not future.done():
            fake_self.connection.is_connected = True
            fake_self.channel = True

        await asyncio.sleep(TEST_DELAY)
        assert future.done()

    @pytest.mark.parametrize('number_of_tasks', [0, 1, 10])
    @pytest.mark.parametrize('pending', [True, False])
    async def test_launch_server(self, connection, number_of_tasks, pending):
        """
        Test for cabbage.AsyncAmqpRpc.stop(). All tasks should execute asynchronously.
        In process of tests it may be created a big task (pending variable) if compare with others ones.
        In this case the task should continue the executing after calling the target function
        """

        small_delay = TEST_DELAY * 0.5
        big_delay = TEST_DELAY * 3
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        rpc.shutdown_timeout = TEST_DELAY
        await rpc.connect()
        await rpc.subscribe(request_handler=lambda x: x, queue=SUBSCRIPTION_QUEUE)

        rpc._tasks = [asyncio.sleep(small_delay) for i in range(number_of_tasks)]
        if pending:
            # a big delay task
            rpc._tasks.append(asyncio.sleep(big_delay))

        await asyncio.sleep(TEST_DELAY)
        future = asyncio.ensure_future(rpc.stop())
        await asyncio.sleep(TEST_DELAY)

        # if self._tasks contains a big delay task, the operation shouldn't be done
        if pending:
            await asyncio.sleep(big_delay)

        assert future.done()
