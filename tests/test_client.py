# -*- coding: utf-8 -*-
import asyncio

import pytest
from asynctest import patch

import cabbage
from tests.conftest import RANDOM_QUEUE, TEST_DESTINATION, RESPONSE_CORR_ID, MockEnvelope, MockProperties, DELIVERY_TAG

pytestmark = pytest.mark.asyncio


class TestSendRpc:
    """AsyncAmqpRpc.send_rpc"""

    @pytest.mark.parametrize('data, expected_payload', [
        ('', b''),
        (b'', b''),
        ('Test message body. Тест', b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82'),
        (b'Raw message \xff', b'Raw message \xff'),
    ])
    async def test_no_response(self, connection, data, expected_payload):
        """Check that data is correctly encoded (if needed) and sent."""
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        await rpc.send_rpc(destination=TEST_DESTINATION, data=data, await_response=False)
        rpc.channel.basic_publish.assert_called_once_with(
            exchange_name='', routing_key=TEST_DESTINATION, properties={}, payload=expected_payload)

    @pytest.mark.parametrize('data, sent_payload, received_payload, expected_result', [
        ('request data', b'request data', b'', ''),
        (b'raw request', b'raw request', b'', b''),
        ('request data', b'request data', b'Test. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82', 'Test. Тест'),
        (b'raw request', b'raw request', b'Raw message \xff', b'Raw message \xff'),
    ])
    async def test_await_response(self, connection, data, sent_payload, received_payload, expected_result):
        """Check that data returned by await_response is parsed and returned correctly."""
        rpc = cabbage.AsyncAmqpRpc(connection=connection)
        await rpc.connect()
        with patch('cabbage.amqp.AsyncAmqpRpc.await_response', return_value=received_payload, autospec=True), \
                patch('cabbage.amqp.uuid.uuid4', return_value=RESPONSE_CORR_ID):
            result = await rpc.send_rpc(destination=TEST_DESTINATION, data=data, await_response=True)
        assert result == expected_result
        rpc.channel.basic_publish.assert_called_once_with(
            exchange_name='', routing_key=TEST_DESTINATION, payload=sent_payload,
            properties={'reply_to': RANDOM_QUEUE, 'correlation_id': RESPONSE_CORR_ID})


class TestAwaitResponse:
    """AsyncAmqpRpc.await_response"""

    async def test_ok(self, rpc):
        # schedule awaiting response in another Task
        task = asyncio.ensure_future(rpc.await_response(correlation_id=RESPONSE_CORR_ID, ttl=10.0))
        # but it's not executing yet
        assert set(rpc.responses.keys()) == set()
        # let it run for a bit
        await asyncio.sleep(0)
        # check that it created a Future
        assert set(rpc.responses.keys()) == {RESPONSE_CORR_ID}
        # set Future result
        rpc.responses[RESPONSE_CORR_ID].set_result('task result')
        # let the Task run to completion
        await task
        assert task.done()
        assert task.result() == 'task result'
        # check that it cleaned up rpc.responses
        assert set(rpc.responses.keys()) == set()

    async def test_timeout(self, rpc):
        with pytest.raises(cabbage.ServiceUnavailableError):
            await rpc.await_response(correlation_id=RESPONSE_CORR_ID, ttl=0)
        assert set(rpc.responses.keys()) == set()


class TestOnResponse:
    """AsyncAmqpRpc.on_response: aioamqp handler for messages in callback queue."""

    @pytest.mark.parametrize('body', [b'', b'Test message body. \xd0\xa2\xd0\xb5\xd1\x81\xd1\x82'])
    async def test_ok(self, rpc, body):
        rpc.responses[RESPONSE_CORR_ID] = asyncio.Future()
        await rpc.on_response(channel=rpc.channel, body=body, envelope=MockEnvelope(), properties=MockProperties())
        assert rpc.responses[RESPONSE_CORR_ID].done()
        assert rpc.responses[RESPONSE_CORR_ID].result() == body
        rpc.channel.basic_client_ack.assert_called_once_with(delivery_tag=DELIVERY_TAG)
        rpc.channel.basic_client_nack.assert_not_called()

    async def test_unexpected_tag(self, rpc):
        await rpc.on_response(channel=rpc.channel, body=b'resp', envelope=MockEnvelope(), properties=MockProperties())
        rpc.channel.basic_client_ack.assert_not_called()
        rpc.channel.basic_client_nack.assert_called_once_with(delivery_tag=DELIVERY_TAG)
