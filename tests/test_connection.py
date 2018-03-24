# -*- coding: utf-8 -*-
import asyncio

import pytest
from aioamqp.protocol import CLOSED
from asynctest import patch, MagicMock

import cabbage
from tests.conftest import MockTransport, MockProtocol

pytestmark = pytest.mark.asyncio

# Sample non-default connection info:
HOST = 'fake_amqp_host'
PORT = 25762
USERNAME = 'amqp_user'
PASSWORD = 'secret_password'
VIRTUALHOST = 'test_vhost'


class TestConnect:
    """AmqpConnection.connect"""

    async def test_connect(self, event_loop):
        """Check typical connection call."""
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        connection = cabbage.AmqpConnection(
            hosts=[(HOST, PORT)], username=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        with patch('aioamqp.connect') as mock_connect:
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(
            host=HOST, port=PORT, login=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_default_params(self, event_loop):
        """Check that AmqpConnection supplies reasonable defaults."""
        connection = cabbage.AmqpConnection(loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        with patch('aioamqp.connect') as mock_connect:
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(
            host='localhost', port=5672, login='guest', password='guest', virtualhost='/', loop=event_loop)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_called_twice(self, event_loop):
        """Check that calling the method twice has no effect."""
        connection = cabbage.AmqpConnection(loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        connection.transport = mock_transport
        connection.protocol = mock_protocol
        with patch('aioamqp.connect') as mock_connect:
            await connection.connect()

        mock_connect.assert_not_called()
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_retry(self, event_loop):
        """Keep reconnecting if an error occurs."""
        attempts = 10

        async def faulty_connect(_attempts=[attempts], **kwargs):
            assert kwargs == dict(
                host=HOST, port=PORT, login=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
            _attempts[0] -= 1
            if _attempts[0]:
                raise OSError('[Errno 113] Connect call failed')
            return mock_transport, mock_protocol

        connection = cabbage.AmqpConnection(
            hosts=[(HOST, PORT)], username=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()

        with patch('aioamqp.connect', new=faulty_connect), patch('asyncio.sleep') as mock_sleep:
            await connection.connect()

        assert mock_sleep.call_count == attempts - 1
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_fatal_error(self, event_loop):
        """Some connection errors are not worth trying to recover from."""
        connection = cabbage.AmqpConnection(hosts=[('angrydev.ru', 80)], loop=event_loop)

        with pytest.raises(asyncio.streams.IncompleteReadError):
            with patch('aioamqp.connect', side_effect=asyncio.streams.IncompleteReadError([], 160)) as mock_connect:
                await connection.connect()

        mock_connect.assert_called_once()
        assert connection.transport is None
        assert connection.protocol is None


class TestDisconnect:
    async def test_ok(self, connection):
        await connection.disconnect()
        connection.protocol.close.assert_called_once_with()

    async def test_no_protocol(self, event_loop):
        connection = cabbage.AmqpConnection(loop=event_loop)
        connection.protocol = MagicMock()
        await connection.disconnect()
        connection.protocol.close.assert_not_called()

    async def test_not_connected(self, connection):
        connection.protocol.state = CLOSED
        await connection.disconnect()
        connection.protocol.close.assert_not_called()
