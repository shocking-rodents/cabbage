# -*- coding: utf-8 -*-
import pytest
from asynctest import patch, CoroutineMock

import cabbage
from tests.conftest import MockTransport, MockProtocol

pytestmark = pytest.mark.asyncio


# Sample non-default connection info:
HOST = '192.168.0.10'
PORT = 25762
USERNAME = 'amqp_user'
PASSWORD = 'secret_password'
VIRTUALHOST = 'test_vhost'


class TestConnect:
    """AmqpConnection.connect"""

    async def test_default_params(self, event_loop):
        """Check that AmqpConnection supplies reasonable defaults."""
        connection = cabbage.AmqpConnection(loop=event_loop)
        with patch('aioamqp.connect', new_callable=CoroutineMock) as mock_connect:
            mock_transport = MockTransport()
            mock_protocol = MockProtocol()
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(
            host='localhost', port=5672, login='guest', password='guest', virtualhost='/', loop=event_loop)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_connect(self, event_loop):
        """Check typical connection call."""
        connection = cabbage.AmqpConnection(
            host=HOST, port=PORT, username=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        with patch('aioamqp.connect') as mock_connect:
            mock_transport = MockTransport()
            mock_protocol = MockProtocol()
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(
            host=HOST, port=PORT, login=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol
