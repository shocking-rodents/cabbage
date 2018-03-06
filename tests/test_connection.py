# -*- coding: utf-8 -*-
from unittest.mock import patch

import pytest

import cabbage
from .conftest import AwaitableMock

pytestmark = pytest.mark.asyncio


async def test_defaults(event_loop):
    connection = cabbage.AmqpConnection(loop=event_loop)
    assert connection.username == 'guest'
    assert connection.password == 'guest'

# Test non-default connection info:
HOST = '192.168.0.10'
PORT = 25762
USERNAME = 'amqp_user'
PASSWORD = 'secret_password'
VIRTUALHOST = 'test_vhost'


class TestConnect:
    """AmqpConnection.connect"""

    async def test_connect(self, event_loop):
        connection = cabbage.AmqpConnection(
            host=HOST, port=PORT, username=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        with patch('aioamqp.connect', new_callable=AwaitableMock) as mock_connect:
            mock_transport = object()
            mock_protocol = object()
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(
            host=HOST, port=PORT, login=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol
