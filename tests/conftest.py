# -*- coding: utf-8 -*-
import logging

import pytest
from asynctest import MagicMock
import aioamqp

from cabbage import AmqpConnection

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


@pytest.fixture
def connection(event_loop):
    conn = AmqpConnection(host='fake_amqp_host', loop=event_loop)
    conn.transport = MockTransport()
    conn.protocol = MockProtocol()
    return conn

# some non-default values to use in tests


HOST = 'fake_amqp_host'
TEST_EXCHANGE = 'rpc_exchange'
TEST_DESTINATION = 'rpc_destination'
SUBSCRIPTION_QUEUE = 'rpc_subscription_queue'
RANDOM_QUEUE = 'amq.gen-random_queue_name'
SUBSCRIPTION_KEY = 'rpc_subscription_key'


# aioamqp classes mocked as factory functions:


def MockTransport():
    return MagicMock(name='MockTransport')


def MockProtocol():
    m = MagicMock(spec=aioamqp.protocol.AmqpProtocol, name='MockProtocol')
    m.state = aioamqp.protocol.OPEN
    m.channel.return_value = MockChannel()
    return m


def MockChannel():
    async def queue_declare(queue_name=None, passive=False, durable=False, exclusive=False,
                            auto_delete=False, no_wait=False, arguments=None):
        return {'queue': queue_name or RANDOM_QUEUE}
    m = MagicMock(spec=aioamqp.channel.Channel, name='MockChannel')
    m.queue_declare.side_effect = queue_declare
    return m


def MockEnvelope():
    return MagicMock(spec=aioamqp.envelope.Envelope, name='MockEnvelope')


def MockProperties():
    return MagicMock(spec=aioamqp.properties.Properties, name='MockProperties')
