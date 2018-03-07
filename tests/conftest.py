# -*- coding: utf-8 -*-
from asynctest import MagicMock
import logging

import aioamqp

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


MockTransport = MagicMock


class MockProtocol:
    def __new__(cls, *args, **kwargs):
        m = MagicMock(spec=aioamqp.protocol.AmqpProtocol, name='MockProtocol')
        m.state = aioamqp.protocol.OPEN
        m.channel.return_value = MockChannel()
        return m


class MockChannel:
    def __new__(cls, *args, **kwargs):
        return MagicMock(spec=aioamqp.channel.Channel, name='MockChannel')
