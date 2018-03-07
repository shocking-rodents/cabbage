# -*- coding: utf-8 -*-
from unittest.mock import MagicMock
import logging

import aioamqp

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


class AwaitableMock(MagicMock):
    """Awaitable MagicMock"""
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class MockProtocol(MagicMock):
    def __init__(self):
        super().__init__()
        self.state = aioamqp.protocol.OPEN

    async def channel(self):
        return MockChannel()


class MockChannel(MagicMock):
    pass
