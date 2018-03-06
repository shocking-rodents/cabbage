# -*- coding: utf-8 -*-
from unittest.mock import MagicMock

import pytest
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


class AwaitableMock(MagicMock):
    """Awaitable MagicMock"""
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)
