# -*- coding: utf-8 -*-
import pytest

from cabbage import AmqpConnection, AsyncAmqpRpc

pytestmark = pytest.mark.asyncio


async def test_sanity(management):
    """Basic sanity check. If this fails, something went horribly wrong (or you are not running in Docker)."""
    connection = AmqpConnection(hosts=[('rabbitmq', 5672)])
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()
    assert management.get_queue('my_queue_that_should_not_exist').get('error') == 'Object Not Found'


@pytest.mark.parametrize('exchange, queue', [
    ('', 'my_queue'),
    ('my_exchange', 'my_queue'),
])
async def test_subscribe(rpc, management, exchange, queue):
    assert management.get_queue(queue).get('error') == 'Object Not Found'
    print(management.get_consumers())
    assert len(management.get_consumers()) == 1  # callback queue
    await rpc.subscribe(exchange=exchange, queue=queue)
    assert management.get_queue(queue).get('name') == queue
    assert len(management.get_consumers()) == 2  # callback queue + test queue
