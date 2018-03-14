# -*- coding: utf-8 -*-
import pytest

from cabbage import AmqpConnection, AsyncAmqpRpc

pytestmark = pytest.mark.asyncio


async def test_sanity(management):
    """Basic sanity check. If this fails, something is horribly wrong (or you are not running in Docker)."""
    connection = AmqpConnection(host='rabbitmq', port=5672)
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()
    assert management.get_queue('/', 'my_crappy_queue_that_should_not_exist') == {
        'error': 'Object Not Found',
        'reason': '"Not Found"\n'
    }


@pytest.mark.parametrize('exchange, queue', [
    ('', 'my_queue'),
    ('my_exchange', 'my_queue'),
])
async def test_subscribe(request, rpc, management, exchange, queue):
    assert management.get_queue(request.node.name, queue) == {
        'error': 'Object Not Found',
        'reason': '"Not Found"\n'
    }
    await rpc.subscribe(exchange=exchange, queue=queue)
    assert management.get_queue(request.node.name, queue).get('name') == queue
