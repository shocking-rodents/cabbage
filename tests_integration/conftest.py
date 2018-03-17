# -*- coding: utf-8 -*-
import logging
from urllib.parse import quote

import pytest
import requests
from requests.auth import HTTPBasicAuth

from cabbage import AmqpConnection, AsyncAmqpRpc

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')


class Management:
    def __init__(self, url):
        self.base_url = url
        self.kwargs = dict(
            auth=HTTPBasicAuth('guest', 'guest'),
            headers={'content-type': 'application/json'},
        )

    def get_queue(self, vhost, name):
        return requests.get(f'{self.base_url}/api/queues/{quote(vhost, safe="")}/{quote(name, safe="")}',
                            **self.kwargs).json()

    def put_vhost(self, vhost):
        requests.put(f'{self.base_url}/api/vhosts/{quote(vhost, safe="")}', **self.kwargs)
        requests.put(f'{self.base_url}/api/permissions/{quote(vhost, safe="")}/guest',
                     json={'configure': '.*', 'write': '.*', 'read': '.*'}, **self.kwargs)

    def delete_vhost(self, vhost):
        requests.delete(f'{self.base_url}/api/vhosts/{quote(vhost, safe="")}', **self.kwargs)


TEST_VHOST = 'cabbage_test'


@pytest.fixture(scope='session')
def management():
    """Wrapper for RabbitMQ Management Plugin API."""
    return Management('http://rabbitmq:15672')


@pytest.yield_fixture(scope='function', autouse=True)
def vhost_environment(management):
    management.put_vhost(TEST_VHOST)
    yield
    management.delete_vhost(TEST_VHOST)


@pytest.fixture
async def rpc(event_loop):
    """Ready-to-work RPC connected to RabbitMQ in Docker."""
    connection = AmqpConnection(host='rabbitmq', port=5672, virtualhost=TEST_VHOST, loop=event_loop)
    rpc = AsyncAmqpRpc(connection=connection, request_handler=lambda x: x)
    await rpc.connect()
    return rpc
