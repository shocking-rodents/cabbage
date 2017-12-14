# -*- coding: utf-8 -*-
import inspect
import uuid
from abc import abstractmethod, ABC
from itertools import cycle
from random import shuffle
from typing import Optional, Callable, Union, Awaitable
import logging
import asyncio

import aioamqp
from aioamqp.protocol import CONNECTING, OPEN

logger = logging.getLogger(__name__)


class ServiceUnavailableError(Exception):
    """External service unavailable. """


class AbstractAsyncRpcServer(ABC):
    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def run(self, app):
        pass

    @abstractmethod
    async def stop(self, app):
        pass


class AmqpConnection:
    def __init__(self, hosts, username, password, loop=None):
        """
        :param loop: asyncio event loop
        :param hosts: list of hosts in host:port format, e.g. ["192.168.1.10:5672", "192.168.1.20:5672"]
        :param username: AMQP login
        :param password: AMQP password
        """
        self.loop = loop or asyncio.get_event_loop()
        self.hosts = hosts
        self.username = username
        self.password = password
        self.connection_cycle = self.cycle_rabbit_host()
        self.transport = None
        self.protocol = None

    async def channel(self):
        if self.protocol is not None:
            return await self.protocol.channel()

    def cycle_rabbit_host(self):
        shuffle(self.hosts)
        for host_post in cycle(self.hosts):
            host, port = host_post.split(':')
            port = int(port)
            yield host, port

    async def connect(self):
        for host, port in self.connection_cycle:
            try:
                self.transport, self.protocol = await aioamqp.connect(
                    loop=self.loop,
                    host=host,
                    port=port,
                    login=self.username,
                    password=self.password
                )
            except Exception as e:
                logger.info(f'connect. fail connect to {host}:{port}, error <{e.__class__.__name__}> {e}')
            else:
                logger.info(f'connect. connected to {host}:{port}')
                break

    async def disconnect(self):
        if (self.protocol is not None and
                self.protocol.state in [CONNECTING, OPEN]):
            await self.protocol.close()


class AsyncAmqpRpcServer(AbstractAsyncRpcServer):
    def __init__(self, connection: AmqpConnection,
                 request_handler: Union[Callable[[str], Optional[str]], Callable[[str], Awaitable[Optional[str]]]],
                 prefetch_count=None, loop=None):
        """
        :param request_handler: request handler, function (def) or coroutine function (async def).
                                It is to take a str and return either str or None, which means no response is required.
        :param loop: asyncio event loop
        :param prefetch_count: per-consumer prefetch message limit, default 1
        """
        self.connection = connection
        self.request_handler = request_handler
        self.loop = loop or asyncio.get_event_loop()
        self.prefetch_count = prefetch_count or 1
        self.channel = None
        self.keep_running = True
        self.amqp_transport = None
        self.amqp_protocol = None
        self.callback_queue = None
        self.responses = dict()

    async def connect(self):
        await self.connection.connect()
        self.channel = await self.connection.channel()

        # setup client
        result = await self.channel.queue_declare(exclusive=True)
        self.callback_queue = result['queue']
        await self.channel.basic_consume(
            self.on_response,
            queue_name=self.callback_queue,
        )

    async def listen(self, exchange, queue, routing_key, **queue_params):
        """
        :param exchange: topic exchange name to get or create
        :param queue: AMQP queue name
        :param routing_key: AMQP routing key binding queue to exchange
        """
        await self.channel.exchange_declare(exchange_name=exchange, type_name='topic', durable=True)
        result = await self.channel.queue_declare(
            queue_name=queue,
            arguments={
                'x-dead-letter-exchange': 'DLX',
                'x-dead-letter-routing-key': 'dlx_rpc',
            },
            **queue_params
        )
        queue = result['queue']  # in case queue name is empty
        await self.channel.queue_bind(queue_name=queue,
                                      exchange_name=exchange,
                                      routing_key=routing_key)
        await self.channel.basic_qos(
            prefetch_count=self.prefetch_count,
            prefetch_size=0,
            connection_global=False,
        )
        logger.debug(f'listening on queue {queue}, bound to exchange {exchange} by {routing_key}')
        await self.channel.basic_consume(
            self.on_request,
            queue_name=queue,
        )

    async def on_request(self, channel, body, envelope, properties):
        """Run handle() in background. """
        self.loop.create_task(self.handle_rpc(channel, body, envelope, properties))

    async def handle_rpc(self, channel, body, envelope, properties):
        """Process request with handler and send response if needed. """
        try:
            logger.debug(f'> handle_rpc: body {body}, routing_key {properties.reply_to}, '
                         f'correlation_id {properties.correlation_id}')
            if inspect.iscoroutinefunction(self.request_handler):
                response = await self.request_handler(body)
            else:
                response = self.request_handler(body)
        except Exception as e:
            logger.error(f'handle_rpc. error <{e.__class__.__name__}> {e}, routing_key {properties.reply_to}, '
                         f'correlation_id {properties.correlation_id}')
            await channel.basic_reject(delivery_tag=envelope.delivery_tag)
        else:
            logger.debug(f'< handle_rpc: result {response}, responding? '
                         f'{properties.reply_to is not None and response is not None}, '
                         f'routing_key {properties.reply_to}, correlation_id {properties.correlation_id}')

            if properties.reply_to is not None and response is not None:
                response_params = dict(
                    payload=response,
                    exchange_name='',
                    routing_key=properties.reply_to,
                    properties={
                        'content_type': 'application/json',
                        'content_encoding': 'utf-8',
                    },
                )

                if properties.correlation_id:
                    response_params['properties']['correlation_id'] = properties.correlation_id

                await channel.basic_publish(**response_params)

            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def main_loop(self):
        """Main routine for the server. """
        try:
            while self.keep_running:
                await self.connect()
                await self.connection.protocol.wait_closed()
        finally:
            await self.connection.disconnect()

    async def run(self, app):
        """aiohttp-compatible on_startup coroutine. """
        app['amqp_connection'] = self.connection
        await self.connect()
        asyncio.ensure_future(self.main_loop(), loop=app.loop)

    async def stop(self, app):
        """aiohttp-compatible on_cleanup coroutine. """
        self.keep_running = False
        await self.connection.disconnect()

    async def on_response(self, channel, body, envelope, properties):
        """Mark future as done. """
        if properties.correlation_id in self.responses:
            self.responses[properties.correlation_id].set_result(body)
        else:
            logger.warning(f'unexpected message with correlation_id {properties.correlation_id}')

    async def await_response(self, correlation_id, ttl):
        """Wait for a response with given correlation id (blocking call). """
        self.responses[correlation_id] = asyncio.Future()
        try:
            await asyncio.wait_for(self.responses[correlation_id], timeout=ttl)
            return self.responses[correlation_id].result()
        except asyncio.TimeoutError:
            logger.warning(f'request {correlation_id} timed out')
            raise ServiceUnavailableError('Request timed out') from None
        finally:
            self.responses.pop(correlation_id)

    async def send_rpc(self, destination, data: str, ttl: float, await_response=True) -> Optional[str]:
        """Execute RPC on remote server. If await_response is True, the call blocks until the result is returned. """
        properties = dict()
        if await_response:
            correlation_id = str(uuid.uuid4())
            properties = {
                'reply_to': self.callback_queue,
                'correlation_id': correlation_id,
            }
        channel = await self.connection.channel()
        logger.debug(f'< send_rpc: destination {destination}, data {data}, ttl {ttl}, properties {properties}')
        await channel.basic_publish(
            exchange_name=self.exchange,
            routing_key=destination,
            properties=properties,
            payload=data.encode('utf-8'),
        )

        if await_response:
            data = await self.await_response(correlation_id=correlation_id, ttl=ttl)
            data = data.decode('utf-8')
            logger.debug(f'> send_rpc: response {data}')
            return data


class AbstractAsyncRpcClient(ABC):
    @abstractmethod
    async def init(self, app):
        """Connect to AMQP and set up callback queue. """

    @abstractmethod
    async def close(self, app):
        """Close connection"""

    @abstractmethod
    async def send_rpc(self, destination, data: str, ttl: float, await_response=True) -> Optional[str]:
        """Perform RPC call. If `response` is True, return result.
        If no response is received after `ttl` seconds, raise ServiceUnavailableError.
        """


class AsyncAmqpRpcClient(AbstractAsyncRpcClient):
    def __init__(self, connection: AmqpConnection, exchange):
        self.connection = connection
        self.exchange = exchange
        self.callback_queue = None
        self.responses = dict()
        self.events = dict()

    async def init(self, app):
        await self.connection.connect()

        channel = await self.connection.channel()
        result = await channel.queue_declare(exclusive=True)
        self.callback_queue = result['queue']

        await channel.basic_consume(
            self.on_response,
            queue_name=self.callback_queue,
        )

    def close(self, app):
        pass

    async def on_response(self, channel, body, envelope, properties):
        if properties.correlation_id in self.responses:
            self.responses[properties.correlation_id].set_result(body)
        else:
            logger.warning(f'unexpected message with correlation_id {properties.correlation_id}')

    async def await_response(self, correlation_id, ttl):
        self.responses[correlation_id] = asyncio.Future()
        try:
            await asyncio.wait_for(self.responses[correlation_id], timeout=ttl)
            return self.responses[correlation_id].result()
        except asyncio.TimeoutError:
            logger.warning(f'request {correlation_id} timed out')
            raise ServiceUnavailableError('Request timed out') from None
        finally:
            self.responses.pop(correlation_id)

    async def send_rpc(self, destination, data: str, ttl: float, await_response=True) -> Optional[str]:
        properties = dict()
        if await_response:
            correlation_id = str(uuid.uuid4())
            properties = {
                'reply_to': self.callback_queue,
                'correlation_id': correlation_id,
            }
        channel = await self.connection.channel()
        logger.debug(f'< send_rpc: destination {destination}, data {data}, ttl {ttl}, properties {properties}')
        await channel.basic_publish(
            exchange_name=self.exchange,
            routing_key=destination,
            properties=properties,
            payload=data.encode('utf-8'),
        )

        if await_response:
            data = await self.await_response(correlation_id=correlation_id, ttl=ttl)
            data = data.decode('utf-8')
            logger.debug(f'> send_rpc: response {data}')
            return data
