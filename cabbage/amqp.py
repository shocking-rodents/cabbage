# -*- coding: utf-8 -*-
import inspect
import uuid
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


class AmqpConnection:
    def __init__(self, username, password, host, port, virtualhost='/', loop=None):
        """
        :param loop: asyncio event loop
        :param username: AMQP login
        :param password: AMQP password
        :param host: server host
        :param port: server port
        :param virtualhost: AMQP virtual host
        """
        self.loop = loop or asyncio.get_event_loop()
        self.username = username
        self.password = password
        self.connection_cycle = cycle([(host, port, virtualhost)])
        self.transport = None
        self.protocol = None

    async def channel(self):
        if self.protocol is not None:
            return await self.protocol.channel()

    def cycle_rabbit_host(self):
        # TODO: add vhost support
        shuffle(self.hosts)
        for host_post in cycle(self.hosts):
            host, port = host_post.split(':')
            port = int(port)
            yield host, port

    async def connect(self):
        delay = 1.0
        for host, port, virtualhost in self.connection_cycle:
            try:
                self.transport, self.protocol = await aioamqp.connect(
                    loop=self.loop,
                    host=host,
                    port=port,
                    virtualhost=virtualhost,
                    login=self.username,
                    password=self.password
                )
            except Exception as e:
                logger.info(f'failed to connect to {host}:{port}, error <{e.__class__.__name__}> {e}, '
                            f'retrying in {int(delay)} seconds')
                await asyncio.sleep(int(delay))
                delay = min(delay * 1.537, 60.0)
            else:
                logger.info(f'connected to {host}:{port}')
                break

    async def disconnect(self):
        if (self.protocol is not None and
                self.protocol.state in [CONNECTING, OPEN]):
            await self.protocol.close()


class AsyncAmqpRpc:
    def __init__(self, connection: AmqpConnection,
                 request_handler: Union[Callable[[str], Optional[str]], Callable[[str], Awaitable[Optional[str]]]],
                 listen_queues=None, prefetch_count=None, loop=None):
        """
        :param request_handler: request handler, function (def) or coroutine function (async def).
                                It is to take a str and return either str or None, which means no response is required.
        :param listen_queues: list of tuples (exchange, queue, routing_key, queue_params)
        :param loop: asyncio event loop
        :param prefetch_count: per-consumer prefetch message limit, default 1
        """
        self.listen_queues = listen_queues or []
        self.connection = connection
        self.request_handler = request_handler
        self.loop = loop
        self.prefetch_count = prefetch_count or 1
        self.keep_running = True
        self.channel = None
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
        logger.debug(f'listening on callback queue {result["queue"]}')

    async def listen(self, exchange, queue, routing_key, **queue_params):
        """
        :param exchange: topic exchange name to get or create
        :param queue: AMQP queue name
        :param routing_key: AMQP routing key binding queue to exchange
        :return: consumer_tag
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
        await self.channel.queue_bind(
            queue_name=queue,
            exchange_name=exchange,
            routing_key=routing_key)
        await self.channel.basic_qos(
            prefetch_count=self.prefetch_count,
            prefetch_size=0,
            connection_global=False,
        )
        result = await self.channel.basic_consume(
            self.on_request,
            queue_name=queue,
        )
        logger.debug(f'listening on queue {queue}, bound to exchange {exchange} by {routing_key}')
        return result['consumer_tag']

    async def unlisten(self, consumer_tag):
        return await self.channel.basic_cancel(consumer_tag)

    async def on_request(self, channel, body, envelope, properties):
        """Run handle() in background. """
        asyncio.ensure_future(self.handle_rpc(channel, body, envelope, properties))

    async def handle_rpc(self, channel, body, envelope, properties):
        """Process request with handler and send response if needed. """
        try:
            data = body.decode('utf-8')
            logger.debug(f'> handle_rpc: data {data}, routing_key {properties.reply_to}, '
                         f'correlation_id {properties.correlation_id}')
            if inspect.iscoroutinefunction(self.request_handler):
                response = await self.request_handler(data)
            else:
                response = self.request_handler(data)
        except Exception as e:
            logger.error(f'handle_rpc. error <{e.__class__.__name__}> {e}, routing_key {properties.reply_to}, '
                         f'correlation_id {properties.correlation_id}')
            await channel.basic_reject(delivery_tag=envelope.delivery_tag)
        else:
            responding = properties.reply_to is not None and response is not None
            logger.debug(f'{"< " * responding}handle_rpc: responding? {responding}, routing_key {properties.reply_to}, '
                         f'correlation_id {properties.correlation_id}, result {response}')
            if responding:
                response_params = dict(
                    payload=response.encode('utf-8'),
                    exchange_name='',
                    routing_key=properties.reply_to
                )

                if properties.correlation_id:
                    response_params['properties'] = {'correlation_id': properties.correlation_id}

                await channel.basic_publish(**response_params)

            if channel.is_open:
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def main_loop(self):
        """Main routine for the server. """
        try:
            while self.keep_running:
                await self.connect()
                for exchange, queue, routing_key, queue_params in self.listen_queues:
                    await self.listen(exchange, queue, routing_key, **queue_params)
                await self.connection.protocol.wait_closed()
        finally:
            await self.connection.disconnect()

    async def run(self, app=None):
        """aiohttp-compatible on_startup coroutine. """
        loop = None
        if app:
            app['amqp_connection'] = self.connection
            loop = app.loop
        if loop is None:
            loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.main_loop(), loop=loop)

    async def stop(self, app=None):
        """aiohttp-compatible on_cleanup coroutine. """
        self.keep_running = False
        await self.connection.disconnect()

    async def on_response(self, channel, body, envelope, properties):
        """Mark future as done. """
        if properties.correlation_id in self.responses:
            self.responses[properties.correlation_id].set_result(body)
        else:
            logger.warning(f'unexpected message with correlation_id {properties.correlation_id}')

        if channel.is_open:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

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

    async def send_rpc(self, exchange, destination, data: str, ttl: float, await_response=True,
                       callback_queue=None) -> Optional[str]:
        """Execute RPC on remote server.
        If `await_response` is True, the call blocks until the result is returned.
        `callback_queue` can be used to specify callback queue other than this RPC's exclusive queue.
        """
        properties = dict()
        if await_response:
            correlation_id = str(uuid.uuid4())
            properties = {
                'reply_to': callback_queue or self.callback_queue,
                'correlation_id': correlation_id,
            }
        logger.debug(f'< send_rpc: destination {destination}, data {data}, ttl {ttl}, properties {properties}')
        await self.channel.basic_publish(
            exchange_name=exchange,
            routing_key=destination,
            properties=properties,
            payload=data.encode('utf-8'),
        )

        if await_response:
            data = await self.await_response(correlation_id=correlation_id, ttl=ttl)
            data = data.decode('utf-8')
            logger.debug(f'> send_rpc: response {data}')
            return data
