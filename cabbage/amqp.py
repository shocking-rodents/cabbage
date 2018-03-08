# -*- coding: utf-8 -*-
import inspect
import uuid
from itertools import cycle
from random import shuffle
from typing import Optional, Callable, Union, Awaitable, Mapping
import logging
import asyncio

import aioamqp
from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties
from aioamqp.protocol import CONNECTING, OPEN

logger = logging.getLogger('cabbage')


class ServiceUnavailableError(Exception):
    """External service unavailable. """


class AmqpConnection:
    def __init__(self, host='localhost', port=5672, username='guest', password='guest', virtualhost='/', loop=None):
        """
        :param host: server host, default localhost
        :param port: server port, default 5672
        :param username: AMQP login, default guest
        :param password: AMQP password, default guest
        :param virtualhost: AMQP virtual host, default /
        :param loop: asyncio event loop, default current event loop
        """
        self.loop = loop or asyncio.get_event_loop()
        self.username = username
        self.password = password
        self.virtualhost = virtualhost
        self.connection_cycle = cycle([(host, port)])
        self.transport = None
        self.protocol = None

    async def channel(self):
        if self.protocol is not None:
            return await self.protocol.channel()

    def cycle_rabbit_host(self):
        # TODO: use cycling
        shuffle(self.hosts)
        for host_post in cycle(self.hosts):
            host, port = host_post.split(':')
            port = int(port)
            yield host, port

    async def connect(self):
        """Connect to AMQP broker. On failure this function will endlessly try reconnecting.
        Do nothing if already connected or connecting.
        """
        if self.protocol is not None and self.protocol.state in [CONNECTING, OPEN]:
            return

        delay = 1.0
        for host, port in self.connection_cycle:
            try:
                self.transport, self.protocol = await aioamqp.connect(
                    loop=self.loop,
                    host=host,
                    port=port,
                    login=self.username,
                    password=self.password,
                    virtualhost=self.virtualhost,
                )
            except OSError as e:
                # Connection-related errors are mostly represented by `ConnectionError`,
                # except for some like `socket.gaierror`, which fall into broader `OSError`
                logger.warning(f'failed to connect to {host}:{port}, error <{e.__class__.__name__}> {e}, '
                               f'retrying in {int(delay)} seconds')
                await asyncio.sleep(int(delay))
                # exponentially increase delay up to 60 seconds
                # this looks like 1, 1, 2, 3, 5, 8, ...
                delay = min(delay * 1.537, 60.0)
            except Exception as e:
                logger.error(f'connection failed, not retrying: <{e.__class__.__name__}> {e}')
                raise
            else:
                logger.info(f'connected to {host}:{port}')
                break

    async def disconnect(self):
        if self.protocol is not None and self.protocol.state in [CONNECTING, OPEN]:
            await self.protocol.close()


class AsyncAmqpRpc:
    def __init__(self, connection: AmqpConnection,
                 request_handler: Union[
                     Callable[[str], Optional[str]],
                     Callable[[bytes], Optional[bytes]],
                     Callable[[str], Awaitable[Optional[str]]],
                     Callable[[bytes], Awaitable[Optional[bytes]]],
                     None
                 ] = None,
                 listen_queues=None, prefetch_count=1, raw=False, default_ttl=15.0):
        """
        All arguments are optional. If `request_handler` is not supplied or None, RPC works only in client mode.

        :param request_handler: request handler, can be a normal or coroutine function
                that maps either str->str or bytes->bytes. If `request_handler` returns None,
                it is taken to mean no response is needed.
        :param listen_queues: list of tuples (exchange, queue, routing_key, queue_params)
        :param raw: do not attempt decoding, use iff `request_handler` maps `bytes -> bytes`.
        :param prefetch_count: per-consumer prefetch message limit, default 1
        :param default_ttl: default timeout for awaiting response when sending remote calls
        """
        self.default_ttl = default_ttl
        self.listen_queues = listen_queues or []
        self.connection = connection
        self.request_handler = request_handler
        self.prefetch_count = prefetch_count
        self.raw = raw
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
            callback=self.on_response,
            queue_name=self.callback_queue,
        )
        logger.debug(f'listening on callback queue {result["queue"]}')

    async def subscribe(self, queue: str, exchange: str = '', routing_key: str = None,
                        exchange_params: Mapping = None, queue_params: Mapping = None):
        """
        Subscribe to a specific queue. Exchange and queue will be created if they do not exist.

        :param exchange: exchange name, default '' (default AMQP exchange)
        :param queue: queue name
        :param routing_key: routing key, default same as `queue`
        :param exchange_params: options for the exchange, default durable and type topic
        :param queue_params: options for the queue, default durable and DLX
        :return: consumer_tag
        """
        if self.request_handler is None:
            raise ValueError('Request handler is not set')
        if routing_key is None:
            routing_key = queue
        if exchange_params is None:
            exchange_params = dict(type_name='topic', durable=True)
        if queue_params is None:
            queue_params = dict(durable=True, arguments={
                'x-dead-letter-exchange': 'DLX',
                'x-dead-letter-routing-key': 'dlx_rpc',
            })
        if exchange != '':
            # operation not permitted on default exchange
            await self.channel.exchange_declare(exchange_name=exchange, **exchange_params)
        result = await self.channel.queue_declare(queue_name=queue, **queue_params)
        queue = result['queue']  # in case queue name was generated by broker
        if exchange != '':
            # operation not permitted on default exchange
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
            callback=self.on_request,
            queue_name=queue,
        )
        logger.debug(f'subscribed to queue {queue}, bound to exchange {exchange} with key {routing_key}')
        return result['consumer_tag']

    async def unsubscribe(self, consumer_tag):
        """
        Stop consuming on a queue.

        :param consumer_tag: consumer tag returned by `subscribe()`
        """
        return await self.channel.basic_cancel(consumer_tag)

    async def on_request(self, channel, body, envelope, properties):
        """Run handle() in background. """
        asyncio.ensure_future(self.handle_rpc(channel, body, envelope, properties))

    async def handle_rpc(self, channel: Channel, body: bytes, envelope: Envelope, properties: Properties):
        """Process request with handler and send response if needed. """
        for e in [channel, body, envelope, properties]:
            print(type(e), e)
        try:
            data = body if self.raw else body.decode('utf-8')
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
                    payload=response if self.raw else response.encode('utf-8'),
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
                    await self.subscribe(exchange, queue, routing_key, **queue_params)
                await self.connection.protocol.wait_closed()
        finally:
            await self.connection.disconnect()

    async def run(self, app=None):
        """aiohttp-compatible on_startup coroutine. """
        asyncio.ensure_future(self.main_loop())

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
        """Wait for a response with given correlation id. Blocks current Task. """
        self.responses[correlation_id] = asyncio.Future()
        try:
            await asyncio.wait_for(self.responses[correlation_id], timeout=ttl)
            return self.responses[correlation_id].result()
        except asyncio.TimeoutError:
            logger.warning(f'request {correlation_id} timed out')
            raise ServiceUnavailableError('Request timed out') from None
        finally:
            self.responses.pop(correlation_id)

    async def send_rpc(self, destination: str, data: str, exchange: str = '', ttl: float = None,
                       await_response=True) -> Optional[str]:
        """Execute a method on remote server.
        If `await_response` is True, the call blocks current Task until the result is returned.
        """
        if ttl is None:
            ttl = self.default_ttl
        properties = dict()
        if await_response:
            correlation_id = str(uuid.uuid4())
            properties = {
                'reply_to': self.callback_queue,
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
