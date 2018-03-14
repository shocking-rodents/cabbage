# -*- coding: utf-8 -*-
import asyncio

from cabbage import AmqpConnection, AsyncAmqpRpc


async def handler(request: str) -> str:
    n = int(request)
    print(n)
    return str(hex(n))


async def main():
    connection = AmqpConnection()
    rpc = AsyncAmqpRpc(connection=connection, request_handler=handler)
    await rpc.connect()
    await rpc.subscribe('my_queue')
    await rpc.run_server()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
