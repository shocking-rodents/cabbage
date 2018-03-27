# -*- coding: utf-8 -*-
import asyncio
import random

from cabbage import AsyncAmqpRpc, AmqpConnection


async def main():
    connection = AmqpConnection()
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()

    while True:
        n = random.randint(1, 100)
        hex_n = await rpc.send_rpc('my_queue', str(n))
        print(n, hex_n)
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
