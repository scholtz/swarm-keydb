import asyncio

from swarm_keydb import AsyncSwarmKeyDb


async def main():
    client = AsyncSwarmKeyDb(host="127.0.0.1", port=6379)
    await client.set_with_ttl("chat:room1:last", '{"from":"alice","text":"hi"}', 60)
    print(await client.list("chat:*"))
    await client.close()


asyncio.run(main())
