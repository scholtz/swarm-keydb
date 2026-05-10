import asyncio
from swarm_keydb_client import AsyncSwarmKeyDbClient


async def main() -> None:
    client = AsyncSwarmKeyDbClient("ws://127.0.0.1:8765/")
    pub = AsyncSwarmKeyDbClient("ws://127.0.0.1:8765/")

    await client.connect()
    await pub.connect()

    await client.set("hello", "world")
    print(await client.get("hello"))

    async with client.subscribe("quickstart:news") as channel:
        await pub.publish("quickstart:news", "swarm-keydb-ready")
        async for message in channel.listen():
            print(message["data"])
            break

    await pub.close()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
