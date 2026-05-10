"""Pub/Sub demo using AsyncSwarmKeyDbClient.

Demonstrates:
- Channel subscribe / publish via WebSocket
- Pattern subscribe with glob matching
- Async generator message delivery

Run:
    python examples/python/pubsub_demo.py
"""

import asyncio
import os

from swarm_keydb_client import AsyncSwarmKeyDbClient

WS_URL = os.environ.get("SWARM_KEYDB_WS_URL", "ws://127.0.0.1:8765/")


async def channel_demo() -> None:
    """Subscribe on one connection, publish on another, print the message."""
    publisher = AsyncSwarmKeyDbClient(WS_URL)
    subscriber = AsyncSwarmKeyDbClient(WS_URL)

    await publisher.connect()
    await subscriber.connect()

    print("Subscribing to 'news' channel …")
    async with subscriber.subscribe("news") as ch:
        # Give the subscription a moment to register server-side
        await asyncio.sleep(0.1)

        # Publish a message
        count = await publisher.publish("news", "SwarmKeyDb is live!")
        print(f"Published to {count} subscriber(s)")

        # Read a single message then exit
        async for msg in ch.listen():
            print(f"Received: channel={msg['channel']!r}  data={msg['data']!r}")
            break

    await publisher.close()
    await subscriber.close()
    print("Channel demo done.")


async def pattern_demo() -> None:
    """Subscribe to all 'sensor.*' topics using a glob pattern."""
    publisher = AsyncSwarmKeyDbClient(WS_URL)
    subscriber = AsyncSwarmKeyDbClient(WS_URL)

    await publisher.connect()
    await subscriber.connect()

    received: list[dict] = []

    def on_message(msg: dict) -> None:
        received.append(msg)
        print(
            f"Pattern match: pattern={msg.get('pattern')!r}  "
            f"channel={msg.get('channel')!r}  data={msg.get('data')!r}"
        )

    await subscriber.psubscribe("sensor.*", handler=on_message)
    await asyncio.sleep(0.1)

    await publisher.publish("sensor.temperature", "22.5")
    await publisher.publish("sensor.humidity", "60%")
    await asyncio.sleep(0.2)

    await subscriber.punsubscribe("sensor.*")
    await publisher.close()
    await subscriber.close()
    print(f"Pattern demo done. Received {len(received)} message(s).")


if __name__ == "__main__":
    print("=== Channel Pub/Sub demo ===")
    asyncio.run(channel_demo())
    print()
    print("=== Pattern Pub/Sub demo ===")
    asyncio.run(pattern_demo())
