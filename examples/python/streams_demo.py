"""Redis Streams demo using AsyncSwarmKeyDbClient.

Demonstrates:
- XADD / XRANGE / XREVRANGE / XLEN
- XREAD (non-blocking and with offset)
- Consumer groups: XGROUP CREATE, XREADGROUP, XACK
- XTRIM

Run:
    python examples/python/streams_demo.py
"""

import asyncio
import os

from swarm_keydb_client import AsyncSwarmKeyDbClient

WS_URL = os.environ.get("SWARM_KEYDB_WS_URL", "ws://127.0.0.1:8765/")
STREAM = "demo:events"
GROUP = "demo-consumers"


async def main() -> None:
    client = AsyncSwarmKeyDbClient(WS_URL)
    await client.connect()

    # Clean up from previous runs
    await client.delete(STREAM)

    # -----------------------------------------------------------------------
    # Append entries
    # -----------------------------------------------------------------------
    print("Appending entries …")
    ids = []
    for i in range(5):
        eid = await client.xadd(STREAM, {"index": str(i), "sensor": "temp", "value": str(20 + i)})
        ids.append(eid)
        print(f"  XADD => {eid}")

    # -----------------------------------------------------------------------
    # XLEN
    # -----------------------------------------------------------------------
    length = await client.xlen(STREAM)
    print(f"XLEN {STREAM} => {length}")

    # -----------------------------------------------------------------------
    # XRANGE
    # -----------------------------------------------------------------------
    print("\nXRANGE - (oldest to newest):")
    entries = await client.xrange(STREAM, "-", "+")
    for eid, fields in entries:
        print(f"  {eid}: {fields}")

    # -----------------------------------------------------------------------
    # XREVRANGE
    # -----------------------------------------------------------------------
    print("\nXREVRANGE + (newest to oldest, limit 2):")
    entries = await client.xrevrange(STREAM, "+", "-", count=2)
    for eid, fields in entries:
        print(f"  {eid}: {fields}")

    # -----------------------------------------------------------------------
    # XREAD from the beginning
    # -----------------------------------------------------------------------
    print("\nXREAD from beginning:")
    result = await client.xread({STREAM: "0"}, count=3)
    if result:
        stream_name, stream_entries = result[0]
        print(f"  Stream: {stream_name}  ({len(stream_entries)} entries)")

    # -----------------------------------------------------------------------
    # Consumer groups
    # -----------------------------------------------------------------------
    print("\nCreating consumer group …")
    await client.xgroup_create(STREAM, GROUP, "0")

    print("XREADGROUP (consumer1, read all pending) …")
    result = await client.xreadgroup(GROUP, "consumer1", {STREAM: ">"})
    if result:
        _, g_entries = result[0]
        print(f"  Delivered {len(g_entries)} entry(ies) to consumer1")
        for eid, fields in g_entries:
            acked = await client.xack(STREAM, GROUP, eid)
            print(f"  XACK {eid} => {acked}")

    # -----------------------------------------------------------------------
    # XTRIM
    # -----------------------------------------------------------------------
    trimmed = await client.xtrim(STREAM, maxlen=3, approximate=False)
    print(f"\nXTRIM to 3 => trimmed {trimmed} entry(ies)")
    print(f"XLEN after trim => {await client.xlen(STREAM)}")

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------
    await client.xgroup_destroy(STREAM, GROUP)
    await client.delete(STREAM)
    await client.close()
    print("\nStreams demo done.")


if __name__ == "__main__":
    asyncio.run(main())
