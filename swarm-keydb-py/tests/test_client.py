import asyncio
import unittest

from swarm_keydb import AsyncSwarmKeyDb, KeyNotFoundError, SwarmKeyDb


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0

    def keys(self, pattern):
        if pattern == "*":
            return list(self.store.keys())
        prefix = pattern.replace("*", "")
        return [k for k in self.store if k.startswith(prefix)]

    def mget(self, keys):
        return [self.store.get(k) for k in keys]

    def mset(self, items):
        self.store.update(items)

    def setex(self, key, ttl, value):
        self.store[key] = value


class FakeAsyncRedis(FakeRedis):
    async def get(self, key):
        return super().get(key)

    async def set(self, key, value):
        super().set(key, value)

    async def delete(self, key):
        return super().delete(key)

    async def keys(self, pattern):
        return super().keys(pattern)

    async def mget(self, keys):
        return super().mget(keys)

    async def mset(self, items):
        super().mset(items)

    async def setex(self, key, ttl, value):
        super().setex(key, ttl, value)

    async def close(self):
        return None


class SwarmKeyDbTests(unittest.TestCase):
    def test_sync_happy_path(self):
        db = SwarmKeyDb(host="localhost", port=6379, redis_client=FakeRedis())
        db.put("a", "1")
        self.assertEqual(db.get("a"), "1")
        self.assertTrue(db.delete("a"))
        self.assertEqual(db.list(), [])

    def test_sync_batch_and_ttl(self):
        db = SwarmKeyDb(host="localhost", port=6379, redis_client=FakeRedis())
        db.batch_put({"k1": "v1", "k2": "v2"})
        self.assertEqual(db.batch_get(["k1", "k2", "k3"]), ["v1", "v2", None])
        db.set_with_ttl("temp", "x", 10)
        self.assertEqual(db.get("temp"), "x")
        db.batch_put({"k3": "v3"})
        self.assertEqual(db.batch_get(["k3"]), ["v3"])
        db.set_with_ttl("temp2", "y", 1)
        self.assertEqual(db.get("temp2"), "y")

    def test_sync_errors(self):
        db = SwarmKeyDb(host="localhost", port=6379, redis_client=FakeRedis())
        with self.assertRaises(ValueError):
            db.put("", "x")
        with self.assertRaises(ValueError):
            db.set_with_ttl("a", "b", 0)
        with self.assertRaises(KeyNotFoundError):
            db.get_or_raise("missing")


class AsyncSwarmKeyDbTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_happy_path(self):
        db = AsyncSwarmKeyDb(host="localhost", port=6379, redis_client=FakeAsyncRedis())
        await db.put("a", "1")
        self.assertEqual(await db.get("a"), "1")
        self.assertTrue(await db.delete("a"))

    async def test_async_batch_and_ttl(self):
        db = AsyncSwarmKeyDb(host="localhost", port=6379, redis_client=FakeAsyncRedis())
        await db.batch_put({"k1": "v1", "k2": "v2"})
        self.assertEqual(await db.batch_get(["k1", "k2", "k3"]), ["v1", "v2", None])
        await db.set_with_ttl("temp", "x", 3)
        await db.batch_put({"k3": "v3"})
        self.assertEqual(await db.batch_get(["k3"]), ["v3"])
        await db.set_with_ttl("temp2", "y", 3)
        self.assertEqual(await db.list("k*"), ["k1", "k2", "k3"])
        await db.close()

    async def test_async_errors(self):
        db = AsyncSwarmKeyDb(host="localhost", port=6379, redis_client=FakeAsyncRedis())
        with self.assertRaises(ValueError):
            await db.put("", "x")
        with self.assertRaises(ValueError):
            await db.set_with_ttl("a", "b", -1)
        with self.assertRaises(KeyNotFoundError):
            await db.get_or_raise("missing")


if __name__ == "__main__":
    unittest.main()
