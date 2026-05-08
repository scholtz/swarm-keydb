from __future__ import annotations

from typing import Any, List, Mapping, Optional, Sequence

import redis
import redis.asyncio as aioredis


class SwarmKeyDbError(Exception):
    pass


class KeyNotFoundError(SwarmKeyDbError):
    pass


class SwarmKeyDb:
    def __init__(self, host: str, port: int, password: Optional[str] = None, redis_client: Any = None):
        self._client = redis_client or redis.Redis(host=host, port=port, password=password, decode_responses=True)

    def get(self, key: str) -> Optional[str]:
        _validate_key(key)
        return self._client.get(key)

    def get_or_raise(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise KeyNotFoundError(f"Key not found: {key}")
        return value

    def put(self, key: str, value: str) -> None:
        _validate_key(key)
        self._client.set(key, value)

    def delete(self, key: str) -> bool:
        _validate_key(key)
        return self._client.delete(key) > 0

    def list(self, pattern: str = "*") -> List[str]:
        return [
            key.decode("utf-8") if isinstance(key, bytes) else key
            for key in self._client.keys(pattern)
        ]

    def batch_get(self, keys: Sequence[str]) -> List[Optional[str]]:
        if not keys:
            return []
        for key in keys:
            _validate_key(key)
        return self._client.mget(keys)

    def batch_put(self, entries: Mapping[str, str]) -> None:
        if not entries:
            return
        for key in entries:
            _validate_key(key)
        self._client.mset(dict(entries))

    def set_with_ttl(self, key: str, value: str, ttl_seconds: int) -> None:
        _validate_key(key)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        self._client.setex(key, ttl_seconds, value)

    def backup(self) -> str:
        return self._client.execute_command("BACKUP")

    def restore(self, ref: str, key: Optional[str] = None) -> int:
        _validate_ref(ref)
        args = ["RESTOREDB", ref]
        if key:
            args.append(key)
        return int(self._client.execute_command(*args))

    def rotate_key(self, old_key: str, new_key: str) -> str:
        _validate_ref(old_key, name="old_key")
        _validate_ref(new_key, name="new_key")
        return self._client.execute_command("ROTATEKEY", old_key, new_key)

    def batchGet(self, keys: Sequence[str]) -> List[Optional[str]]:
        """Compatibility alias for `batch_get`; prefer snake_case in Python code."""
        return self.batch_get(keys)

    def batchPut(self, entries: Mapping[str, str]) -> None:
        """Compatibility alias for `batch_put`; prefer snake_case in Python code."""
        self.batch_put(entries)

    def setWithTTL(self, key: str, value: str, ttl_seconds: int) -> None:
        """Compatibility alias for `set_with_ttl`; prefer snake_case in Python code."""
        self.set_with_ttl(key, value, ttl_seconds)


class AsyncSwarmKeyDb:
    def __init__(self, host: str, port: int, password: Optional[str] = None, redis_client: Any = None):
        self._client = redis_client or aioredis.Redis(host=host, port=port, password=password, decode_responses=True)

    async def get(self, key: str) -> Optional[str]:
        _validate_key(key)
        return await self._client.get(key)

    async def get_or_raise(self, key: str) -> str:
        value = await self.get(key)
        if value is None:
            raise KeyNotFoundError(f"Key not found: {key}")
        return value

    async def put(self, key: str, value: str) -> None:
        _validate_key(key)
        await self._client.set(key, value)

    async def delete(self, key: str) -> bool:
        _validate_key(key)
        return await self._client.delete(key) > 0

    async def list(self, pattern: str = "*") -> List[str]:
        keys = await self._client.keys(pattern)
        return [key.decode("utf-8") if isinstance(key, bytes) else key for key in keys]

    async def batch_get(self, keys: Sequence[str]) -> List[Optional[str]]:
        if not keys:
            return []
        for key in keys:
            _validate_key(key)
        return await self._client.mget(keys)

    async def batch_put(self, entries: Mapping[str, str]) -> None:
        if not entries:
            return
        for key in entries:
            _validate_key(key)
        await self._client.mset(dict(entries))

    async def set_with_ttl(self, key: str, value: str, ttl_seconds: int) -> None:
        _validate_key(key)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        await self._client.setex(key, ttl_seconds, value)

    async def backup(self) -> str:
        return await self._client.execute_command("BACKUP")

    async def restore(self, ref: str, key: Optional[str] = None) -> int:
        _validate_ref(ref)
        args = ["RESTOREDB", ref]
        if key:
            args.append(key)
        return int(await self._client.execute_command(*args))

    async def rotate_key(self, old_key: str, new_key: str) -> str:
        _validate_ref(old_key, name="old_key")
        _validate_ref(new_key, name="new_key")
        return await self._client.execute_command("ROTATEKEY", old_key, new_key)

    async def batchGet(self, keys: Sequence[str]) -> List[Optional[str]]:
        """Compatibility alias for `batch_get`; prefer snake_case in Python code."""
        return await self.batch_get(keys)

    async def batchPut(self, entries: Mapping[str, str]) -> None:
        """Compatibility alias for `batch_put`; prefer snake_case in Python code."""
        await self.batch_put(entries)

    async def setWithTTL(self, key: str, value: str, ttl_seconds: int) -> None:
        """Compatibility alias for `set_with_ttl`; prefer snake_case in Python code."""
        await self.set_with_ttl(key, value, ttl_seconds)

    async def close(self) -> None:
        await self._client.close()


def _validate_key(key: str) -> None:
    if not isinstance(key, str) or len(key) == 0:
        raise ValueError("key must be a non-empty string")


def _validate_ref(value: str, name: str = "ref") -> None:
    if not isinstance(value, str) or len(value) == 0:
        raise ValueError(f"{name} must be a non-empty string")
