from __future__ import annotations

import hashlib
import hmac
from typing import Any, List, Mapping, Optional, Sequence

import redis
import redis.asyncio as aioredis


class SwarmKeyDbError(Exception):
    pass


class KeyNotFoundError(SwarmKeyDbError):
    pass


class PrivacyMode:
    NONE = "none"
    OBLIVIOUS_HASHING = "oblivious_hashing"
    FULL_PSI = "full_psi"


class SwarmKeyDb:
    def __init__(
        self,
        host: str,
        port: int,
        password: Optional[str] = None,
        redis_client: Any = None,
        privacy_mode: str = PrivacyMode.NONE,
        privacy_key: Optional[str] = None,
    ):
        self._client = redis_client or redis.Redis(host=host, port=port, password=password, decode_responses=True)
        self._privacy_mode = privacy_mode
        self._privacy_key = privacy_key
        self._token_to_plain: dict[str, str] = {}

    def get(self, key: str) -> Optional[str]:
        _validate_key(key)
        return self._client.get(self._tokenize_key(key))

    def get_or_raise(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise KeyNotFoundError(f"Key not found: {key}")
        return value

    def put(self, key: str, value: str) -> None:
        _validate_key(key)
        token = self._tokenize_key(key)
        self._client.set(token, value)
        self._remember_key(token, key)

    def delete(self, key: str) -> bool:
        _validate_key(key)
        token = self._tokenize_key(key)
        deleted = self._client.delete(token) > 0
        if deleted:
            self._token_to_plain.pop(token, None)
        return deleted

    def list(self, pattern: str = "*") -> List[str]:
        if self._privacy_mode != PrivacyMode.NONE:
            return [key for key in self._token_to_plain.values() if _matches_pattern(key, pattern)]
        return [
            key.decode("utf-8") if isinstance(key, bytes) else key
            for key in self._client.keys(pattern)
        ]

    def batch_get(self, keys: Sequence[str]) -> List[Optional[str]]:
        if not keys:
            return []
        for key in keys:
            _validate_key(key)
        return self._client.mget([self._tokenize_key(key) for key in keys])

    def batch_put(self, entries: Mapping[str, str]) -> None:
        if not entries:
            return
        for key in entries:
            _validate_key(key)
        tokenized = {self._tokenize_key(key): value for key, value in entries.items()}
        self._client.mset(tokenized)
        for key, token in zip(entries.keys(), tokenized.keys()):
            self._remember_key(token, key)

    def set_with_ttl(self, key: str, value: str, ttl_seconds: int) -> None:
        _validate_key(key)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        token = self._tokenize_key(key)
        self._client.setex(token, ttl_seconds, value)
        self._remember_key(token, key)

    def backup(self) -> str:
        return self._client.execute_command("BACKUP")

    def restore(self, ref: str, key: Optional[str] = None) -> int:
        _validate_non_empty_string(ref, name="ref")
        args = ["RESTOREDB", ref]
        if key:
            args.append(key)
        return int(self._client.execute_command(*args))

    def rotate_key(self, old_key: str, new_key: str) -> str:
        _validate_non_empty_string(old_key, name="old_key")
        _validate_non_empty_string(new_key, name="new_key")
        return self._client.execute_command("ROTATEKEY", old_key, new_key)

    def _tokenize_key(self, key: str) -> str:
        if self._privacy_mode == PrivacyMode.NONE:
            return key
        if not self._privacy_key:
            raise ValueError("privacy_key must be set when privacy_mode is enabled")
        return hmac.new(bytes.fromhex(self._privacy_key), key.encode("utf-8"), hashlib.sha256).hexdigest()

    def _remember_key(self, token: str, key: str) -> None:
        if self._privacy_mode != PrivacyMode.NONE:
            self._token_to_plain[token] = key

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
    def __init__(
        self,
        host: str,
        port: int,
        password: Optional[str] = None,
        redis_client: Any = None,
        privacy_mode: str = PrivacyMode.NONE,
        privacy_key: Optional[str] = None,
    ):
        self._client = redis_client or aioredis.Redis(host=host, port=port, password=password, decode_responses=True)
        self._privacy_mode = privacy_mode
        self._privacy_key = privacy_key
        self._token_to_plain: dict[str, str] = {}

    async def get(self, key: str) -> Optional[str]:
        _validate_key(key)
        return await self._client.get(self._tokenize_key(key))

    async def get_or_raise(self, key: str) -> str:
        value = await self.get(key)
        if value is None:
            raise KeyNotFoundError(f"Key not found: {key}")
        return value

    async def put(self, key: str, value: str) -> None:
        _validate_key(key)
        token = self._tokenize_key(key)
        await self._client.set(token, value)
        self._remember_key(token, key)

    async def delete(self, key: str) -> bool:
        _validate_key(key)
        token = self._tokenize_key(key)
        deleted = await self._client.delete(token) > 0
        if deleted:
            self._token_to_plain.pop(token, None)
        return deleted

    async def list(self, pattern: str = "*") -> List[str]:
        if self._privacy_mode != PrivacyMode.NONE:
            return [key for key in self._token_to_plain.values() if _matches_pattern(key, pattern)]
        keys = await self._client.keys(pattern)
        return [key.decode("utf-8") if isinstance(key, bytes) else key for key in keys]

    async def batch_get(self, keys: Sequence[str]) -> List[Optional[str]]:
        if not keys:
            return []
        for key in keys:
            _validate_key(key)
        return await self._client.mget([self._tokenize_key(key) for key in keys])

    async def batch_put(self, entries: Mapping[str, str]) -> None:
        if not entries:
            return
        for key in entries:
            _validate_key(key)
        tokenized = {self._tokenize_key(key): value for key, value in entries.items()}
        await self._client.mset(tokenized)
        for key, token in zip(entries.keys(), tokenized.keys()):
            self._remember_key(token, key)

    async def set_with_ttl(self, key: str, value: str, ttl_seconds: int) -> None:
        _validate_key(key)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        token = self._tokenize_key(key)
        await self._client.setex(token, ttl_seconds, value)
        self._remember_key(token, key)

    async def backup(self) -> str:
        return await self._client.execute_command("BACKUP")

    async def restore(self, ref: str, key: Optional[str] = None) -> int:
        _validate_non_empty_string(ref, name="ref")
        args = ["RESTOREDB", ref]
        if key:
            args.append(key)
        return int(await self._client.execute_command(*args))

    async def rotate_key(self, old_key: str, new_key: str) -> str:
        _validate_non_empty_string(old_key, name="old_key")
        _validate_non_empty_string(new_key, name="new_key")
        return await self._client.execute_command("ROTATEKEY", old_key, new_key)

    def _tokenize_key(self, key: str) -> str:
        if self._privacy_mode == PrivacyMode.NONE:
            return key
        if not self._privacy_key:
            raise ValueError("privacy_key must be set when privacy_mode is enabled")
        return hmac.new(bytes.fromhex(self._privacy_key), key.encode("utf-8"), hashlib.sha256).hexdigest()

    def _remember_key(self, token: str, key: str) -> None:
        if self._privacy_mode != PrivacyMode.NONE:
            self._token_to_plain[token] = key

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


def _validate_non_empty_string(value: str, name: str = "value") -> None:
    if not isinstance(value, str) or len(value) == 0:
        raise ValueError(f"{name} must be a non-empty string")


def _matches_pattern(key: str, pattern: str) -> bool:
    if pattern in ("", "*"):
        return True
    if "*" not in pattern:
        return key == pattern
    return key.startswith(pattern.split("*", 1)[0])
