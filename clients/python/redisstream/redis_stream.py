from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .errors import RedisStreamWriteError, UnsupportedRedisCommandError


@dataclass(frozen=True)
class RedisCommandSupport:
    server_version: str
    supports_xackdel: bool
    supports_xnack: bool

    @classmethod
    def from_info(cls, info: Mapping[str, Any]) -> "RedisCommandSupport":
        version = str(info.get("redis_version") or info.get("valkey_version") or "0.0.0")
        major, minor, patch = _parse_version(version)
        supports_xackdel = (major, minor, patch) >= (8, 2, 0)
        supports_xnack = (major, minor, patch) >= (8, 8, 0)
        return cls(version, supports_xackdel, supports_xnack)


class RedisStreamCommands:
    """Centralized Redis Stream command template used by producer and consumer."""

    def __init__(self, redis_client: Any, command_support: RedisCommandSupport | None = None):
        self.redis_client = redis_client
        self.command_support = command_support or self._detect_command_support()

    def xadd_nomkstream(
        self,
        stream_key: str,
        fields: Mapping[str, Any],
        *,
        max_len: int,
        approximate_trimming: bool = True,
    ) -> str:
        if not stream_key:
            raise ValueError("stream_key must not be blank")
        if not fields:
            raise ValueError("Redis Stream message fields must not be empty")
        if max_len <= 0:
            raise ValueError("max_len must be positive")

        args: list[Any] = [
            "XADD",
            stream_key,
            "NOMKSTREAM",
            "MAXLEN",
            "~" if approximate_trimming else "=",
            str(max_len),
            "*",
        ]
        for field, value in fields.items():
            if not str(field):
                raise ValueError("Redis Stream message field names must not be blank")
            args.append(str(field))
            args.append(str(value))
        try:
            response = self.redis_client.execute_command(*args)
        except Exception as exc:  # redis-py raises ResponseError for NOMKSTREAM/stale-route failures.
            raise RedisStreamWriteError(str(exc)) from exc
        if response is None:
            raise RedisStreamWriteError(
                f"Redis XADD NOMKSTREAM returned no record id for {stream_key}; the stream key may not exist"
            )
        return _decode(response)

    def xreadgroup(
        self,
        stream_keys: list[str],
        consumer_group: str,
        consumer_name: str,
        *,
        count: int,
        block_ms: int,
    ) -> Any:
        if not stream_keys:
            return []
        ids = [">"] * len(stream_keys)
        return self.redis_client.execute_command(
            "XREADGROUP",
            "GROUP",
            consumer_group,
            consumer_name,
            "COUNT",
            str(count),
            "BLOCK",
            str(block_ms),
            "STREAMS",
            *stream_keys,
            *ids,
        )

    def xack(self, stream_key: str, consumer_group: str, record_id: str) -> int:
        return int(self.redis_client.execute_command("XACK", stream_key, consumer_group, record_id) or 0)

    def xackdel(self, stream_key: str, consumer_group: str, reference_policy: str, record_id: str) -> None:
        if not self.command_support.supports_xackdel:
            raise UnsupportedRedisCommandError(
                f"Redis {self.command_support.server_version} does not support XACKDEL; Redis 8.2.0 or newer is required"
            )
        self.redis_client.execute_command(
            "XACKDEL",
            stream_key,
            consumer_group,
            reference_policy,
            "IDS",
            "1",
            record_id,
        )

    def xnack(
        self,
        stream_key: str,
        consumer_group: str,
        mode: str,
        record_id: str,
        *,
        retry_count: int | None = None,
        force: bool = False,
    ) -> None:
        if not self.command_support.supports_xnack:
            raise UnsupportedRedisCommandError(
                f"Redis {self.command_support.server_version} does not support XNACK; Redis 8.8.0 or newer is required"
            )
        args: list[Any] = ["XNACK", stream_key, consumer_group, mode, "IDS", "1", record_id]
        if retry_count is not None:
            if retry_count < 0:
                raise ValueError("retry_count must be non-negative")
            args.extend(["RETRYCOUNT", str(retry_count)])
        if force:
            args.append("FORCE")
        self.redis_client.execute_command(*args)

    def _detect_command_support(self) -> RedisCommandSupport:
        try:
            info = self.redis_client.info("server")
        except TypeError:
            info = self.redis_client.info()
        return RedisCommandSupport.from_info(info)


class RedisStreamReader:
    def __init__(self, commands: RedisStreamCommands, consumer_group: str, consumer_name: str):
        self.commands = commands
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name
        self._cursor = 0

    def poll_round_robin(self, stream_keys: list[str], *, count: int, timeout_ms: int) -> Any:
        if not stream_keys:
            return []
        rotated = stream_keys[self._cursor :] + stream_keys[: self._cursor]
        self._cursor = (self._cursor + 1) % len(stream_keys)
        return self.commands.xreadgroup(
            rotated,
            self.consumer_group,
            self.consumer_name,
            count=count,
            block_ms=timeout_ms,
        )

    def poll_messages_round_robin(self, stream_keys: list[str], *, count: int, timeout_ms: int) -> list["RedisStreamMessage"]:
        response = self.poll_round_robin(stream_keys, count=count, timeout_ms=timeout_ms)
        return [
            RedisStreamMessage(
                stream_key=stream_key,
                record_id=record_id,
                fields=fields,
                commands=self.commands,
                consumer_group=self.consumer_group,
            )
            for stream_key, record_id, fields in normalize_xreadgroup_response(response)
        ]


class RedisStreamMessage:
    """Message wrapper passed to stream listener functions."""

    def __init__(
        self,
        *,
        stream_key: str,
        record_id: str,
        fields: Mapping[str, Any],
        commands: RedisStreamCommands,
        consumer_group: str,
    ):
        self.stream_key = stream_key
        self.record_id = record_id
        self.fields = dict(fields)
        self._commands = commands
        self._consumer_group = consumer_group

    def ack(self) -> int:
        """Acknowledges this record with XACK after business processing succeeds."""
        return self._commands.xack(self.stream_key, self._consumer_group, self.record_id)

    def ack_del(self, reference_policy: str = "ACKED") -> None:
        """Acknowledges and deletes this record with XACKDEL when Redis supports it."""
        self._commands.xackdel(self.stream_key, self._consumer_group, reference_policy, self.record_id)

    def nack(self, mode: str = "SILENT", *, retry_count: int | None = None, force: bool = False) -> None:
        """Releases a failed record with XNACK when Redis supports it."""
        self._commands.xnack(
            self.stream_key,
            self._consumer_group,
            mode,
            self.record_id,
            retry_count=retry_count,
            force=force,
        )


def _parse_version(version: str) -> tuple[int, int, int]:
    parts = version.split("-", 1)[0].split(".")
    nums = []
    for part in parts[:3]:
        try:
            nums.append(int(part))
        except ValueError:
            nums.append(0)
    while len(nums) < 3:
        nums.append(0)
    return nums[0], nums[1], nums[2]


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def normalize_xreadgroup_response(response: Any) -> list[tuple[str, str, dict[str, str]]]:
    """Normalizes redis-py/raw XREADGROUP response into stream key, record id, field map tuples."""
    if not response:
        return []
    normalized: list[tuple[str, str, dict[str, str]]] = []
    for stream_entry in response:
        if len(stream_entry) != 2:
            continue
        stream_key = _decode(stream_entry[0])
        records = stream_entry[1] or []
        for record in records:
            if len(record) != 2:
                continue
            record_id = _decode(record[0])
            fields = _normalize_fields(record[1])
            normalized.append((stream_key, record_id, fields))
    return normalized


def _normalize_fields(fields: Any) -> dict[str, str]:
    if isinstance(fields, Mapping):
        return {_decode(key): _decode(value) for key, value in fields.items()}
    if isinstance(fields, list) or isinstance(fields, tuple):
        result: dict[str, str] = {}
        iterator = iter(fields)
        for key in iterator:
            try:
                value = next(iterator)
            except StopIteration:
                break
            result[_decode(key)] = _decode(value)
        return result
    return {}
