from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping

from .client import CoordinatorClient
from .errors import InvalidRoutingMetadataError, NoAvailableShardError, RedisStreamWriteError
from .hashing import unbiased_shard_index
from .models import ProducerRoutingResponse, ProducerRoutingShard
from .protocol import DEFAULT_PUBLISH_MAX_ATTEMPTS, DEFAULT_ROUTING_REFRESH_SECONDS, DEFAULT_XADD_MAX_LEN
from .redis_stream import RedisStreamCommands


@dataclass(frozen=True)
class ProducerRoute:
    shard_index: int
    stream_key: str
    metadata_version: int


@dataclass(frozen=True)
class PublishedRedisStreamMessage:
    stream_key: str
    shard_index: int
    record_id: str
    metadata_version: int


class ProducerRoutingCache:
    """Caches coordinator producer routing metadata and maps partition keys to shards."""

    def __init__(
        self,
        coordinator_client: CoordinatorClient,
        stream_prefix: str,
        consumer_group: str,
        *,
        refresh_interval: float = DEFAULT_ROUTING_REFRESH_SECONDS,
    ):
        self.coordinator_client = coordinator_client
        self.stream_prefix = stream_prefix
        self.consumer_group = consumer_group
        self.refresh_interval = refresh_interval
        self._lock = threading.RLock()
        self._metadata: ProducerRoutingResponse | None = None
        self._loaded_at = 0.0

    def metadata(self, *, force_refresh: bool = False) -> ProducerRoutingResponse:
        with self._lock:
            now = time.monotonic()
            expired = now - self._loaded_at >= self.refresh_interval
            if force_refresh or self._metadata is None or expired:
                metadata = self.coordinator_client.producer_routing(self.stream_prefix, self.consumer_group)
                self._validate_metadata(metadata)
                self._metadata = metadata
                self._loaded_at = now
            return self._metadata

    def invalidate(self) -> None:
        with self._lock:
            self._metadata = None
            self._loaded_at = 0.0

    def route(self, partition_key: str | bytes) -> ProducerRoute:
        key_bytes = partition_key.encode("utf-8") if isinstance(partition_key, str) else partition_key
        metadata = self.metadata()
        if metadata.shard_count <= 0 or not metadata.shards:
            raise NoAvailableShardError(
                f"producer routing for {self.stream_prefix}/{self.consumer_group} has no active shard"
            )
        index = unbiased_shard_index(key_bytes, metadata.shard_count)
        shard = _shard_by_index(metadata.shards, index)
        if shard is None:
            raise NoAvailableShardError(f"producer routing has no shard metadata for index {index}")
        return ProducerRoute(index, shard.stream_key, metadata.metadata_version)

    def _validate_metadata(self, metadata: ProducerRoutingResponse) -> None:
        if metadata.stream_prefix != self.stream_prefix:
            raise InvalidRoutingMetadataError(
                f"routing streamPrefix mismatch: expected {self.stream_prefix}, got {metadata.stream_prefix}"
            )
        if metadata.consumer_group != self.consumer_group:
            raise InvalidRoutingMetadataError(
                f"routing consumerGroup mismatch: expected {self.consumer_group}, got {metadata.consumer_group}"
            )
        if metadata.shard_count < 0:
            raise InvalidRoutingMetadataError("routing shardCount must not be negative")
        if metadata.shard_count > 0 and not metadata.shards:
            raise InvalidRoutingMetadataError("routing shard list must not be empty when shardCount is positive")


class RedisStreamPublisher:
    """Publishes records to Redis Streams using coordinator-managed routing metadata."""

    def __init__(
        self,
        routing_cache: ProducerRoutingCache,
        commands: RedisStreamCommands,
        *,
        publish_max_attempts: int = DEFAULT_PUBLISH_MAX_ATTEMPTS,
        xadd_max_len: int = DEFAULT_XADD_MAX_LEN,
        xadd_approximate_trimming: bool = True,
    ):
        if publish_max_attempts < 1:
            raise ValueError("publish_max_attempts must be positive")
        self.routing_cache = routing_cache
        self.commands = commands
        self.publish_max_attempts = publish_max_attempts
        self.xadd_max_len = xadd_max_len
        self.xadd_approximate_trimming = xadd_approximate_trimming

    def publish(
        self,
        partition_key: str | bytes,
        fields: Mapping[str, Any],
        *,
        max_len: int | None = None,
        approximate_trimming: bool | None = None,
    ) -> PublishedRedisStreamMessage:
        last_error: Exception | None = None
        for attempt in range(self.publish_max_attempts):
            if attempt > 0:
                self.routing_cache.invalidate()
            route = self.routing_cache.route(partition_key)
            try:
                record_id = self.commands.xadd_nomkstream(
                    route.stream_key,
                    fields,
                    max_len=max_len or self.xadd_max_len,
                    approximate_trimming=(
                        self.xadd_approximate_trimming
                        if approximate_trimming is None
                        else approximate_trimming
                    ),
                )
                return PublishedRedisStreamMessage(
                    stream_key=route.stream_key,
                    shard_index=route.shard_index,
                    record_id=record_id,
                    metadata_version=route.metadata_version,
                )
            except RedisStreamWriteError as exc:
                last_error = exc
                self.routing_cache.invalidate()
        assert last_error is not None
        raise last_error


def _shard_by_index(shards: list[ProducerRoutingShard], shard_index: int) -> ProducerRoutingShard | None:
    for shard in shards:
        if shard.shard_index == shard_index:
            return shard
    return None
