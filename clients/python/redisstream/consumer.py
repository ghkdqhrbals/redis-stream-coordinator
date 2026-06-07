from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .client import CoordinatorClient
from .identity import default_member_id, split_member_ids
from .models import (
    CoordinatorShard,
    HeartbeatRequest,
    HeartbeatResponse,
    RuntimeConsumerCapacity,
)
from .producer import ProducerRoutingCache, RedisStreamPublisher
from .protocol import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_POLL_TIMEOUT_SECONDS,
    FENCED_MEMBER_EPOCH,
    OK,
    PROTOCOL_VERSION,
    RETRY,
    REVOKE_PENDING,
    SYNC_METADATA,
    UNKNOWN_MEMBER_ID,
)
from .redis_stream import RedisStreamCommands, RedisStreamMessage, RedisStreamReader

Handler = Callable[[RedisStreamMessage], None]


@dataclass(frozen=True)
class StreamListenerConfig:
    stream_prefix: str
    group_id: str
    concurrency: int = 1
    poll_batch_size: int = 10
    poll_timeout: float = DEFAULT_POLL_TIMEOUT_SECONDS
    heartbeat_interval: float | None = None

    def __post_init__(self) -> None:
        if not self.stream_prefix:
            raise ValueError("stream_prefix must not be blank")
        if not self.group_id:
            raise ValueError("group_id must not be blank")
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")
        if self.poll_batch_size < 1:
            raise ValueError("poll_batch_size must be positive")
        if self.poll_timeout < 0:
            raise ValueError("poll_timeout must not be negative")
        if self.heartbeat_interval is not None and self.heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive when provided")


@dataclass
class ConsumerAssignmentState:
    member_epoch: int = 0
    metadata_version: int = 0
    group_epoch: int = 0
    assignment_epoch: int = 0
    assigned_shards: set[CoordinatorShard] = field(default_factory=set)
    pending_shards: set[CoordinatorShard] = field(default_factory=set)
    status: str = OK


class ManagedConsumer:
    """One logical member with its own heartbeat state, Redis consumer name, and assignment."""

    def __init__(
        self,
        *,
        coordinator_client: CoordinatorClient,
        commands: RedisStreamCommands,
        config: StreamListenerConfig,
        member_id: str,
        member_name: str | None = None,
    ):
        self.coordinator_client = coordinator_client
        self.commands = commands
        self.config = config
        self.member_id = member_id
        self.member_name = member_name or member_id
        self.state = ConsumerAssignmentState()
        self.reader = RedisStreamReader(commands, config.group_id, member_id)
        self._stopped = threading.Event()

    def heartbeat(self) -> HeartbeatResponse:
        """Sends one heartbeat and applies the coordinator response idempotently."""
        request = HeartbeatRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id=str(uuid.uuid4()),
            member_id=self.member_id,
            member_name=self.member_name,
            member_epoch=self.state.member_epoch,
            metadata_version=self.state.metadata_version,
            runtime_consumer_capacity=RuntimeConsumerCapacity(
                runtime_max_concurrency=1,
                available_concurrency=1,
            ),
            owned_shards=set(self.state.assigned_shards),
        )
        response = self.coordinator_client.heartbeat(
            self.config.stream_prefix,
            self.config.group_id,
            self.member_id,
            request,
        )
        self.apply_heartbeat_response(response)
        return response

    def apply_heartbeat_response(self, response: HeartbeatResponse) -> None:
        status = response.status
        self.state.status = status
        if status in {UNKNOWN_MEMBER_ID, FENCED_MEMBER_EPOCH}:
            self._reset_for_rejoin(response)
            return
        if status == RETRY:
            return

        self.state.member_epoch = response.member_epoch
        self.state.metadata_version = response.metadata_version
        self.state.group_epoch = response.group_epoch
        self.state.assignment_epoch = response.assignment_epoch

        if status in {SYNC_METADATA, REVOKE_PENDING}:
            self.state.assigned_shards &= response.assignment.assigned_shards
            self.state.pending_shards = set(response.assignment.pending_shards)
            return
        if status == OK:
            self.state.assigned_shards = set(response.assignment.assigned_shards)
            self.state.pending_shards = set(response.assignment.pending_shards)

    def assigned_stream_keys(self) -> list[str]:
        return [shard.stream_key(self.config.stream_prefix) for shard in sorted(self.state.assigned_shards)]

    def poll_once(self) -> Any:
        if self.state.status != OK:
            return []
        return self.poll_messages_once()

    def poll_messages_once(self) -> list[RedisStreamMessage]:
        if self.state.status != OK:
            return []
        return self.reader.poll_messages_round_robin(
            self.assigned_stream_keys(),
            count=self.config.poll_batch_size,
            timeout_ms=int(self.config.poll_timeout * 1000),
        )

    def handle_once(self, handler: Handler) -> int:
        messages = self.poll_messages_once()
        for message in messages:
            handler(message)
        return len(messages)

    def run_forever(self, handler: Handler) -> None:
        heartbeat_interval = self.config.heartbeat_interval or DEFAULT_HEARTBEAT_INTERVAL_SECONDS
        next_heartbeat = 0.0
        while not self._stopped.is_set():
            now = time.monotonic()
            if now >= next_heartbeat:
                response = self.heartbeat()
                heartbeat_interval = response.heartbeat_interval_ms / 1000.0
                next_heartbeat = now + heartbeat_interval
            records = self.poll_once()
            if records:
                for record in records:
                    handler(record)
            else:
                time.sleep(min(0.05, heartbeat_interval))

    def stop(self) -> None:
        self._stopped.set()

    def _reset_for_rejoin(self, response: HeartbeatResponse) -> None:
        self.state.member_epoch = 0
        self.state.metadata_version = response.metadata_version
        self.state.group_epoch = response.group_epoch
        self.state.assignment_epoch = response.assignment_epoch
        self.state.assigned_shards.clear()
        self.state.pending_shards.clear()


class RedisStreamCoordinator:
    """Application facade that creates Python stream listeners and publishers."""

    def __init__(
        self,
        *,
        coordinator_base_url: str,
        redis_url: str | None = None,
        redis_client: Any | None = None,
        bearer_token: str | None = None,
        coordinator_client: CoordinatorClient | None = None,
    ):
        self.coordinator_client = coordinator_client or CoordinatorClient(
            coordinator_base_url,
            bearer_token=bearer_token,
        )
        self.redis_client = redis_client or self._new_redis_client(redis_url)
        self.commands = RedisStreamCommands(self.redis_client)
        self._registrations: list[tuple[StreamListenerConfig, Handler]] = []
        self._managed_consumers: list[ManagedConsumer] = []
        self._threads: list[threading.Thread] = []

    def stream_listener(
        self,
        *,
        stream_prefix: str,
        group_id: str,
        concurrency: int = 1,
        poll_batch_size: int = 10,
        poll_timeout: float = DEFAULT_POLL_TIMEOUT_SECONDS,
        heartbeat_interval: float | None = None,
    ) -> Callable[[Handler], Handler]:
        config = StreamListenerConfig(
            stream_prefix=stream_prefix,
            group_id=group_id,
            concurrency=concurrency,
            poll_batch_size=poll_batch_size,
            poll_timeout=poll_timeout,
            heartbeat_interval=heartbeat_interval,
        )

        def decorator(handler: Handler) -> Handler:
            self._registrations.append((config, handler))
            return handler

        return decorator

    def consumers(self, config: StreamListenerConfig, *, base_member_id: str | None = None) -> list[ManagedConsumer]:
        base = base_member_id or default_member_id()
        return [
            ManagedConsumer(
                coordinator_client=self.coordinator_client,
                commands=self.commands,
                config=config,
                member_id=member_id,
            )
            for member_id in split_member_ids(base, config.concurrency)
        ]

    def start(self, *, base_member_id: str | None = None, daemon: bool = True) -> None:
        """Starts all registered stream listeners in background threads."""
        if self._threads:
            raise RuntimeError("RedisStreamCoordinator is already started")
        for config, handler in self._registrations:
            for consumer in self.consumers(config, base_member_id=base_member_id):
                thread = threading.Thread(
                    target=consumer.run_forever,
                    args=(handler,),
                    name=f"redisstream-{consumer.member_id}",
                    daemon=daemon,
                )
                self._managed_consumers.append(consumer)
                self._threads.append(thread)
                thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        """Stops all listener threads created by start()."""
        for consumer in self._managed_consumers:
            consumer.stop()
        for thread in self._threads:
            thread.join(timeout=timeout)
        self._threads.clear()
        self._managed_consumers.clear()

    def publisher(
        self,
        stream_prefix: str,
        group_id: str,
        *,
        routing_refresh_interval: float = 5.0,
        publish_max_attempts: int = 2,
        xadd_max_len: int = 100_000,
        xadd_approximate_trimming: bool = True,
    ) -> RedisStreamPublisher:
        return RedisStreamPublisher(
            ProducerRoutingCache(
                self.coordinator_client,
                stream_prefix,
                group_id,
                refresh_interval=routing_refresh_interval,
            ),
            self.commands,
            publish_max_attempts=publish_max_attempts,
            xadd_max_len=xadd_max_len,
            xadd_approximate_trimming=xadd_approximate_trimming,
        )

    @staticmethod
    def _new_redis_client(redis_url: str | None) -> Any:
        if not redis_url:
            raise ValueError("redis_url or redis_client is required")
        import redis

        return redis.Redis.from_url(redis_url)


def stream_listener(**kwargs: Any) -> Callable[[Handler], Handler]:
    """Standalone decorator that attaches listener configuration metadata to a function."""
    config = StreamListenerConfig(**kwargs)

    def decorator(handler: Handler) -> Handler:
        setattr(handler, "__redisstream_listener__", config)
        return handler

    return decorator
