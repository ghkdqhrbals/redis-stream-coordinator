from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, order=True)
class CoordinatorShard:
    shard_index: int

    def to_json(self) -> dict[str, int]:
        return {"shardIndex": self.shard_index}

    @classmethod
    def from_json(cls, value: Any) -> "CoordinatorShard":
        if isinstance(value, CoordinatorShard):
            return value
        if isinstance(value, int):
            return cls(value)
        if isinstance(value, dict):
            return cls(int(value["shardIndex"]))
        raise TypeError(f"unsupported shard value: {value!r}")

    def stream_key(self, stream_prefix: str) -> str:
        return f"{stream_prefix}:{self.shard_index}"


@dataclass(frozen=True)
class RuntimeConsumerCapacity:
    runtime_max_concurrency: int
    available_concurrency: int

    def to_json(self) -> dict[str, int]:
        return {
            "runtimeMaxConcurrency": self.runtime_max_concurrency,
            "availableConcurrency": self.available_concurrency,
        }


@dataclass(frozen=True)
class RevokingShardReport:
    shard: CoordinatorShard
    state: str
    in_flight: int = 0
    acked_at: str | None = None

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "shard": self.shard.to_json(),
            "state": self.state,
            "inFlight": self.in_flight,
        }
        if self.acked_at is not None:
            data["ackedAt"] = self.acked_at
        return data


@dataclass(frozen=True)
class ShardConsumptionProgress:
    shard: CoordinatorShard
    stream_key: str
    last_delivered_id: str | None = None
    last_acked_id: str | None = None
    pending_count: int = 0
    updated_at: str | None = None

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "shard": self.shard.to_json(),
            "streamKey": self.stream_key,
            "pendingCount": self.pending_count,
        }
        if self.last_delivered_id is not None:
            data["lastDeliveredId"] = self.last_delivered_id
        if self.last_acked_id is not None:
            data["lastAckedId"] = self.last_acked_id
        if self.updated_at is not None:
            data["updatedAt"] = self.updated_at
        return data


@dataclass(frozen=True)
class HeartbeatRequest:
    protocol_version: int
    request_id: str
    member_id: str
    member_epoch: int
    metadata_version: int
    runtime_consumer_capacity: RuntimeConsumerCapacity
    member_name: str | None = None
    owned_shards: set[CoordinatorShard] = field(default_factory=set)
    revoking_shards: list[RevokingShardReport] = field(default_factory=list)
    shard_progress: list[ShardConsumptionProgress] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "protocolVersion": self.protocol_version,
            "requestId": self.request_id,
            "memberId": self.member_id,
            "memberName": self.member_name,
            "memberEpoch": self.member_epoch,
            "metadataVersion": self.metadata_version,
            "runtimeConsumerCapacity": self.runtime_consumer_capacity.to_json(),
            "ownedShards": [shard.to_json() for shard in sorted(self.owned_shards)],
            "revokingShards": [report.to_json() for report in self.revoking_shards],
            "shardProgress": [progress.to_json() for progress in self.shard_progress],
        }


@dataclass(frozen=True)
class AssignmentView:
    assigned_shards: set[CoordinatorShard]
    pending_shards: set[CoordinatorShard]
    metadata_version: int

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "AssignmentView":
        return cls(
            assigned_shards={CoordinatorShard.from_json(it) for it in data.get("assignedShards", [])},
            pending_shards={CoordinatorShard.from_json(it) for it in data.get("pendingShards", [])},
            metadata_version=int(data.get("metadataVersion", 0)),
        )


@dataclass(frozen=True)
class HeartbeatResponse:
    response_to: str
    status: str
    member_id: str
    member_epoch: int
    heartbeat_interval_ms: int
    rebalance_timeout_ms: int
    group_epoch: int
    assignment_epoch: int
    metadata_version: int
    assignment: AssignmentView

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "HeartbeatResponse":
        return cls(
            response_to=str(data.get("responseTo", "")),
            status=str(data["status"]),
            member_id=str(data["memberId"]),
            member_epoch=int(data["memberEpoch"]),
            heartbeat_interval_ms=int(data["heartbeatIntervalMs"]),
            rebalance_timeout_ms=int(data.get("rebalanceTimeoutMs", 0)),
            group_epoch=int(data["groupEpoch"]),
            assignment_epoch=int(data["assignmentEpoch"]),
            metadata_version=int(data["metadataVersion"]),
            assignment=AssignmentView.from_json(data.get("assignment", {})),
        )


@dataclass(frozen=True)
class ProducerRoutingShard:
    shard_index: int
    stream_key: str
    redis_slot: int | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ProducerRoutingShard":
        return cls(
            shard_index=int(data["shardIndex"]),
            stream_key=str(data["streamKey"]),
            redis_slot=None if data.get("redisSlot") is None else int(data["redisSlot"]),
        )


@dataclass(frozen=True)
class ProducerRoutingResponse:
    stream_prefix: str
    consumer_group: str
    metadata_version: int
    shard_count: int
    stream_key_pattern: str
    shards: list[ProducerRoutingShard]

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "ProducerRoutingResponse":
        return cls(
            stream_prefix=str(data["streamPrefix"]),
            consumer_group=str(data["consumerGroup"]),
            metadata_version=int(data["metadataVersion"]),
            shard_count=int(data["shardCount"]),
            stream_key_pattern=str(data.get("streamKeyPattern", "")),
            shards=[ProducerRoutingShard.from_json(it) for it in data.get("shards", [])],
        )

