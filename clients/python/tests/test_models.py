import unittest

from redisstream.models import (
    AssignmentView,
    CoordinatorShard,
    HeartbeatRequest,
    HeartbeatResponse,
    RuntimeConsumerCapacity,
)
from redisstream.protocol import PROTOCOL_VERSION


class ModelJsonTest(unittest.TestCase):
    def test_heartbeat_request_serializes_coordinator_wire_names(self):
        request = HeartbeatRequest(
            protocol_version=PROTOCOL_VERSION,
            request_id="req-1",
            member_id="pod-1-m0",
            member_name="pod-1-m0",
            member_epoch=3,
            metadata_version=7,
            runtime_consumer_capacity=RuntimeConsumerCapacity(
                runtime_max_concurrency=1,
                available_concurrency=1,
            ),
            owned_shards={CoordinatorShard(2), CoordinatorShard(1)},
        )

        self.assertEqual(
            request.to_json(),
            {
                "protocolVersion": 1,
                "requestId": "req-1",
                "memberId": "pod-1-m0",
                "memberName": "pod-1-m0",
                "memberEpoch": 3,
                "metadataVersion": 7,
                "runtimeConsumerCapacity": {
                    "runtimeMaxConcurrency": 1,
                    "availableConcurrency": 1,
                },
                "ownedShards": [{"shardIndex": 1}, {"shardIndex": 2}],
                "revokingShards": [],
                "shardProgress": [],
            },
        )

    def test_heartbeat_response_deserializes_assignment(self):
        response = HeartbeatResponse.from_json(
            {
                "responseTo": "req-1",
                "status": "OK",
                "memberId": "pod-1-m0",
                "memberEpoch": 4,
                "heartbeatIntervalMs": 3000,
                "rebalanceTimeoutMs": 60000,
                "groupEpoch": 10,
                "assignmentEpoch": 11,
                "metadataVersion": 12,
                "assignment": {
                    "assignedShards": [{"shardIndex": 0}, {"shardIndex": 1}],
                    "pendingShards": [{"shardIndex": 2}],
                    "metadataVersion": 12,
                },
            }
        )

        self.assertEqual(response.assignment, AssignmentView({CoordinatorShard(0), CoordinatorShard(1)}, {CoordinatorShard(2)}, 12))


if __name__ == "__main__":
    unittest.main()

