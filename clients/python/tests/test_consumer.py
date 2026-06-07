import unittest

from redisstream.consumer import ManagedConsumer, StreamListenerConfig
from redisstream.identity import split_member_ids
from redisstream.models import AssignmentView, CoordinatorShard, HeartbeatResponse
from redisstream.redis_stream import RedisCommandSupport, RedisStreamCommands


class FakeCoordinator:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def heartbeat(self, stream_prefix, consumer_group, member_id, request):
        self.requests.append((stream_prefix, consumer_group, member_id, request))
        return self.responses.pop(0)


class FakeRedis:
    def __init__(self, response=None):
        self.commands = []
        self.response = response if response is not None else []

    def execute_command(self, *args):
        self.commands.append(args)
        return self.response


class ConsumerStateTest(unittest.TestCase):
    def test_listener_config_validates_required_values(self):
        with self.assertRaises(ValueError):
            StreamListenerConfig("", "demo-workers")
        with self.assertRaises(ValueError):
            StreamListenerConfig("create-order", "")
        with self.assertRaises(ValueError):
            StreamListenerConfig("create-order", "demo-workers", concurrency=0)
        with self.assertRaises(ValueError):
            StreamListenerConfig("create-order", "demo-workers", poll_batch_size=0)
        with self.assertRaises(ValueError):
            StreamListenerConfig("create-order", "demo-workers", poll_timeout=-1)

    def test_concurrency_four_creates_four_logical_member_ids(self):
        self.assertEqual(
            split_member_ids("10.0.0.1", 4),
            ["10.0.0.1-m0", "10.0.0.1-m1", "10.0.0.1-m2", "10.0.0.1-m3"],
        )

    def test_unknown_member_resets_local_ownership_for_rejoin(self):
        consumer = self._consumer(
            self._response("UNKNOWN_MEMBER_ID", assigned=[], pending=[], metadata=8)
        )
        consumer.state.member_epoch = 5
        consumer.state.metadata_version = 7
        consumer.state.assigned_shards = {CoordinatorShard(0)}

        consumer.heartbeat()

        self.assertEqual(consumer.state.member_epoch, 0)
        self.assertEqual(consumer.state.metadata_version, 8)
        self.assertEqual(consumer.state.assigned_shards, set())

    def test_sync_metadata_does_not_start_new_shards(self):
        consumer = self._consumer(
            self._response("SYNC_METADATA", assigned=[0, 1], pending=[2], metadata=9)
        )
        consumer.state.assigned_shards = {CoordinatorShard(0)}

        consumer.heartbeat()

        self.assertEqual(consumer.state.assigned_shards, {CoordinatorShard(0)})
        self.assertEqual(consumer.state.pending_shards, {CoordinatorShard(2)})

    def test_revoke_pending_does_not_start_new_shards(self):
        consumer = self._consumer(
            self._response("REVOKE_PENDING", assigned=[0, 1], pending=[2], metadata=9)
        )
        consumer.state.assigned_shards = {CoordinatorShard(0)}

        consumer.heartbeat()

        self.assertEqual(consumer.state.assigned_shards, {CoordinatorShard(0)})
        self.assertEqual(consumer.state.pending_shards, {CoordinatorShard(2)})

    def test_ok_replaces_assignment(self):
        consumer = self._consumer(self._response("OK", assigned=[1, 2], pending=[], metadata=10))
        consumer.state.assigned_shards = {CoordinatorShard(0)}

        consumer.heartbeat()

        self.assertEqual(consumer.state.assigned_shards, {CoordinatorShard(1), CoordinatorShard(2)})

    def test_round_robin_poll_rotates_assigned_shards(self):
        redis = FakeRedis()
        consumer = self._consumer(self._response("OK", assigned=[], pending=[], metadata=1), redis=redis)
        consumer.state.assigned_shards = {CoordinatorShard(0), CoordinatorShard(1)}

        consumer.poll_once()
        consumer.poll_once()

        first = redis.commands[0]
        second = redis.commands[1]
        self.assertEqual(first[-4:], ("create-order:0", "create-order:1", ">", ">"))
        self.assertEqual(second[-4:], ("create-order:1", "create-order:0", ">", ">"))

    def test_handle_once_passes_normalized_message_to_handler(self):
        redis = FakeRedis(
            response=[
                [
                    b"create-order:0",
                    [
                        (
                            b"1780000000000-0",
                            {b"eventId": b"evt-1", b"payload": b"created"},
                        )
                    ],
                ]
            ]
        )
        consumer = self._consumer(self._response("OK", assigned=[], pending=[], metadata=1), redis=redis)
        consumer.state.assigned_shards = {CoordinatorShard(0)}
        seen = []

        count = consumer.handle_once(seen.append)

        self.assertEqual(count, 1)
        self.assertEqual(seen[0].stream_key, "create-order:0")
        self.assertEqual(seen[0].record_id, "1780000000000-0")
        self.assertEqual(seen[0].fields, {"eventId": "evt-1", "payload": "created"})

    def _consumer(self, response, redis=None):
        redis = redis or FakeRedis()
        return ManagedConsumer(
            coordinator_client=FakeCoordinator([response]),
            commands=RedisStreamCommands(redis, RedisCommandSupport("8.8.0", True, True)),
            config=StreamListenerConfig("create-order", "demo-workers", poll_batch_size=10),
            member_id="pod-1-m0",
        )

    @staticmethod
    def _response(status, assigned, pending, metadata):
        return HeartbeatResponse(
            response_to="req-1",
            status=status,
            member_id="pod-1-m0",
            member_epoch=1,
            heartbeat_interval_ms=3000,
            rebalance_timeout_ms=60000,
            group_epoch=1,
            assignment_epoch=1,
            metadata_version=metadata,
            assignment=AssignmentView(
                assigned_shards={CoordinatorShard(it) for it in assigned},
                pending_shards={CoordinatorShard(it) for it in pending},
                metadata_version=metadata,
            ),
        )


if __name__ == "__main__":
    unittest.main()
