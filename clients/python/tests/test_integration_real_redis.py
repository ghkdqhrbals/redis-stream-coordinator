import json
import os
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from redisstream import CoordinatorClient, ManagedConsumer, ProducerRoutingCache, RedisStreamPublisher
from redisstream.consumer import StreamListenerConfig
from redisstream.models import AssignmentView, CoordinatorShard, HeartbeatResponse, ProducerRoutingResponse, ProducerRoutingShard
from redisstream.redis_stream import RedisStreamCommands


@unittest.skipUnless(
    os.getenv("RSC_REAL_REDIS_URL"),
    "set RSC_REAL_REDIS_URL to run real Redis integration tests",
)
class RealRedisPythonClientIntegrationTest(unittest.TestCase):
    def setUp(self):
        import redis

        self.redis = redis.Redis.from_url(os.environ["RSC_REAL_REDIS_URL"], decode_responses=False)
        self.stream_prefix = f"py-it-{uuid.uuid4().hex[:12]}"
        self.consumer_group = "py-workers"
        self.stream_keys = [f"{self.stream_prefix}:0", f"{self.stream_prefix}:1"]
        self._create_stream_group(self.stream_keys)

        self.fake_coordinator = FakeCoordinatorServer(
            stream_prefix=self.stream_prefix,
            consumer_group=self.consumer_group,
            stream_keys=self.stream_keys,
        )
        self.fake_coordinator.start()
        self.client = CoordinatorClient(self.fake_coordinator.base_url)
        self.commands = RedisStreamCommands(self.redis)

    def tearDown(self):
        self.fake_coordinator.stop()
        for key in self.stream_keys + [f"{self.stream_prefix}:stale"]:
            self.redis.delete(key)

    def test_publish_heartbeat_consume_ack_against_real_redis(self):
        publisher = RedisStreamPublisher(
            ProducerRoutingCache(self.client, self.stream_prefix, self.consumer_group, refresh_interval=60),
            self.commands,
            publish_max_attempts=2,
            xadd_max_len=1000,
        )

        published = publisher.publish("order-123", {"eventId": "evt-1", "payload": "created"})
        self.assertIn(published.stream_key, self.stream_keys)

        consumer = ManagedConsumer(
            coordinator_client=self.client,
            commands=self.commands,
            config=StreamListenerConfig(
                stream_prefix=self.stream_prefix,
                group_id=self.consumer_group,
                poll_batch_size=10,
                poll_timeout=0,
            ),
            member_id="python-it-m0",
        )

        heartbeat = consumer.heartbeat()
        self.assertEqual(heartbeat.status, "OK")
        self.assertEqual(consumer.assigned_stream_keys(), self.stream_keys)

        seen = []
        count = consumer.handle_once(lambda message: (seen.append(message), message.ack()))

        self.assertEqual(count, 1)
        self.assertEqual(seen[0].record_id, published.record_id)
        self.assertEqual(seen[0].fields["eventId"], "evt-1")
        self.assertEqual(self.redis.xpending(published.stream_key, self.consumer_group)["pending"], 0)

    def test_stale_route_retry_refreshes_and_writes_existing_stream(self):
        self.fake_coordinator.route_sequence = [
            ProducerRoutingResponse(
                stream_prefix=self.stream_prefix,
                consumer_group=self.consumer_group,
                metadata_version=1,
                shard_count=1,
                stream_key_pattern=f"{self.stream_prefix}:{{shardIndex}}",
                shards=[ProducerRoutingShard(0, f"{self.stream_prefix}:stale", 0)],
            ),
            ProducerRoutingResponse(
                stream_prefix=self.stream_prefix,
                consumer_group=self.consumer_group,
                metadata_version=2,
                shard_count=1,
                stream_key_pattern=f"{self.stream_prefix}:{{shardIndex}}",
                shards=[ProducerRoutingShard(0, self.stream_keys[0], 0)],
            ),
        ]
        publisher = RedisStreamPublisher(
            ProducerRoutingCache(self.client, self.stream_prefix, self.consumer_group, refresh_interval=60),
            self.commands,
            publish_max_attempts=2,
        )

        published = publisher.publish("order-456", {"eventId": "evt-2"})

        self.assertEqual(published.stream_key, self.stream_keys[0])
        self.assertEqual(published.metadata_version, 2)
        self.assertEqual(self.redis.exists(f"{self.stream_prefix}:stale"), 0)

    def _create_stream_group(self, stream_keys):
        for key in stream_keys:
            self.redis.execute_command("XADD", key, "MAXLEN", "~", "1000", "*", "seed", "1")
            self.redis.execute_command("XGROUP", "CREATE", key, self.consumer_group, "$", "MKSTREAM")


class FakeCoordinatorServer:
    def __init__(self, *, stream_prefix, consumer_group, stream_keys):
        self.stream_prefix = stream_prefix
        self.consumer_group = consumer_group
        self.route_sequence = [
            ProducerRoutingResponse(
                stream_prefix=stream_prefix,
                consumer_group=consumer_group,
                metadata_version=1,
                shard_count=len(stream_keys),
                stream_key_pattern=f"{stream_prefix}:{{shardIndex}}",
                shards=[
                    ProducerRoutingShard(index, stream_key, index)
                    for index, stream_key in enumerate(stream_keys)
                ],
            )
        ]
        self.heartbeat_response = HeartbeatResponse(
            response_to="req",
            status="OK",
            member_id="python-it-m0",
            member_epoch=1,
            heartbeat_interval_ms=3000,
            rebalance_timeout_ms=60000,
            group_epoch=1,
            assignment_epoch=1,
            metadata_version=1,
            assignment=AssignmentView(
                assigned_shards={CoordinatorShard(index) for index in range(len(stream_keys))},
                pending_shards=set(),
                metadata_version=1,
            ),
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def start(self):
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()

    def next_routing(self):
        if len(self.route_sequence) == 1:
            return self.route_sequence[0]
        return self.route_sequence.pop(0)

    def _handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.endswith("/producer-routing"):
                    self._send_json(_routing_to_json(owner.next_routing()))
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self):
                if self.path.endswith("/heartbeat"):
                    content_length = int(self.headers.get("Content-Length", "0"))
                    if content_length:
                        self.rfile.read(content_length)
                    self._send_json(_heartbeat_to_json(owner.heartbeat_response))
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, *_args):
                return

            def _send_json(self, data):
                body = json.dumps(data).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def _routing_to_json(response):
    return {
        "streamPrefix": response.stream_prefix,
        "consumerGroup": response.consumer_group,
        "metadataVersion": response.metadata_version,
        "shardCount": response.shard_count,
        "streamKeyPattern": response.stream_key_pattern,
        "shards": [
            {"shardIndex": shard.shard_index, "streamKey": shard.stream_key, "redisSlot": shard.redis_slot}
            for shard in response.shards
        ],
    }


def _heartbeat_to_json(response):
    return {
        "responseTo": response.response_to,
        "status": response.status,
        "memberId": response.member_id,
        "memberEpoch": response.member_epoch,
        "heartbeatIntervalMs": response.heartbeat_interval_ms,
        "rebalanceTimeoutMs": response.rebalance_timeout_ms,
        "groupEpoch": response.group_epoch,
        "assignmentEpoch": response.assignment_epoch,
        "metadataVersion": response.metadata_version,
        "assignment": {
            "assignedShards": [
                {"shardIndex": shard.shard_index}
                for shard in sorted(response.assignment.assigned_shards)
            ],
            "pendingShards": [],
            "metadataVersion": response.assignment.metadata_version,
        },
    }


if __name__ == "__main__":
    unittest.main()

