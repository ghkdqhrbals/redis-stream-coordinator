import os
import unittest
import uuid

from redisstream import CoordinatorClient, ManagedConsumer, ProducerRoutingCache, RedisStreamPublisher
from redisstream.consumer import StreamListenerConfig
from redisstream.redis_stream import RedisStreamCommands


@unittest.skipUnless(
    os.getenv("RSC_PYTHON_INTEGRATION") == "1",
    "set RSC_PYTHON_INTEGRATION=1 with coordinator/redis env vars to run integration tests",
)
class OptionalCoordinatorIntegrationTest(unittest.TestCase):
    def setUp(self):
        import redis

        coordinator_base_url = os.environ["RSC_COORDINATOR_BASE_URL"]
        redis_url = os.environ["RSC_REDIS_URL"]
        self.stream_prefix = os.environ["RSC_STREAM_PREFIX"]
        self.consumer_group = os.environ["RSC_CONSUMER_GROUP"]
        self.client = CoordinatorClient(
            coordinator_base_url,
            bearer_token=os.getenv("RSC_COORDINATOR_BEARER_TOKEN"),
        )
        self.commands = RedisStreamCommands(redis.Redis.from_url(redis_url))

    def test_producer_publish_then_consumer_heartbeat_existing_group(self):
        publisher = RedisStreamPublisher(
            ProducerRoutingCache(self.client, self.stream_prefix, self.consumer_group),
            self.commands,
        )
        published = publisher.publish(
            f"python-it-{uuid.uuid4()}",
            {"eventId": f"python-it-{uuid.uuid4()}", "payload": "integration"},
        )
        self.assertTrue(published.record_id)

        consumer = ManagedConsumer(
            coordinator_client=self.client,
            commands=self.commands,
            config=StreamListenerConfig(
                stream_prefix=self.stream_prefix,
                group_id=self.consumer_group,
                poll_batch_size=1,
            ),
            member_id=f"python-it-{uuid.uuid4()}",
        )
        response = consumer.heartbeat()
        self.assertIn(response.status, {"OK", "SYNC_METADATA", "REVOKE_PENDING", "RETRY"})


if __name__ == "__main__":
    unittest.main()

