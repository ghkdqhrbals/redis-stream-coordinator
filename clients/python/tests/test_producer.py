import unittest

from redisstream.errors import InvalidRoutingMetadataError, NoAvailableShardError, RedisStreamWriteError
from redisstream.hashing import murmur3_32, unbiased_shard_index
from redisstream.models import ProducerRoutingResponse, ProducerRoutingShard
from redisstream.producer import ProducerRoutingCache, RedisStreamPublisher


class FakeCoordinator:
    def __init__(self, metadata):
        self.metadata = list(metadata)
        self.calls = 0

    def producer_routing(self, stream_prefix, consumer_group):
        self.calls += 1
        if len(self.metadata) == 1:
            return self.metadata[0]
        return self.metadata.pop(0)


class FakeCommands:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []

    def xadd_nomkstream(self, stream_key, fields, *, max_len, approximate_trimming):
        self.calls.append((stream_key, dict(fields), max_len, approximate_trimming))
        if self.failures:
            self.failures -= 1
            raise RedisStreamWriteError("stale route")
        return f"{len(self.calls)}-0"


class ProducerRoutingTest(unittest.TestCase):
    def test_murmur3_known_fixture_and_shard_mapping(self):
        self.assertEqual(murmur3_32(b""), 0)
        self.assertEqual(murmur3_32(b"hello"), 613153351)
        self.assertEqual(unbiased_shard_index(b"order-123", 10), 1)

    def test_zero_shard_publish_fails_closed(self):
        cache = ProducerRoutingCache(FakeCoordinator([self._metadata(0)]), "create-order", "demo-workers")

        with self.assertRaises(NoAvailableShardError):
            cache.route("order-1")

    def test_routing_metadata_mismatch_fails_fast(self):
        cache = ProducerRoutingCache(
            FakeCoordinator(
                [
                    ProducerRoutingResponse(
                        stream_prefix="other-stream",
                        consumer_group="demo-workers",
                        metadata_version=1,
                        shard_count=1,
                        stream_key_pattern="other-stream:{shardIndex}",
                        shards=[ProducerRoutingShard(0, "other-stream:0", 1)],
                    )
                ]
            ),
            "create-order",
            "demo-workers",
        )

        with self.assertRaises(InvalidRoutingMetadataError):
            cache.metadata()

    def test_publisher_invalidates_and_retries_after_stale_route_failure(self):
        coordinator = FakeCoordinator([self._metadata(2, version=1), self._metadata(2, version=2)])
        cache = ProducerRoutingCache(coordinator, "create-order", "demo-workers", refresh_interval=60)
        commands = FakeCommands(failures=1)
        publisher = RedisStreamPublisher(cache, commands, publish_max_attempts=2)

        published = publisher.publish("order-1", {"eventId": "evt-1"})

        self.assertEqual(published.record_id, "2-0")
        self.assertEqual(coordinator.calls, 2)
        self.assertEqual(len(commands.calls), 2)

    @staticmethod
    def _metadata(shard_count, version=1):
        return ProducerRoutingResponse(
            stream_prefix="create-order",
            consumer_group="demo-workers",
            metadata_version=version,
            shard_count=shard_count,
            stream_key_pattern="create-order:{shardIndex}",
            shards=[
                ProducerRoutingShard(shard_index=i, stream_key=f"create-order:{i}", redis_slot=i)
                for i in range(shard_count)
            ],
        )


if __name__ == "__main__":
    unittest.main()
