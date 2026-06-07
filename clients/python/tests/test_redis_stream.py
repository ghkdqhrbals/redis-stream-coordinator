import unittest

from redisstream.errors import RedisStreamWriteError, UnsupportedRedisCommandError
from redisstream.redis_stream import RedisCommandSupport, RedisStreamCommands, normalize_xreadgroup_response


class FakeRedis:
    def __init__(self, response="1-0", info=None, error=None):
        self.response = response
        self.info_response = info or {"redis_version": "8.8.0"}
        self.error = error
        self.commands = []

    def info(self, *args):
        return self.info_response

    def execute_command(self, *args):
        self.commands.append(args)
        if self.error:
            raise self.error
        return self.response


class RedisStreamCommandTest(unittest.TestCase):
    def test_xadd_uses_nomkstream_and_maxlen(self):
        redis = FakeRedis(response=b"1780000000000-0")
        commands = RedisStreamCommands(redis)

        record_id = commands.xadd_nomkstream("create-order:0", {"eventId": "evt-1"}, max_len=1000)

        self.assertEqual(record_id, "1780000000000-0")
        self.assertEqual(
            redis.commands[0],
            ("XADD", "create-order:0", "NOMKSTREAM", "MAXLEN", "~", "1000", "*", "eventId", "evt-1"),
        )

    def test_xadd_nomkstream_none_response_is_write_error(self):
        redis = FakeRedis(response=None)
        commands = RedisStreamCommands(redis)

        with self.assertRaises(RedisStreamWriteError):
            commands.xadd_nomkstream("create-order:0", {"eventId": "evt-1"}, max_len=1000)

    def test_xadd_nomkstream_redis_error_is_write_error(self):
        redis = FakeRedis(error=RuntimeError("NOGROUP or no such key"))
        commands = RedisStreamCommands(redis)

        with self.assertRaises(RedisStreamWriteError):
            commands.xadd_nomkstream("create-order:0", {"eventId": "evt-1"}, max_len=1000)

    def test_xreadgroup_command(self):
        redis = FakeRedis(response=[])
        commands = RedisStreamCommands(redis)

        commands.xreadgroup(["create-order:0"], "demo-workers", "pod-1", count=10, block_ms=250)

        self.assertEqual(
            redis.commands[0],
            (
                "XREADGROUP",
                "GROUP",
                "demo-workers",
                "pod-1",
                "COUNT",
                "10",
                "BLOCK",
                "250",
                "STREAMS",
                "create-order:0",
                ">",
            ),
        )

    def test_xreadgroup_response_normalization_supports_map_and_flat_fields(self):
        normalized = normalize_xreadgroup_response(
            [
                [b"orders:0", [(b"1-0", {b"eventId": b"evt-1"})]],
                ["orders:1", [("2-0", ["eventId", "evt-2", "payload", "ok"])]],
            ]
        )

        self.assertEqual(
            normalized,
            [
                ("orders:0", "1-0", {"eventId": "evt-1"}),
                ("orders:1", "2-0", {"eventId": "evt-2", "payload": "ok"}),
            ],
        )

    def test_xack(self):
        redis = FakeRedis(response=1)
        commands = RedisStreamCommands(redis)

        self.assertEqual(commands.xack("create-order:0", "demo-workers", "1-0"), 1)
        self.assertEqual(redis.commands[0], ("XACK", "create-order:0", "demo-workers", "1-0"))

    def test_xackdel_supported_and_unsupported(self):
        supported = FakeRedis()
        RedisStreamCommands(supported, RedisCommandSupport("8.8.0", True, True)).xackdel(
            "create-order:0", "demo-workers", "ACKED", "1-0"
        )
        self.assertEqual(
            supported.commands[0],
            ("XACKDEL", "create-order:0", "demo-workers", "ACKED", "IDS", "1", "1-0"),
        )

        with self.assertRaises(UnsupportedRedisCommandError):
            RedisStreamCommands(FakeRedis(), RedisCommandSupport("8.0.0", False, False)).xackdel(
                "create-order:0", "demo-workers", "ACKED", "1-0"
            )

    def test_xnack_supported_and_unsupported(self):
        supported = FakeRedis()
        RedisStreamCommands(supported, RedisCommandSupport("8.8.0", True, True)).xnack(
            "create-order:0", "demo-workers", "SILENT", "1-0", retry_count=2, force=True
        )
        self.assertEqual(
            supported.commands[0],
            (
                "XNACK",
                "create-order:0",
                "demo-workers",
                "SILENT",
                "IDS",
                "1",
                "1-0",
                "RETRYCOUNT",
                "2",
                "FORCE",
            ),
        )

        with self.assertRaises(UnsupportedRedisCommandError):
            RedisStreamCommands(FakeRedis(), RedisCommandSupport("8.2.0", True, False)).xnack(
                "create-order:0", "demo-workers", "SILENT", "1-0"
            )


if __name__ == "__main__":
    unittest.main()
