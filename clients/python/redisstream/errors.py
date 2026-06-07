class RedisStreamCoordinatorError(Exception):
    """Base error raised by the Python Redis Stream Coordinator client."""


class CoordinatorHttpError(RedisStreamCoordinatorError):
    def __init__(self, status_code: int, body: str):
        super().__init__(f"coordinator request failed with HTTP {status_code}: {body}")
        self.status_code = status_code
        self.body = body


class UnsupportedRedisCommandError(RedisStreamCoordinatorError):
    """Raised when the connected Redis server cannot execute a requested stream command."""


class NoAvailableShardError(RedisStreamCoordinatorError):
    """Raised when producer routing has no active shard to write to."""


class InvalidRoutingMetadataError(RedisStreamCoordinatorError):
    """Raised when coordinator routing metadata does not match the requested stream/group."""


class RedisStreamWriteError(RedisStreamCoordinatorError):
    """Raised when Redis rejects a stream write."""
