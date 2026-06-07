# redisstream-coordinator

Sync-first Python client library for Redis Stream Coordinator.

```python
from redisstream import RedisStreamCoordinator

app = RedisStreamCoordinator(
    coordinator_base_url="https://coordinator.example.com",
    redis_url="redis://localhost:6379",
)

@app.stream_listener(
    stream_prefix="create-order",
    group_id="demo-workers",
    concurrency=4,
    poll_batch_size=10,
)
def handle(message):
    # business side effect
    message.ack()

publisher = app.publisher("create-order", "demo-workers")
publisher.publish("order-123", {"eventId": "evt-1", "payload": "..."})

app.start()
```
