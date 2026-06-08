# Redis Stream Publisher Pod Sample

This sample runs a Spring Boot pod that uses a named `StreamProducer` bean from the starter. It asks the coordinator for the active producer routing view, writes messages to the correct Redis Stream shard, and can auto-publish sample records for local end-to-end tests.

Producer applications should define one `StreamProducer` bean per logical stream and inject the required producer with `@Qualifier`.

```kotlin
@Bean("ordersStreamProducer")
fun ordersStreamProducer(...): StreamProducer =
    StreamProducer(
        streamPrefix = "create-order",
        consumerGroupName = "demo-workers",
        client = coordinatorClient,
        redisConnectionFactory = redisConnectionFactory,
    )
```

Useful endpoints:

- `GET /sample/status`
- `POST /sample/publish`

Example request:

```bash
curl -X POST http://localhost:18090/sample/publish \
  -H 'Content-Type: application/json' \
  -d '{"partitionKey":"order-1","payload":"hello"}'
```
