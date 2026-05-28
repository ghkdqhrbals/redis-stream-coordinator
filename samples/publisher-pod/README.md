# Redis Stream Publisher Pod Sample

This sample runs a Spring Boot pod that uses `RedisStreamPublisher` from the starter. It asks the coordinator for the active producer routing view, writes messages to the correct versioned Redis Stream shard, and can auto-publish sample records for local end-to-end tests.

Useful endpoints:

- `GET /sample/status`
- `POST /sample/publish`

Example request:

```bash
curl -X POST http://localhost:18090/sample/publish \
  -H 'Content-Type: application/json' \
  -d '{"partitionKey":"order-1","payload":"hello"}'
```
