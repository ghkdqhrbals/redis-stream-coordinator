# Redis Stream Consumer Pod Sample

This sample is a small Spring Boot server that loads `com.redisstream:redisstream-spring-boot-starter` as a real application would.

It is intended for local end-to-end testing:

* sends coordinator heartbeats
* receives assigned, pending, revoked, and fenced callbacks
* consumes Redis Stream messages from assigned shards
* exposes a small publish endpoint for smoke testing producer routing

Example:

```bash
SERVER_PORT=18081 CONSUMER_MEMBER_ID=consumer-pod-1 CONSUMER_MEMBER_NAME=consumer-pod-1 \
  ./gradlew :samples:consumer-pod:bootRun
```

Useful endpoints:

* `GET /sample/status`
* `GET /sample/events`
* `DELETE /sample/events`
* `POST /sample/publish`

