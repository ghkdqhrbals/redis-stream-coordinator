# KIP-848 Implementation Coverage

## Purpose

이 문서는 Redis Stream Coordinator가 KIP-848의 어떤 아이디어를 구현하고, 어떤 부분을 Redis Stream 제약에 맞게 바꾸거나 제외했는지 정리한다. 목표는 Kafka protocol 호환이 아니라 KIP-848의 coordinator-managed rebalance 모델을 Redis Stream shard ownership에 적용하는 것이다.

## Concept Mapping

| KIP-848 Concept | Redis Stream Coordinator |
| --- | --- |
| Group Coordinator | Coordinator API server |
| Consumer Group | `{streamPrefix, consumerGroup}` |
| Member | member runtime이 생성한 UUID `memberId` |
| Topic Partition | `{streamVersion, shardIndex}` |
| Target Assignment | coordinator가 저장한 desired shard ownership |
| Current Assignment | member가 heartbeat로 보고한 실제 적용 상태 |
| ConsumerGroupHeartbeat | member heartbeat request/response assignment channel |
| Group Epoch | group metadata 변경 세대 |
| Assignment Epoch | target assignment 계산 기준 세대 |
| Member Epoch | member fencing과 assignment 적용 세대 |

## Implemented From KIP-848

* Coordinator-driven rebalance: member가 owner를 최종 결정하지 않고 coordinator가 target assignment를 계산한다.
* Declarative target assignment: coordinator는 group이 수렴해야 할 desired shard ownership을 저장한다.
* Current assignment reporting: member는 heartbeat마다 실제 owned/revoking shard 상태를 보고한다.
* Current assignment validation: coordinator는 target assignment와 이전에 수락한 current assignment 기준으로 stale ownership report를 fencing한다.
* Heartbeat assignment channel: coordinator는 heartbeat response로 assigned/pending shard assignment와 fencing status를 전달한다.
* Incremental reconciliation: group-wide stop-the-world barrier 없이 변경된 member/shard만 revoke/assign한다.
* Revoke-before-assign dependency: 기존 owner의 revoke ack 또는 member expiration 확인 전 새 owner에게 assign하지 않는다.
* Epoch model: `groupEpoch`, `assignmentEpoch`, `memberEpoch`으로 group metadata, target assignment, member fencing을 구분한다.
* Coordinator event loop: heartbeat, metadata change, member lease expiration을 coordinator loop에서 순차적으로 평가한다.
* Session timeout and fencing: `member-lease-ttl` 동안 heartbeat가 없는 member를 fencing하고 shard를 재할당한다.
* Sticky movement minimization: assignment는 sticky partition 전제로 기존 owner를 최대한 유지한다.

## Redis-Specific Adaptations

* Kafka broker coordinator 대신 Coordinator API server가 group metadata와 assignment를 관리한다.
* Kafka broker wire protocol 대신 custom HTTP Admin/Heartbeat/Monitoring API를 사용한다.
* Kafka topic partition 대신 Redis Stream shard key `{streamVersion, shardIndex}`를 assignment 단위로 사용한다.
* Kafka offset commit fencing은 구현하지 않는다. Redis Stream read/ack fencing은 member data-plane이 coordinator의 assignment/member epoch을 기준으로 적용한다.
* Kafka partition expansion 대신 stream version migration으로 shard scale-out/in을 처리한다.
* Kafka `ConsumerGroupHeartbeat` RPC는 internal coordinator API 또는 Redis mailbox request/response로 구현한다.
* member id는 member runtime이 생성하고 coordinator가 epoch/fencing 상태를 관리한다.
* Kafka의 `group.consumer.heartbeat.interval.ms`와 `group.consumer.session.timeout.ms`는 coordinator `heartbeat-interval`과 `member-lease-ttl`로 단순화한다.
* KIP-848의 server-side assignor 선택은 제공하지 않는다. sticky partition assignment는 설계 전제이다.
* Basic Auth, Monitoring API, Redis 접속 YAML, Admin API default 값은 Redis coordinator 운영을 위한 추가 계층이며 KIP-848 protocol 개념이 아니다.

## Intentional Differences From KIP-848

| Area | KIP-848 | Redis Stream Coordinator |
| --- | --- | --- |
| Coordinator placement | Kafka broker group coordinator가 `__consumer_offsets` 기반 state machine으로 관리한다. | 별도 lightweight Coordinator API server가 Redis metadata store를 사용한다. |
| Wire protocol | Kafka RPC와 error code를 확장한다. | HTTP Admin/Heartbeat/Monitoring API를 사용하고 Kafka protocol 호환을 목표로 하지 않는다. |
| Member ID | KIP-848 원문은 server-generated member id를 설명하고, 이후 KIP-1082로 client-generated id가 반영됐다. | member runtime이 UUID `memberId`를 만들고 coordinator가 등록/epoch/fencing을 관리한다. `memberId` 자체가 runtime incarnation id이다. |
| Heartbeat payload | 첫 heartbeat나 error 이후에는 전체 필드를 보내고, 이후에는 변경된 subscription/assignor/owned partition field만 보낼 수 있다. | MVP heartbeat는 owned shards, revoking shards, runtime consumer capacity를 명시적으로 보고하는 custom schema를 사용한다. |
| Heartbeat/session | server-side heartbeat interval과 session timeout을 member에게 전달한다. | `heartbeatIntervalMs`로 다음 heartbeat 주기를 전달하고, `member-lease-ttl` 초과 member를 `EXPIRED/FENCED` 처리한다. |
| Rebalance timeout | revoke 완료를 기다리는 rebalance timeout이 있고, 초과하면 member를 group에서 제거할 수 있다. | `rebalanceTimeoutMs`를 heartbeat field로 받는다. coordinator global config가 아니라 member revoke deadline으로만 사용한다. |
| Assignor model | server-side assignor 선택, assignor negotiation, client-side assignor delegation을 모델링한다. | sticky partition assignment만 전제로 하며 선택/협상/위임을 제공하지 않는다. |
| Subscription model | topic name, topic id, regex subscription, partition metadata 변경을 group metadata로 다룬다. | `{streamPrefix, consumerGroup}` Admin API 대상과 Redis Stream version metadata를 사용한다. regex subscription은 없다. |
| Member metadata | instance id, rack id, client id, client host, subscribed topics, assignor metadata를 group assignment input으로 다룬다. | member lifecycle, heartbeat time, assignment epoch, runtime/assigned concurrency만 coordinator state로 둔다. |
| Partition scaling | Kafka topic partition metadata 변경이 group epoch 증가 trigger가 된다. | shard count 변경은 Coordinator Admin API만 가능하고 next stream version migration으로 처리한다. |
| Static membership | instance id 기반 static membership과 temporary leave를 지원한다. | MVP에서는 static membership을 구현하지 않는다. restart는 새 runtime incarnation으로 본다. |
| Offset APIs | offset commit/fetch가 member epoch으로 fencing된다. | Redis Stream `XREADGROUP`/`XACK`는 at-least-once이다. Coordinator는 stale owner fencing과 assignment visibility를 제공하지만 단일 처리 보장은 제공하지 않는다. |
| Error model | Kafka protocol error code(`UNKNOWN_MEMBER_ID`, `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_ASSIGNOR` 등)를 사용한다. | Redis coordinator는 sticky assignment 전제라 assignor error를 제외하고 `HeartbeatStatus` enum을 custom API contract로 정의한다. |
| Admin/describe APIs | Kafka group/offset describe APIs와 Kafka ACL을 사용한다. | custom Admin API, Monitoring API, Basic Auth를 사용한다. |
| Config model | Kafka broker/group config에 consumer assignors, heartbeat/session, group size 등을 둔다. | coordinator YAML은 Redis 접속, Basic Auth, loop timing, request default만 가진다. stream/group별 shard count와 consumer `maxConcurrency`는 Admin API metadata에 저장한다. |
| Metadata retention | Kafka coordinator log compacted state와 group cleanup 정책을 따른다. | `EXPIRED` member와 migration metadata는 운영 디버깅을 위해 장기간 유지한다. |

## Not Implemented

* Kafka wire protocol compatibility.
* Kafka `ConsumerGroupPrepareAssignment` / `ConsumerGroupInstallAssignment` client-side assignment delegation.
* assignor negotiation, assignor version probing, assignor metadata compatibility handling.
* regex topic subscription and server-side topic metadata resolution.
* Kafka static membership semantics such as temporary leave with instance id replacement.
* Kafka offset commit/fetch APIs and topic-id based offset handling.
* Kafka group dynamic config API. Redis Stream Coordinator uses its own Admin API for create, shard scale, consumer concurrency update, and rollback.

## Residual Gaps To Decide

* member-generated UUID만으로 충분한지, 첫 heartbeat에서 coordinator registration ack를 별도 contract로 강제할지.
* Monitoring API response를 Kafka `ConsumerGroupDescribe`와 유사한 형태로 맞출지, Redis 운영에 필요한 custom summary로 고정할지.

## Coverage Notes

KIP-848 says the new protocol moves complexity from clients to the group coordinator, removes a global synchronization barrier, stores target/current assignment, and uses heartbeat to carry member state and assignment. This design implements those parts directly, then replaces Kafka-specific broker, partition, offset, and protocol concerns with Redis metadata keys, stream version migration, and member data-plane boundaries.
