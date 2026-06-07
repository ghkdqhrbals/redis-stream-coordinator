# Edge Case Q&A

이 문서는 운영 중 자주 나오는 엣지케이스를 Q&A 형식으로 모아둔다. 상세 state-machine 규칙은 [Failure Modes and Edge Cases](12-failure-modes-edge-cases.md)에 있고, 이 문서는 빠르게 상황별 판단을 확인하기 위한 페이지이다.

## Coordinator 가용성

### Q. Coordinator가 짧게 사라지면 어떻게 되나?

Consumer는 local assignment lease가 유효한 동안만 기존 assigned shard를 계속 처리한다. Producer는 routing cache lease가 유효한 동안만 cached route로 publish한다. Lease가 만료되면 stale ownership 또는 무기한 stale routing을 쓰지 않고 fail closed한다.

### Q. Coordinator crash 또는 rolling update 뒤 새 pod가 뜨면 어디서 이어가나?

새 pod는 이전 stack frame을 이어받지 않는다. Redis에 기록된 group metadata key를 load하고, mutation 전 Redis mutex를 획득한 뒤, metadata에 기록된 workflow state만 advance한다.

### Q. Rolling update 중 coordinator pod가 순간적으로 2개 떠도 안전한가?

두 pod 모두 같은 Redis mutex와 `storeRevision` compare-and-set을 사용한다. 먼저 commit한 pod의 write가 저장되고, 늦은 pod는 reload 후 retry하거나 retryable conflict를 반환한다. 사용자가 직접 single-active pod를 보장해야 하는 구조는 아니다.

### Q. 종료 중인 old pod가 traffic을 받으면 어떻게 되나?

Terminating mode 이후 신규 critical-section 작업은 retryable coordinator-terminating error를 반환한다. 이미 local gate를 통과한 짧은 in-flight critical section은 Redis write와 mutex release를 완료할 수 있도록 끝까지 실행된다.

## Member 만료와 Rebalance

### Q. 모든 consumer member의 heartbeat가 끊기면 어떻게 되나?

Event loop가 member lease TTL 이후 member를 `EXPIRED`로 표시하고, metadata/group epoch를 증가시키고, target assignment를 비운 뒤 group을 `EMPTY`로 바꾼다. Expired member record는 관측성과 stale heartbeat fencing을 위해 잠시 남고, 이후 stale-member cleanup으로 제거될 수 있다.

### Q. 이전 owner가 revoke하기 전에 shard가 새 owner에게 넘어갈 수 있나?

이전 owner가 live 상태이면 안 된다. Coordinator는 이전 owner가 `REVOKED`를 보고하거나, member lease TTL로 expire되거나, rebalance timeout으로 fence되기 전까지 target owner에게 해당 shard를 `pending`으로만 내려준다.

### Q. Consumer가 DRAIN을 받은 뒤 coordinator가 죽으면 다시 읽어도 되나?

안 된다. Shard가 local revoking state에 들어가면 heartbeat가 실패한다는 이유만으로 read를 재개하면 안 된다. Consumer는 local in-flight work를 끝내고 `revokingShards`로 heartbeat retry를 계속하다가 coordinator가 새 assignment를 내려줄 때만 다시 읽을 수 있다.

### Q. Consumer가 draining 중 죽으면 shard가 영원히 막히나?

아니다. Coordinator는 lease 또는 rebalance timeout 이후 해당 member를 expire/fence한다. 그 뒤 recorded target assignment 기준으로 shard를 assign할 수 있다. 이 경우 duplicate processing은 가능하며 at-least-once 보장 경계에 포함된다.

## Scale-In과 제거 대상 Shard

### Q. Live member가 없는 drain 문제는 `targetShardCount=0`만의 문제인가?

아니다. 모든 scale-in에 적용된다. 예를 들어 `10 -> 1` scale-in에서도 모든 live member가 revoke ack 전에 expire되면 같은 문제가 발생한다. Coordinator는 heartbeat 대상이 없다는 이유만으로 migration을 `ACTIVE`에 고정하면 안 된다.

### Q. Scale-in 중 모든 member가 expire되면 coordinator는 무엇을 하나?

Revoke ack를 보낼 live member가 없으므로 consumer-level revoke wait를 생략한다. Migration은 Redis-level drain check로 넘어가고, 제거 대상 shard stream의 모든 Redis consumer group이 `pending=0`과 known `lag=0`을 보고할 때만 완료된다.

### Q. Live owner가 없으면 제거 대상 shard를 바로 retired 처리해도 되나?

안 된다. Live owner가 없다는 것은 heartbeat로 revoke를 받을 대상이 없다는 뜻일 뿐, Redis Stream data가 모두 drain됐다는 증거는 아니다. Removed shard는 Redis consumer-group evidence가 pending entry와 remaining known lag가 없음을 증명한 뒤에만 retired 처리한다.

### Q. Redis가 제거 대상 shard group에 대해 `lag=null`을 반환하면?

Coordinator는 drain 완료가 증명되지 않았다고 보고 migration을 `DRAINING`에 유지한다. 자동 완료를 기대하려면 `ENTRIESREAD` tracking을 보존하고, Redis가 lag를 계산할 수 없게 만드는 delete/trim 패턴을 피해야 한다.

### Q. Scale-in 대상 shard에 남아 있던 메시지는 어떻게 되나?

Coordinator가 다른 shard stream으로 메시지를 이동하지 않는다. Live consumer가 있으면 기존 consumer가 해당 record를 drain한다. Live consumer가 없으면 coordinator는 Redis group lag와 pending count가 제거 대상 stream이 drain됐음을 증명할 때까지 기다린다.

### Q. `targetShardCount=0`이면 무엇이 달라지나?

Producer routing은 결국 `shardCount=0`과 빈 shard list를 반환한다. 하지만 removed shard stream retired 판단은 다른 scale-in과 동일하게 Redis-level drain check를 통과해야 한다.

## Producer Routing

### Q. Producer도 heartbeat를 보내나?

아니다. Producer routing은 pull 기반이다. Producer는 coordinator에서 routing metadata를 refresh하고 bounded lease 동안 cache한다.

### Q. Python producer/consumer는 다른 routing 또는 heartbeat 규칙을 따르는가?

아니다. Python client는 JVM starter와 동일한 coordinator heartbeat status, listener concurrency의 logical-member split, Murmur3 32-bit routing, modulo-bias 제거, `XADD NOMKSTREAM` stale-route 보호를 따른다.

### Q. Scale-in 이후 producer가 stale routing을 들고 있으면?

Publisher는 Redis `XADD NOMKSTREAM`을 사용한다. 제거된 stream key를 stale route로 쓰려고 하면 Redis가 stream key를 재생성하지 않고 실패한다. 이때 routing cache를 invalidate하고 coordinator에서 새 routing을 받은 뒤 retry한다.

### Q. 같은 partition key가 resharding 이후 다른 shard로 갈 수 있나?

그렇다. Routing determinism은 같은 routing protocol, 같은 shard count, 같은 partition key 안에서만 보장된다. Shard count가 바뀌면 routing domain이 바뀐다.

### Q. Resharding 중 같은 event id가 두 shard에 쓰일 수 있나?

그럴 수 있다. Producer는 shard-level idempotent XADD를 제공할 수 있지만 old/new shard layout 전체에 걸친 global event-id deduplication은 제공하지 않는다. 중복에 민감한 workload는 shard count 변경 전 producer를 멈추고 in-flight publish retry를 drain해야 한다.

## Metadata와 Version Correction

### Q. Client가 Redis보다 높은 metadata version을 보고하면?

Redis가 source of truth이다. Coordinator는 metadata correction round를 시작하고, consumer가 Redis에 기록된 metadata version으로 heartbeat할 때까지 `SYNC_METADATA`를 반환한다.

### Q. Coordinator가 consumer report로 Redis metadata를 재구성하나?

아니다. Consumer report는 권위 있는 source of truth가 아니다. Coordinator는 reconciliation과 fencing에 consumer report를 사용하지만, stale client report로 metadata를 재구성하지 않는다.

### Q. Redis metadata가 삭제되거나 깨지면?

Coordinator는 fail closed한다. Monitoring projection, consumer report, producer cache에서 source-of-truth state를 추론해 복구하지 않는다.

## Processing Guarantee

### Q. Exactly-once processing을 제공하나?

아니다. 기본 보장은 at-least-once이다. Redis Stream ACK와 임의의 business side effect, 예를 들어 DB write, Redis write, HTTP call, 외부 API 호출을 하나의 원자적 transaction으로 묶을 수 없기 때문이다.

### Q. Idempotency와 duplicate side-effect 방지는 누가 담당하나?

애플리케이션이 담당한다. Domain idempotency key, DB unique constraint, deduplication table, compensation, application-specific retry policy를 사용해야 한다.

### Q. Coordinator가 Redis Stream 메시지를 읽거나 ack/delete하나?

아니다. Coordinator는 control plane이다. Message read, handler 실행, ack/ack-delete/nack, retry, DLQ, business idempotency는 member data plane 책임이다.
