# Member Data-Plane Boundary

Redis Stream Coordinator는 message processor가 아니다. 이 문서는 coordinator와 member data-plane의 경계만 정의한다.

## Coordinator Owns

* group metadata
* member liveness
* target assignment
* current assignment report
* assigned/pending shard assignment
* member epoch fencing status
* stream version migration state

## Member Data-Plane Owns

* Redis Stream `XREADGROUP` / `XACK`
* pending recovery
* handler execution
* retry / DLQ
* idempotency marker
* key ordering
* read batch, block timeout, worker thread tuning

## Contract

Coordinator heartbeat response의 `assignment.assignedShards`에서 기존 owned shard가 빠지면 member는 해당 shard의 신규 read를 중단하고 local in-flight가 0이 된 뒤 heartbeat의 `revokingShards.state=REVOKED`로 보고한다.

Coordinator heartbeat response의 `assignment.assignedShards`에 새 shard가 포함되면 member는 shard를 `ownedShards`에 반영하고, 실제 Redis Stream read/recovery는 member data-plane 정책에 따라 수행한다.

Coordinator가 `FENCED_MEMBER_EPOCH`을 반환하거나 member epoch mismatch가 발생하면 member는 read/ack를 중단하고 `memberEpoch=0` full heartbeat로 rejoin한다.
