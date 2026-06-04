# Redis Stream Coordinator 문서 작업 지침

이 폴더는 Redis Stream sharding을 KIP-848 스타일의 coordinator-managed rebalance로 운영하기 위한 PRD와 설계 문서를 관리한다.

## 작업 원칙

* `PRD.md`는 entrypoint와 lazy loading index 역할만 한다.
* 상세 설계는 `prd/*.md`에 주제별로 나누어 둔다.
* 기존 `redis-stream-sharding/`은 coordinatorless 설계이다. 이 폴더는 중앙 Group Coordinator를 두는 대안 설계로 관리한다.
* KIP-848의 개념을 그대로 복사하지 말고 Redis Stream 제약에 맞게 번역한다.
* 구현이 확정되지 않은 항목은 `Open Questions` 또는 `Decision Needed`로 둔다.

## 아키텍처 기준

* coordinator가 group metadata, target assignment, member current assignment를 source of truth로 관리한다.
* member는 직접 shard owner를 최종 결정하지 않는다. coordinator가 내려준 assigned/pending shard assignment에 수렴한다.
* 권한 획득은 member의 무한 루프가 아니라 coordinator event loop와 heartbeat/reconciliation loop로 구동한다.
* shard count 변경은 in-place key rewrite가 아니라 resharding으로 처리한다.
* coordinator는 KIP-848 스타일 rebalance control plane만 담당한다.
* message read/ack, handler, retry, DLQ, idempotency marker는 member data-plane 책임으로 분리한다.

## 문서 작성 규칙

* "정확히 한 번" 같은 표현은 쓰지 않는다. 보장 범위와 실패 경계를 명시한다.
* epoch, member lease TTL, metadata version 같은 fencing 조건을 숨기지 않는다.
* Mermaid diagram은 실제 flow를 설명할 때만 사용한다.
* KIP-848 용어를 사용할 때는 Redis Stream 대응 용어를 함께 적는다.

## 문서 소유권

* KIP-848 대응 범위와 구현/비구현 목록은 `prd/08-kip848-implementation-coverage.md`에만 둔다.
* heartbeat request/response와 member lifecycle flow는 `prd/02-coordinator-architecture.md`에만 둔다.
* group state, epoch, target/current assignment data model은 `prd/03-group-assignment-model.md`에만 둔다.
* shard scale-out/in, resharding, Admin API는 `prd/04-resharding-routing.md`에만 둔다.
* coordinator-owned metrics, logs, config, Redis key naming은 `prd/06-data-config-observability.md`에만 둔다.
* 전체 HTTP endpoint catalog는 `prd/09-api-endpoints.md`에만 둔다.
* 같은 내용을 여러 문서에 복사하지 말고, entrypoint나 다른 문서에서는 링크와 짧은 요약만 둔다.

## 참고 설계

* KIP-848: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol>
* 작성자 정리 글: <https://ghkdqhrbals.github.io/portfolios/docs/Java/51/>
* coordinatorless 대안 설계: [`../redis-stream-sharding/PRD.md`](../redis-stream-sharding/PRD.md)
