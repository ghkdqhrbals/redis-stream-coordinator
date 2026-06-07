# Terraform and GitOps Governance

## Purpose

Coordinator admin APIs change the control-plane source of truth: group creation, group deletion, shard count, and resharding rollback. In production, these mutations should be reviewed and applied through Terraform or another GitOps workflow whenever practical.

Terraform is not a replacement for runtime audit logging. It records desired state, plan output, approvals, and who merged or applied a change. The coordinator audit log records what actually reached the API at runtime, including failed requests, forbidden requests, request ids, caller identity, client address, request summary, and request body fingerprint.

## Governance Boundary

Terraform or GitOps should manage:

| Desired state | Coordinator API |
| --- | --- |
| Group existence | `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` and `DELETE /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` |
| Initial shard count | create group request |
| Target shard count | `POST /coord/v1/streams/{streamPrefix}/scale` |
| Operational reason and actor metadata | mutation request body plus `X-Request-Id` |

Terraform or GitOps should not manage:

| Runtime state | Reason |
| --- | --- |
| Member heartbeat | It is ephemeral liveness and ownership reconciliation. |
| Consumer runtime parallelism | It belongs to consumer deployment configuration, such as `@StreamListener(concurrency = N)`, not the coordinator admin API. |
| Current assignment reports | They are member-reported observations, not desired state. |
| Redis Stream offsets, pending entries, and message payloads | They belong to the data plane. |
| Revoke/drain progress | It is a live rebalance transition. |
| Producer routing cache | It is local application runtime cache. |

## Recommended Provider Contract

The project should expose a Terraform provider around the stable `/coord/v1` API instead of asking users to hand-write local scripts for production mutation.

Recommended resources:

| Resource | Responsibility |
| --- | --- |
| `redisstreamcoordinator_group` | Own one `{streamPrefix, consumerGroup}` metadata record and initial shard count. |
| `redisstreamcoordinator_group_shard_count` | Apply explicit shard count changes when a group already exists and wait for the coordinator to expose the new producer routing metadata. |

The provider should support import ids in this format:

```text
{streamPrefix}/{consumerGroup}
```

The provider should include these headers on mutation calls:

```http
X-Request-Id: terraform-${workspace}-${run_id}
Authorization: Basic <write-user>
```

Mutation request bodies should include:

```json
{
  "requestedBy": "terraform:<workspace>",
  "reason": "terraform apply <run id>"
}
```

## Provided Terraform Module

The repository includes an initial Terraform module at:

```text
terraform/modules/stream-group
```

The module is designed for teams that want Terraform-managed shard counts before a dedicated provider exists. It uses `terraform_data` plus a small `curl` runner to call the Coordinator Admin API.

Managed inputs:

| Input | Meaning |
| --- | --- |
| `coordinator_base_url` | Coordinator API base URL. |
| `coordinator_username` / `coordinator_password` | Optional Basic Auth credentials with `WRITE` permission. |
| `stream_prefix` | Redis Stream shard prefix. |
| `consumer_group` | Redis Stream consumer group. |
| `shard_count` | Desired physical shard count. Changing this value calls `/scale`. |
| `requested_by` / `reason` | Audit metadata recorded by the coordinator. |

Apply behavior:

1. `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` creates the group if it does not exist.
2. If the group already exists, `409` is treated as an expected condition.
3. The module reads `GET /producer-routing`.
4. If the current `shardCount` differs from `shard_count`, it calls `POST /coord/v1/streams/{streamPrefix}/scale`.
5. The module polls producer routing metadata until the desired shard count is visible.

Destroy behavior is intentionally a no-op. Terraform destroy must not accidentally delete coordinator metadata or active stream assignments. Deleting a group remains an explicit Coordinator Admin API operation with audit logging and safety validation.

Example:

```hcl
module "create_order_group" {
  source = "../../modules/stream-group"

  coordinator_base_url = "https://api.example.com"
  coordinator_username = var.coordinator_username
  coordinator_password = var.coordinator_password

  stream_prefix  = "create-order"
  consumer_group = "demo-workers"
  shard_count    = 20

  requested_by = "terraform:${terraform.workspace}"
  reason       = "scale create-order stream group"
}
```

## Drift Detection

Terraform read operations should call coordinator source-of-truth APIs:

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing
```

The provider should treat these fields as managed desired state:

* group existence,
* `shardCount`.

The provider should treat these fields as computed runtime state:

* consumer runtime parallelism reported through heartbeat,
* `groupEpoch`,
* `assignmentEpoch`,
* `metadataVersion`,
* `members`,
* `targetAssignments`,
* `currentAssignments`,
* `migrations`,
* `revokeProgress`.

## Audit Evidence

Every admin mutation must produce a coordinator audit event even when Terraform is the caller.

Audit events should include:

* `action`,
* `outcome`,
* HTTP status,
* authenticated principal,
* `requestedBy`,
* `reason`,
* `requestId`,
* client address,
* user agent,
* roles,
* route and path,
* duration,
* stream prefix,
* consumer group,
* resharding id when present,
* request summary,
* SHA-256 request body fingerprint.

For Redis-backed audit, events are written to:

```text
redis-stream:coord:{streamPrefix:consumerGroup}:admin:audit
```

For log-backed audit, events are emitted as structured application logs.

## Failure Handling

Terraform apply must not assume that an accepted resharding is immediately complete. The scale API returns `202 Accepted`; consumers converge through heartbeat reconciliation.

Recommended flow:

1. Apply the desired shard count.
2. Poll producer routing metadata until `shardCount` equals the requested target.
3. Poll monitoring APIs until assignment and revoke progress are healthy.
4. Fail the Terraform run if the coordinator reports an active incompatible resharding or unsafe delete condition.

For `targetShardCount=0`, step 2 means producer routing returns `shardCount=0` and an empty shard list. Terraform must still poll monitoring state until every removed stream shard is drained for every Redis consumer group before treating the stream as retired.

Duplicate-sensitive workloads must still quiesce producers before shard count changes. Terraform can orchestrate the admin mutation, but it cannot prove that all application publish retries are drained unless the application exposes that signal.
