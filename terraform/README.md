# Terraform Shard Management

This directory contains Terraform support for coordinator-managed Redis Stream shard counts.

The first module is intentionally small:

* It creates a coordinator group when the group does not exist.
* It calls the stream-level coordinator scale API when `shard_count` changes.
* It waits until producer routing metadata exposes the desired shard count.
* It does not delete coordinator metadata during `terraform destroy`.

The coordinator remains the only writer for group metadata. Terraform declares desired shard count and records review history, while the coordinator performs validation, Redis stream provisioning, mutex protection, epoch updates, rebalance state transitions, and audit logging.

## Example

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

Apply:

```bash
terraform init
terraform apply
```

Scale up or down by changing `shard_count` and running `terraform apply` again. The module calls `POST /coord/v1/streams/{streamPrefix}/scale`; consumer groups are not part of the scale path and receive the new assignment plan on heartbeat.

Duplicate-sensitive workloads must quiesce producers before changing shard count. A shard count change changes the routing domain, so the same partition key can route to a different physical Redis Stream shard after the update.
