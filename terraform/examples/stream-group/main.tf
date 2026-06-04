terraform {
  required_version = ">= 1.5.0"
}

module "create_order_group" {
  source = "../../modules/stream-group"

  coordinator_base_url = var.coordinator_base_url
  coordinator_username = var.coordinator_username
  coordinator_password = var.coordinator_password

  stream_prefix  = var.stream_prefix
  consumer_group = var.consumer_group
  shard_count    = var.shard_count

  requested_by = "terraform:${terraform.workspace}"
  reason       = var.reason
}
