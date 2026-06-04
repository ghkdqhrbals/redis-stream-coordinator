terraform {
  required_version = ">= 1.5.0"
}

resource "terraform_data" "stream_group" {
  input = {
    coordinator_base_url = var.coordinator_base_url
    stream_prefix        = var.stream_prefix
    consumer_group       = var.consumer_group
    shard_count          = var.shard_count
  }

  triggers_replace = [
    var.coordinator_base_url,
    var.stream_prefix,
    var.consumer_group,
    tostring(var.shard_count),
  ]

  provisioner "local-exec" {
    command = "${path.module}/scripts/apply.sh"

    environment = {
      COORDINATOR_BASE_URL = var.coordinator_base_url
      COORDINATOR_USERNAME = var.coordinator_username
      COORDINATOR_PASSWORD = var.coordinator_password
      STREAM_PREFIX        = var.stream_prefix
      CONSUMER_GROUP       = var.consumer_group
      SHARD_COUNT          = tostring(var.shard_count)
      REQUESTED_BY         = var.requested_by
      REASON               = var.reason
      REQUEST_ID_PREFIX    = var.request_id_prefix
      TIMEOUT_SECONDS      = tostring(var.timeout_seconds)
      POLL_INTERVAL_SECONDS = tostring(var.poll_interval_seconds)
    }
  }

  provisioner "local-exec" {
    when    = destroy
    command = "echo 'redis-stream-coordinator stream-group destroy is a no-op; delete group metadata with the coordinator admin API when that is intentional.'"
  }
}
