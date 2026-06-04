output "id" {
  description = "Coordinator group identity."
  value       = "${var.stream_prefix}/${var.consumer_group}"
}

output "stream_prefix" {
  description = "Managed Redis Stream shard prefix."
  value       = var.stream_prefix
}

output "consumer_group" {
  description = "Managed Redis Stream consumer group."
  value       = var.consumer_group
}

output "shard_count" {
  description = "Desired physical shard count declared by Terraform."
  value       = var.shard_count
}
