output "stream_group_id" {
  description = "Coordinator group identity managed by Terraform."
  value       = module.create_order_group.id
}
