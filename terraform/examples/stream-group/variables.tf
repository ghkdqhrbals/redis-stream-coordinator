variable "coordinator_base_url" {
  description = "Coordinator base URL."
  type        = string
}

variable "coordinator_username" {
  description = "Coordinator Basic Auth username with WRITE permission."
  type        = string
  default     = ""
}

variable "coordinator_password" {
  description = "Coordinator Basic Auth password with WRITE permission."
  type        = string
  default     = ""
  sensitive   = true
}

variable "stream_prefix" {
  description = "Redis Stream shard prefix."
  type        = string
  default     = "create-order"
}

variable "consumer_group" {
  description = "Redis Stream consumer group."
  type        = string
  default     = "demo-workers"
}

variable "shard_count" {
  description = "Desired physical shard count."
  type        = number
  default     = 20
}

variable "reason" {
  description = "Reason recorded in coordinator audit logs."
  type        = string
  default     = "terraform managed shard count"
}
