variable "coordinator_base_url" {
  description = "Coordinator base URL, for example https://api.example.com or http://localhost:8080."
  type        = string
}

variable "coordinator_username" {
  description = "Coordinator Basic Auth username with WRITE permission. Leave empty when auth is disabled."
  type        = string
  default     = ""
}

variable "coordinator_password" {
  description = "Coordinator Basic Auth password with WRITE permission. This value is passed to curl at apply time and is not stored in the terraform_data input."
  type        = string
  default     = ""
  sensitive   = true
}

variable "stream_prefix" {
  description = "Redis Stream shard prefix managed by the coordinator, for example create-order."
  type        = string
}

variable "consumer_group" {
  description = "Redis Stream consumer group name managed by the coordinator."
  type        = string
}

variable "shard_count" {
  description = "Desired physical Redis Stream shard count. Changing this value calls the coordinator scale API."
  type        = number

  validation {
    condition     = var.shard_count >= 1
    error_message = "shard_count must be at least 1."
  }
}

variable "requested_by" {
  description = "Actor recorded in coordinator audit logs."
  type        = string
  default     = "terraform"
}

variable "reason" {
  description = "Human-readable reason recorded in coordinator audit logs."
  type        = string
  default     = "terraform managed shard count"
}

variable "request_id_prefix" {
  description = "Prefix used for X-Request-Id on coordinator admin API calls."
  type        = string
  default     = "terraform"
}

variable "timeout_seconds" {
  description = "Maximum time to wait for producer routing metadata to expose the desired shard count."
  type        = number
  default     = 120
}

variable "poll_interval_seconds" {
  description = "Polling interval for producer routing metadata after a scale request."
  type        = number
  default     = 2
}
