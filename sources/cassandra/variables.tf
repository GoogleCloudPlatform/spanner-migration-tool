variable "common_params" {
  type = object({
    spanner_project_id   = string
    spanner_instance_id  = string
    spanner_database_id  = string
    region               = string
    zone                 = string
    instance_count       = optional(string, 1)
    instance_name_prefix = string
    network              = optional(string, "default")
    machine_type         = optional(string, "n2-standard-8")
    service_account_key  = string # Path to service account key file with Spanner write access
  })
  description = "Common parameters for GCP configuration."
}

variable "zdm_config" {
  type = object({
    origin_contact_points        = string
    origin_port                  = optional(number, 9042)
    origin_username              = string
    origin_password              = string
    proxy_listen_address         = optional(string, "0.0.0.0")
    proxy_listen_port            = optional(number, 14002)
    proxy_request_timeout_ms     = optional(number, 10000)
    proxy_max_client_connections = optional(number, 1000)
    proxy_max_stream_ids         = optional(number, 2048)
    log_level                    = optional(string, "WARN")

    target_contact_points        = optional(string, "127.0.0.1") # This should not be changed if running Cassandra to Spanner proxy as a sidecar. 
    target_port                  = optional(number, 9042)        # This should not be changed if running Cassandra to Spanner proxy as a sidecar. 
    target_username              = optional(string, "cassandra") # This should not be changed if running Cassandra to Spanner proxy as a sidecar. 
    target_password              = optional(string, "cassandra") # This should not be changed if running Cassandra to Spanner proxy as a sidecar. 
 
  })
  description = "ZDM Proxy configuration."
}