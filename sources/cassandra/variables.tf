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
  })
  description = "Common parameters for GCP configuration."
}

variable "zdm_config" {
  type = object({
    origin_contact_points = string
    origin_port           = optional(string, "9042")
    origin_username       = string
    origin_password       = string
    target_contact_points = optional(string, "127.0.0.1")
    target_username       = optional(string, "cassandra")
    target_password       = optional(string, "cassandra")
    proxy_listen_address  = optional(string, "0.0.0.0")
  })
  description = "ZDM Proxy configuration."
}