output "instance_metadata" {
  description = "Metadata of the compute instances created"
  value = [
    for instance in google_compute_instance.cos_instances : {
      name            = instance.name
      internal_ip     = instance.network_interface[0].network_ip
      machine_type    = instance.machine_type
      zone            = instance.zone
      tags            = instance.tags
      metadata        = instance.metadata
      service_account = instance.service_account[0].email
    }
  ]
}

output "proxy_service_account_email" {
  description = "Service account email attached to the ZDM proxy VMs"
  value       = local.proxy_service_account
}
