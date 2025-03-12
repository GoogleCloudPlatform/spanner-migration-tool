output "instance_metadata" {
  description = "Metadata of the compute instances created"
  value = [
    for instance in google_compute_instance.cos_instances : {
      name         = instance.name
      internal_ip  = instance.network_interface[0].network_ip
      external_ip  = instance.network_interface[0].access_config[0].nat_ip
      machine_type = instance.machine_type
      zone         = instance.zone
      tags         = instance.tags
      metadata     = instance.metadata
    }
  ]
} 