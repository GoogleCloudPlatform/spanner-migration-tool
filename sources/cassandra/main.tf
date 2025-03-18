resource "google_compute_firewall" "allow_ports" {
  name    = "allow-zdm-ports"
  network = var.common_params.network

  allow {
    protocol = "tcp"
    ports    = ["14002", "9042"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["zdm"]
}

resource "google_compute_instance" "cos_instances" {
  count        = var.common_params.instance_count
  name         = "${var.common_params.instance_name_prefix}-${count.index}"
  machine_type = var.common_params.machine_type
  zone         = var.common_params.zone

  boot_disk {
    initialize_params {
      image = "projects/cos-cloud/global/images/cos-105-17412-535-55"
      size  = 32
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = var.common_params.network
    access_config {}
  }

  tags = ["zdm"]

  metadata = {
    enable-oslogin = "TRUE"
  }
}

resource "local_file" "zdm_config" {
  count    = var.common_params.instance_count
  filename = "zdm-config-${count.index}.yaml"
  content = yamlencode({
    origin_contact_points = var.zdm_config.origin_contact_points
    origin_port           = var.zdm_config.origin_port
    origin_username       = var.zdm_config.origin_username
    origin_password       = var.zdm_config.origin_password

    target_contact_points = var.zdm_config.target_contact_points
    target_port           = var.zdm_config.target_port
    target_username       = var.zdm_config.target_username
    target_password       = var.zdm_config.target_password

    proxy_listen_address         = var.zdm_config.proxy_listen_address
    proxy_listen_port            = var.zdm_config.proxy_listen_port
    proxy_request_timeout_ms     = var.zdm_config.proxy_request_timeout_ms
    proxy_max_client_connections = var.zdm_config.proxy_max_client_connections
    proxy_max_stream_ids         = var.zdm_config.proxy_max_stream_ids
    log_level                    = var.zdm_config.log_level
    proxy_topology_addresses     = join(",", google_compute_instance.cos_instances[*].network_interface[0].network_ip)
    proxy_topology_index         = count.index
  })
  depends_on = [google_compute_instance.cos_instances]
}

resource "null_resource" "setup_instances" {
  count = var.common_params.instance_count

  provisioner "local-exec" {
    command = <<-EOT
      for i in {1..3}; do
        if gcloud compute scp Dockerfile entrypoint.sh zdm-config-${count.index}.yaml ${google_compute_instance.cos_instances[count.index].name}:~/ --project="${var.common_params.spanner_project_id}" --zone "${var.common_params.zone}"; then
          break
        fi
        sleep 10
      done

      for i in {1..3}; do
        if gcloud compute scp ${var.common_params.service_account_key} ${google_compute_instance.cos_instances[count.index].name}:~/keys.json --project="${var.common_params.spanner_project_id}" --zone "${var.common_params.zone}"; then
          break
        fi
        sleep 10
      done

      for i in {1..3}; do
        if gcloud compute ssh ${google_compute_instance.cos_instances[count.index].name} --project="${var.common_params.spanner_project_id}" --zone "${var.common_params.zone}" --command="sudo docker build -t docker-test:latest ."; then
          break
        fi
        sleep 10
      done

      for i in {1..3}; do
        if gcloud compute ssh ${google_compute_instance.cos_instances[count.index].name} --project="${var.common_params.spanner_project_id}" --zone "${var.common_params.zone}" --command="sudo docker run --restart always -d -p 14002:14002 -v ~/zdm-config-${count.index}.yaml:/zdm-config.yaml -v ~/keys.json:/var/keys.json  -e SPANNER_PROJECT=${var.common_params.spanner_project_id} -e SPANNER_INSTANCE=${var.common_params.spanner_instance_id} -e SPANNER_DATABASE=${var.common_params.spanner_database_id} -e ZDM_CONFIG=/zdm-config.yaml -e GOOGLE_APPLICATION_CREDENTIALS='/var/keys.json' docker-test:latest"; then
          break
        fi
        sleep 10
      done
    EOT
  }

  depends_on = [google_compute_instance.cos_instances, local_file.zdm_config]
}