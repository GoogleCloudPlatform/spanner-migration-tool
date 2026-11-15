locals {
  create_service_account = var.common_params.service_account_email == null
  generated_sa_id        = "${trimsuffix(substr(lower(replace(var.common_params.instance_name_prefix, "_", "-")), 0, 23), "-")}-zdm-sa"
  proxy_service_account  = local.create_service_account ? google_service_account.zdm_proxy[0].email : var.common_params.service_account_email

  has_origin_password_secret = var.zdm_config.origin_password_secret != null && var.zdm_config.origin_password_secret != ""
  origin_password_secret_id = !local.has_origin_password_secret ? null : (
    can(regex("^projects/[^/]+/secrets/[^/]+/versions/[^/]+$", var.zdm_config.origin_password_secret))
    ? regex("^(projects/[^/]+/secrets/[^/]+)/versions/[^/]+$", var.zdm_config.origin_password_secret)[0]
    : (
      can(regex("^projects/[^/]+/secrets/[^/]+$", var.zdm_config.origin_password_secret))
      ? var.zdm_config.origin_password_secret
      : "projects/${var.common_params.spanner_project_id}/secrets/${var.zdm_config.origin_password_secret}"
    )
  )
  origin_password_secret_version = !local.has_origin_password_secret ? "" : (
    can(regex("^projects/[^/]+/secrets/[^/]+/versions/[^/]+$", var.zdm_config.origin_password_secret))
    ? var.zdm_config.origin_password_secret
    : "${local.origin_password_secret_id}/versions/latest"
  )

  gcloud_iap_flag = var.common_params.enable_external_ip ? "" : "--tunnel-through-iap"
}

resource "google_service_account" "zdm_proxy" {
  count        = local.create_service_account ? 1 : 0
  account_id   = local.generated_sa_id
  display_name = "ZDM Proxy Service Account (${var.common_params.instance_name_prefix})"
  project      = var.common_params.spanner_project_id
}

resource "google_spanner_database_iam_member" "zdm_proxy_spanner_user" {
  count    = local.create_service_account ? 1 : 0
  project  = var.common_params.spanner_project_id
  instance = var.common_params.spanner_instance_id
  database = var.common_params.spanner_database_id
  role     = "roles/spanner.databaseUser"
  member   = "serviceAccount:${google_service_account.zdm_proxy[0].email}"
}

resource "google_project_iam_member" "zdm_proxy_metric_writer" {
  count   = local.create_service_account ? 1 : 0
  project = var.common_params.spanner_project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.zdm_proxy[0].email}"
}

resource "google_secret_manager_secret_iam_member" "zdm_proxy_origin_password" {
  count     = local.create_service_account && local.has_origin_password_secret ? 1 : 0
  secret_id = local.origin_password_secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.zdm_proxy[0].email}"
}

resource "google_compute_firewall" "allow_ports" {
  name    = "allow-zdm-ports"
  network = var.common_params.network

  allow {
    protocol = "tcp"
    ports    = ["14002", "9042"]
  }

  source_ranges = var.common_params.allowed_source_ranges
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

    dynamic "access_config" {
      for_each = var.common_params.enable_external_ip ? [1] : []
      content {}
    }
  }

  service_account {
    email  = local.proxy_service_account
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  tags = ["zdm"]

  metadata = {
    enable-oslogin = "TRUE"
  }
}

resource "local_file" "zdm_config" {
  count           = var.common_params.instance_count
  filename        = "zdm-config-${count.index}.yaml"
  file_permission = "0600"
  content = yamlencode({
    origin_contact_points = var.zdm_config.origin_contact_points
    origin_port           = var.zdm_config.origin_port
    origin_username       = var.zdm_config.origin_username

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
      set -e
      retry() {
        for i in 1 2 3; do
          if "$@"; then
            return 0
          fi
          if [ "$i" -lt 3 ]; then
            sleep 10
          fi
        done
        return 1
      }

      retry gcloud compute scp ${local.gcloud_iap_flag} Dockerfile entrypoint.sh spanner-cassandra-config.yaml zdm-config-${count.index}.yaml "${google_compute_instance.cos_instances[count.index].name}:~/" --project="${var.common_params.spanner_project_id}" --zone="${var.common_params.zone}"

      retry gcloud compute ssh ${local.gcloud_iap_flag} "${google_compute_instance.cos_instances[count.index].name}" --project="${var.common_params.spanner_project_id}" --zone="${var.common_params.zone}" --command="sudo docker build -t docker-test:latest ."

      retry gcloud compute ssh ${local.gcloud_iap_flag} "${google_compute_instance.cos_instances[count.index].name}" --project="${var.common_params.spanner_project_id}" --zone="${var.common_params.zone}" --command="sudo docker rm -f zdm-proxy >/dev/null 2>&1 || true; sudo docker run --name zdm-proxy --restart always -d -p 14002:14002 -v ~/zdm-config-${count.index}.yaml:/zdm-config.yaml:ro -e SPANNER_PROJECT='${var.common_params.spanner_project_id}' -e SPANNER_INSTANCE='${var.common_params.spanner_instance_id}' -e SPANNER_DATABASE='${var.common_params.spanner_database_id}' -e ZDM_CONFIG=/zdm-config.yaml -e ORIGIN_PASSWORD_SECRET='${local.origin_password_secret_version}' docker-test:latest"
    EOT
  }

  depends_on = [
    google_compute_instance.cos_instances,
    local_file.zdm_config,
    google_spanner_database_iam_member.zdm_proxy_spanner_user,
    google_project_iam_member.zdm_proxy_metric_writer,
    google_secret_manager_secret_iam_member.zdm_proxy_origin_password,
  ]
}
