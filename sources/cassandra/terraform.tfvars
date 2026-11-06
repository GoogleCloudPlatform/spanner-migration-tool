common_params = {
  spanner_project_id    = "test-project"
  spanner_instance_id   = "test-instance"
  spanner_database_id   = "test-db"
  region                = "us-central1"
  zone                  = "us-central1-a"
  instance_count        = 10
  instance_name_prefix  = "node-10-zdm"
  network               = "default"
  machine_type          = "c2-standard-30"
  allowed_source_ranges = ["10.128.0.0/9"]
  # Optional: provide an existing service account email instead of having Terraform create one.
  # service_account_email = "zdm-proxy@test-project.iam.gserviceaccount.com"
}
zdm_config = {
  origin_contact_points  = "10.128.0.14,10.128.0.15"
  origin_username        = "cassandra"
  origin_password_secret = "cassandra-origin-password"
}
