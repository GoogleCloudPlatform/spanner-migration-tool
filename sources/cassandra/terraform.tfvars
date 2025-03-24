common_params = {
  spanner_project_id   = "test-project"
  spanner_instance_id  = "test-instance"
  spanner_database_id  = "test-db"
  region               = "us-central1"
  zone                 = "us-central1-a"
  instance_count       = "10"
  instance_name_prefix = "node-10-zdm"
  network              = "default"
  machine_type         = "c2-standard-30"
  service_account_key  = "path/to/local/key/file"
}
zdm_config = {
  origin_contact_points = "10.128.0.14,10.128.0.15"
  origin_username       = "cassandra"
  origin_password       = "cassandra"
}