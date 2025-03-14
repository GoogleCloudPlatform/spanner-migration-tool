common_params = {
  spanner_project_id   = "test-project"
  spanner_instance_id  = "test-instance"
  spanner_database_id  = "test-db"
  region               = "us-central1"
  zone                 = "us-central1-a"
  instance_count       = "10"
  instance_name_prefix = "node-10-zdm"
  network              = "default"
  machine_type         = "n2-standard-8"
}
zdm_config = {
  origin_contact_points = "10.128.0.14,10.128.0.15"
  origin_port           = "9042"
  target_contact_points = "127.0.0.1"
  target_username       = "cassandra"
  target_password       = "cassandra"
  proxy_listen_address  = "0.0.0.0"
}