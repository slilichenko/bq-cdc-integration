resource "google_bigtable_instance" "instance" {
  name = var.bigtable_instance_id
  instance_type = "DEVELOPMENT"
  cluster {
    cluster_id = "bq-sync-instance-cluster"
    zone = "us-central1-b"
    storage_type = "HDD"
  }
}

resource "google_bigtable_table" "session" {
  name = "session"
  instance_name = "${google_bigtable_instance.instance.name}"
  column_family {
    family = "main"
  }
}