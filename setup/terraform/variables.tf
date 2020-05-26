variable "project_id" {
  type = string
}

variable "bigtable_instance_id" {
  type = string
  default = "bq-sync-instance"
}

variable "bigquery_dataset_id" {
  type = string
  default = "cdc_demo"
}