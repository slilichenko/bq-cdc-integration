resource "google_bigquery_dataset" "data" {
  dataset_id = "data"
  friendly_name = "data"
  description = "Dataset to store all the data in this project"
  location = "US"

  depends_on = ["google_bigtable_table.session"]
  provisioner "local-exec" {
    command = "./create-bigtable-session-link.sh ${var.project_id} ${google_bigquery_dataset.data.dataset_id}"
  }
  provisioner "local-exec" {
    when = "destroy"
    command = "./remove-bigtable-session-link.sh ${google_bigquery_dataset.data.dataset_id}"
  }
}

variable "common_session_columns" {
  default = <<EOF
{
    "name": "session_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Primary key of the session"
  },
  {
    "name": "start_ts",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Session start"
  },
  {
    "name": "end_ts",
    "type": "TIMESTAMP",
    "mode": "NULLABLE",
    "description": "Session end"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Session status"
  },
  {
    "name": "customer_key",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Customer key"
  }
EOF
}

variable "delta_columns" {
  default = <<EOF
  {
    "name": "di_sequence_number",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "description": "Data Services-generated integer for sequencing"
  },
  {
    "name": "di_operation_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Data Services-generated row operation type"
  }
EOF
}

variable "last_update_seq_number" {
  default = <<EOF
  {
    "name": "last_di_sequence_number",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "description": "Data Services-generated integer for sequencing"
  }
EOF
}

resource "google_bigquery_table" "session" {
  dataset_id = "${google_bigquery_dataset.data.dataset_id}"
  table_id = "session"
  schema = "[${var.common_session_columns}, ${var.last_update_seq_number}]"
}

resource "google_bigquery_table" "session_delta" {
  dataset_id = google_bigquery_dataset.data.dataset_id
  table_id = "session_delta"

  time_partitioning {
    type = "DAY"
    // There is no field id provided here; the delta table will be partitioned by the data inject time
  }

  schema = "[${var.common_session_columns}, ${var.delta_columns}]"
}

resource "google_bigquery_table" "session_source_v" {
  dataset_id = "${google_bigquery_dataset.data.dataset_id}"
  table_id = "session_source_v"

  view {
    use_legacy_sql = false
    query = <<EOF
SELECT rowkey as session_id,
  main.customer_key.cell.value as customer_key,
  main.status.cell.value as status,
  TIMESTAMP(main.start_ts.cell.value) as start_ts,
  TIMESTAMP(main.end_ts.cell.value) as end_ts
  FROM `${var.project_id}.${google_bigquery_dataset.data.dataset_id}.source_session`
EOF
  }
}

resource "google_bigquery_table" "session_latest_v" {
  dataset_id = google_bigquery_dataset.data.dataset_id
  table_id = "session_latest_v"
  view {
    use_legacy_sql = false
    query = <<EOF
SELECT
  * EXCEPT(di_operation_type, row_num)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY di_sequence_number DESC) AS row_num
  FROM (
    SELECT
      * EXCEPT(di_operation_type),
      di_operation_type
    FROM
      `${var.project_id}.${google_bigquery_dataset.data.dataset_id}.${google_bigquery_table.session_delta.table_id}`
    UNION ALL
    SELECT
      *,
      'I'
    FROM
      `${var.project_id}.${google_bigquery_dataset.data.dataset_id}.${google_bigquery_table.session.table_id}`))
WHERE
  row_num = 1
  AND di_operation_type <> 'D'
EOF
  }
}
