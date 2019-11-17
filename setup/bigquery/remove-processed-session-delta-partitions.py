from google.cloud import bigquery

def remove_partitions():
  client = bigquery.Client()

  query_file = open('get-session-delta-partitions-safe-to-delete.sql', "r")
  query = query_file.read()
  query_job = client.query(query)

  results = query_job.result()

  for row in results:
    partition_id = row.partition_id
    table_ref = client.dataset('data').table("{}${}".format('session_delta', partition_id))
    print("Partition to be deleted: {}".format(table_ref))
    client.delete_table(table_ref)

if __name__ == '__main__':
  remove_partitions()