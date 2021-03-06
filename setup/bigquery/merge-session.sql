MERGE
  `cdc_demo.session_main` m
USING
  (
  SELECT
    * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY delta.session_id ORDER BY delta.di_sequence_number DESC) AS row_num
    FROM
      `cdc_demo.session_delta` delta )
  WHERE
    row_num = 1) d
ON
  m.session_id = d.session_id
  WHEN NOT MATCHED AND di_operation_type IN ('I', 'U') THEN INSERT (session_id, status, customer_key, start_ts, end_ts, last_di_sequence_number) VALUES (d.session_id, d.status, d.customer_key, d.start_ts, d.end_ts, d.di_sequence_number)
  WHEN MATCHED
  AND d.di_operation_type = 'D' THEN
DELETE
  WHEN MATCHED
  AND d.di_operation_type = 'U'
  AND (m.last_di_sequence_number < d.di_sequence_number) THEN
UPDATE
SET
  status = d.status,
  customer_key = d.customer_key,
  start_ts = d.start_ts,
  end_ts = d.end_ts,
  last_di_sequence_number = d.di_sequence_number