MERGE
  `data.session` m
USING
  (
  SELECT
    * EXCEPT(last)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY delta.session_id ORDER BY delta.di_sequence_number DESC) AS Last
    FROM
      `data.session_delta` delta )
  WHERE
    Last = 1) d
ON
  m.session_id = d.session_id
  WHEN NOT MATCHED AND di_operation_type IN ('I', 'U') THEN INSERT (session_id, status, customer_key, start_ts, end_ts, last_di_sequence_number) VALUES (d.session_id, d.status, d.customer_key, d.start_ts, d.end_ts, d.di_sequence_number)
  WHEN MATCHED
  AND d.di_operation_type = 'D' THEN
DELETE
  WHEN MATCHED
  AND d.di_operation_type = 'U'
  AND (m.status <> d.status) THEN
UPDATE
SET
  status = d.status,
  customer_key = d.customer_key,
  start_ts = d.start_ts,
  end_ts = d.end_ts,
  last_di_sequence_number = d.di_sequence_number