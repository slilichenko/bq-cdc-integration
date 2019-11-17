SELECT
  *,
  CURRENT_TIMESTAMP() AS measured_ts
FROM (
  SELECT
    'Records not in destination' AS description,
    COUNT(*) AS count
  FROM
    cdc_demo.session_source_v src
  WHERE
    NOT EXISTS(
    SELECT
      session_id
    FROM
      cdc_demo.session_latest_v dest
    WHERE
      dest.session_id = src.session_id)
  UNION ALL
  SELECT
    'Records not in source' AS description,
    COUNT(*) AS count
  FROM
    cdc_demo.session_latest_v dest
  WHERE
    NOT EXISTS(
    SELECT
      session_id
    FROM
      cdc_demo.session_source_v src
    WHERE
      dest.session_id = src.session_id)
  UNION ALL
  SELECT
    'Records with data mismatch' AS description,
    COUNT(*) AS count
  FROM
    cdc_demo.session_source_v src
  INNER JOIN
    cdc_demo.session_latest_v dest
  ON
    dest.session_id = src.session_id
  WHERE
    dest.status <> src.status
  UNION ALL
  SELECT
    'Total record in the source' AS description,
    COUNT(*) AS count
  FROM
    cdc_demo.session_source_v src )
ORDER BY
  description