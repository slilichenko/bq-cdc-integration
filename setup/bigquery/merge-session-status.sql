SELECT *, CURRENT_TIMESTAMP() as measured_ts FROM
(SELECT
  'Records not in destination' AS description,
  COUNT(*) AS count
FROM
  data.session_source_v src
WHERE
  NOT EXISTS(
  SELECT
    session_id
  FROM
    data.session dest
  WHERE
    dest.session_id = src.session_id)
UNION ALL
SELECT
  'Records not in source' AS description,
  COUNT(*) AS count
FROM
  data.session dest
WHERE
  NOT EXISTS(
  SELECT
    session_id
  FROM
    data.session_source_v src
  WHERE
    dest.session_id = src.session_id)
UNION ALL
SELECT
  'Records with data mismatch' AS description,
  COUNT(*) AS count
FROM
  data.session_source_v src
INNER JOIN
  data.session dest
ON
  dest.session_id = src.session_id
  WHERE dest.status <> src.status) ORDER BY 1