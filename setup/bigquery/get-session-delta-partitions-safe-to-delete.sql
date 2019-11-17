SELECT
  FORMAT_TIMESTAMP("%Y%m%d", pt) AS partition_id
FROM (
  SELECT
    pt,
    ROW_NUMBER() OVER (ORDER BY pt DESC) AS row_num
  FROM (
    SELECT
      DISTINCT(_partitiontime) AS pt
    FROM
      data.session_delta d,
      data.session s
    WHERE
      d.session_id = s.session_id
      AND d._partitiontime IS NOT NULL))
WHERE
  row_num > 1
ORDER BY
  partition_id