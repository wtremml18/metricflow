-- Constrain Output with WHERE
-- Pass Only Elements: ['bookers', 'metric_time__day']
-- Aggregate Measures
-- Compute Metrics via Expressions
SELECT
  metric_time__day
  , COUNT(DISTINCT bookers) AS every_two_days_bookers
FROM (
  -- Join Self Over Time Range
  SELECT
    subq_11.ds AS metric_time__day
    , bookings_source_src_28000.guest_id AS bookers
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN
    ***************************.fct_bookings bookings_source_src_28000
  ON
    (
      DATETIME_TRUNC(bookings_source_src_28000.ds, day) <= subq_11.ds
    ) AND (
      DATETIME_TRUNC(bookings_source_src_28000.ds, day) > DATE_SUB(CAST(subq_11.ds AS DATETIME), INTERVAL 2 day)
    )
) subq_12
WHERE metric_time__day = '2020-01-03' or metric_time__day = '2020-01-07'
GROUP BY
  metric_time__day
