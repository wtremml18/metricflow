test_name: test_derived_offset_metric_with_one_input_metric
test_filename: test_derived_metric_rendering.py
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_5
WITH cm_4_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['bookings', 'metric_time__day']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_11.ds AS metric_time__day
    , SUM(subq_9.bookings) AS bookings_5_days_ago
  FROM ***************************.mf_time_spine subq_11
  INNER JOIN (
    -- Read Elements From Semantic Model 'bookings_source'
    -- Metric Time Dimension 'ds'
    SELECT
      DATE_TRUNC('day', ds) AS metric_time__day
      , 1 AS bookings
    FROM ***************************.fct_bookings bookings_source_src_28000
  ) subq_9
  ON
    subq_11.ds - INTERVAL 5 day = subq_9.metric_time__day
  GROUP BY
    subq_11.ds
)

, cm_5_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , bookings_5_days_ago AS bookings_5_day_lag
  FROM (
    -- Read From CTE For node_id=cm_4
    SELECT
      metric_time__day
      , bookings_5_days_ago
    FROM cm_4_cte cm_4_cte
  ) subq_15
)

SELECT
  metric_time__day AS metric_time__day
  , bookings_5_day_lag AS bookings_5_day_lag
FROM cm_5_cte cm_5_cte
