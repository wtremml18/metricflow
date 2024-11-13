test_name: test_offset_window_metric_multiple_granularities
test_filename: test_derived_metric_rendering.py
docstring:
  Test a query where an offset window metric is queried with multiple granularities.
sql_engine: DuckDB
---
-- Read From CTE For node_id=cm_8
WITH cm_6_cte AS (
  -- Join to Time Spine Dataset
  -- Pass Only Elements: ['booking_value', 'metric_time__day', 'metric_time__month', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    subq_17.ds AS metric_time__day
    , DATE_TRUNC('month', subq_17.ds) AS metric_time__month
    , DATE_TRUNC('year', subq_17.ds) AS metric_time__year
    , SUM(bookings_source_src_28000.booking_value) AS booking_value
  FROM ***************************.mf_time_spine subq_17
  INNER JOIN
    ***************************.fct_bookings bookings_source_src_28000
  ON
    subq_17.ds - INTERVAL 1 week = DATE_TRUNC('day', bookings_source_src_28000.ds)
  GROUP BY
    subq_17.ds
    , DATE_TRUNC('month', subq_17.ds)
    , DATE_TRUNC('year', subq_17.ds)
)

, cm_7_cte AS (
  -- Read Elements From Semantic Model 'bookings_source'
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['bookers', 'metric_time__day', 'metric_time__month', 'metric_time__year']
  -- Aggregate Measures
  -- Compute Metrics via Expressions
  SELECT
    DATE_TRUNC('day', ds) AS metric_time__day
    , DATE_TRUNC('month', ds) AS metric_time__month
    , DATE_TRUNC('year', ds) AS metric_time__year
    , COUNT(DISTINCT guest_id) AS bookers
  FROM ***************************.fct_bookings bookings_source_src_28000
  GROUP BY
    DATE_TRUNC('day', ds)
    , DATE_TRUNC('month', ds)
    , DATE_TRUNC('year', ds)
)

, cm_8_cte AS (
  -- Compute Metrics via Expressions
  SELECT
    metric_time__day
    , metric_time__month
    , metric_time__year
    , booking_value * 0.05 / bookers AS booking_fees_last_week_per_booker_this_week
  FROM (
    -- Combine Aggregated Outputs
    SELECT
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day) AS metric_time__day
      , COALESCE(cm_6_cte.metric_time__month, cm_7_cte.metric_time__month) AS metric_time__month
      , COALESCE(cm_6_cte.metric_time__year, cm_7_cte.metric_time__year) AS metric_time__year
      , MAX(cm_6_cte.booking_value) AS booking_value
      , MAX(cm_7_cte.bookers) AS bookers
    FROM cm_6_cte cm_6_cte
    FULL OUTER JOIN
      cm_7_cte cm_7_cte
    ON
      (
        cm_6_cte.metric_time__day = cm_7_cte.metric_time__day
      ) AND (
        cm_6_cte.metric_time__month = cm_7_cte.metric_time__month
      ) AND (
        cm_6_cte.metric_time__year = cm_7_cte.metric_time__year
      )
    GROUP BY
      COALESCE(cm_6_cte.metric_time__day, cm_7_cte.metric_time__day)
      , COALESCE(cm_6_cte.metric_time__month, cm_7_cte.metric_time__month)
      , COALESCE(cm_6_cte.metric_time__year, cm_7_cte.metric_time__year)
  ) subq_27
)

SELECT
  metric_time__day AS metric_time__day
  , metric_time__month AS metric_time__month
  , metric_time__year AS metric_time__year
  , booking_fees_last_week_per_booker_this_week AS booking_fees_last_week_per_booker_this_week
FROM cm_8_cte cm_8_cte
