test_name: test_min_max_metric_time_week
test_filename: test_query_rendering.py
docstring:
  Tests a plan to get the min & max distinct values of metric_time with non-default granularity.
---
-- Calculate min and max
SELECT
  MIN(metric_time__week) AS metric_time__week__min
  , MAX(metric_time__week) AS metric_time__week__max
FROM (
  -- Time Spine
  -- Metric Time Dimension 'ds'
  -- Pass Only Elements: ['metric_time__week',]
  SELECT
    DATETIME_TRUNC(ds, isoweek) AS metric_time__week
  FROM ***************************.mf_time_spine time_spine_src_28006
  GROUP BY
    metric_time__week
) subq_5
