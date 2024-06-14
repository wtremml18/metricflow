-- Re-aggregate Metrics via Window Functions
SELECT
  metric_time__year
  , trailing_2_months_revenue
FROM (
  SELECT
    metric_time__year
    , FIRST_VALUE(trailing_2_months_revenue) OVER (
      PARTITION BY metric_time__year
      ORDER BY metric_time__day
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS trailing_2_months_revenue
  FROM (
    -- Join Self Over Time Range
    -- Pass Only Elements: ['txn_revenue', 'metric_time__year', 'metric_time__day']
    -- Aggregate Measures
    -- Compute Metrics via Expressions
    SELECT
      subq_11.metric_time__day AS metric_time__day
      , subq_11.metric_time__year AS metric_time__year
      , SUM(revenue_src_28000.revenue) AS trailing_2_months_revenue
    FROM (
      -- Time Spine
      SELECT
        ds AS metric_time__day
        , DATE_TRUNC('year', ds) AS metric_time__year
      FROM ***************************.mf_time_spine subq_12
      GROUP BY
        ds
        , DATE_TRUNC('year', ds)
    ) subq_11
    INNER JOIN
      ***************************.fct_revenue revenue_src_28000
    ON
      (
        DATE_TRUNC('day', revenue_src_28000.created_at) <= subq_11.metric_time__day
      ) AND (
        DATE_TRUNC('day', revenue_src_28000.created_at) > subq_11.metric_time__day - MAKE_INTERVAL(months => 2)
      )
    GROUP BY
      subq_11.metric_time__day
      , subq_11.metric_time__year
  ) subq_16
) subq_17
GROUP BY
  metric_time__year
  , trailing_2_months_revenue
