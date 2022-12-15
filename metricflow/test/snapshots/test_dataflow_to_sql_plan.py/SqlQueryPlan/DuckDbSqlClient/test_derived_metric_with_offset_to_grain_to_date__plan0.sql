-- Compute Metrics via Expressions
SELECT
  subq_11.metric_time
  , (bookings - bookings_at_start_of_month) / bookings_at_start_of_month AS bookings_growth_since_start_of_month
FROM (
  -- Combine Metrics
  SELECT
    COALESCE(subq_9.metric_time, subq_10.metric_time) AS metric_time
    , subq_9.bookings AS bookings
    , subq_10.bookings_at_start_of_month AS bookings_at_start_of_month
  FROM (
    -- Date Spine
    SELECT
      subq_8.ds AS metric_time
    FROM ***************************.mf_time_spine subq_8
  ) subq_8
  INNER JOIN (
    -- Compute Metrics via Expressions
    SELECT
      subq_7.metric_time
      , subq_7.bookings AS bookings_at_start_of_month
    FROM (
      -- Aggregate Measures
      SELECT
        subq_6.metric_time
        , SUM(subq_6.bookings) AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time']
        SELECT
          subq_5.metric_time
          , subq_5.bookings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_4.ds
            , subq_4.ds__week
            , subq_4.ds__month
            , subq_4.ds__quarter
            , subq_4.ds__year
            , subq_4.ds_partitioned
            , subq_4.ds_partitioned__week
            , subq_4.ds_partitioned__month
            , subq_4.ds_partitioned__quarter
            , subq_4.ds_partitioned__year
            , subq_4.booking_paid_at
            , subq_4.booking_paid_at__week
            , subq_4.booking_paid_at__month
            , subq_4.booking_paid_at__quarter
            , subq_4.booking_paid_at__year
            , subq_4.create_a_cycle_in_the_join_graph__ds
            , subq_4.create_a_cycle_in_the_join_graph__ds__week
            , subq_4.create_a_cycle_in_the_join_graph__ds__month
            , subq_4.create_a_cycle_in_the_join_graph__ds__quarter
            , subq_4.create_a_cycle_in_the_join_graph__ds__year
            , subq_4.create_a_cycle_in_the_join_graph__ds_partitioned
            , subq_4.create_a_cycle_in_the_join_graph__ds_partitioned__week
            , subq_4.create_a_cycle_in_the_join_graph__ds_partitioned__month
            , subq_4.create_a_cycle_in_the_join_graph__ds_partitioned__quarter
            , subq_4.create_a_cycle_in_the_join_graph__ds_partitioned__year
            , subq_4.create_a_cycle_in_the_join_graph__booking_paid_at
            , subq_4.create_a_cycle_in_the_join_graph__booking_paid_at__week
            , subq_4.create_a_cycle_in_the_join_graph__booking_paid_at__month
            , subq_4.create_a_cycle_in_the_join_graph__booking_paid_at__quarter
            , subq_4.create_a_cycle_in_the_join_graph__booking_paid_at__year
            , subq_4.ds AS metric_time
            , subq_4.ds__week AS metric_time__week
            , subq_4.ds__month AS metric_time__month
            , subq_4.ds__quarter AS metric_time__quarter
            , subq_4.ds__year AS metric_time__year
            , subq_4.listing
            , subq_4.guest
            , subq_4.host
            , subq_4.create_a_cycle_in_the_join_graph
            , subq_4.create_a_cycle_in_the_join_graph__listing
            , subq_4.create_a_cycle_in_the_join_graph__guest
            , subq_4.create_a_cycle_in_the_join_graph__host
            , subq_4.is_instant
            , subq_4.create_a_cycle_in_the_join_graph__is_instant
            , subq_4.bookings
            , subq_4.instant_bookings
            , subq_4.booking_value
            , subq_4.max_booking_value
            , subq_4.min_booking_value
            , subq_4.bookers
            , subq_4.average_booking_value
            , subq_4.referred_bookings
          FROM (
            -- Read Elements From Data Source 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_10001.booking_value
              , bookings_source_src_10001.booking_value AS max_booking_value
              , bookings_source_src_10001.booking_value AS min_booking_value
              , bookings_source_src_10001.guest_id AS bookers
              , bookings_source_src_10001.booking_value AS average_booking_value
              , bookings_source_src_10001.booking_value AS booking_payments
              , CASE WHEN referrer_id IS NOT NULL THEN 1 ELSE 0 END AS referred_bookings
              , bookings_source_src_10001.is_instant
              , bookings_source_src_10001.ds
              , DATE_TRUNC('week', bookings_source_src_10001.ds) AS ds__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds) AS ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds) AS ds__year
              , bookings_source_src_10001.ds_partitioned
              , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS ds_partitioned__year
              , bookings_source_src_10001.booking_paid_at
              , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS booking_paid_at__year
              , bookings_source_src_10001.is_instant AS create_a_cycle_in_the_join_graph__is_instant
              , bookings_source_src_10001.ds AS create_a_cycle_in_the_join_graph__ds
              , DATE_TRUNC('week', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds) AS create_a_cycle_in_the_join_graph__ds__year
              , bookings_source_src_10001.ds_partitioned AS create_a_cycle_in_the_join_graph__ds_partitioned
              , DATE_TRUNC('week', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.ds_partitioned) AS create_a_cycle_in_the_join_graph__ds_partitioned__year
              , bookings_source_src_10001.booking_paid_at AS create_a_cycle_in_the_join_graph__booking_paid_at
              , DATE_TRUNC('week', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10001.booking_paid_at) AS create_a_cycle_in_the_join_graph__booking_paid_at__year
              , bookings_source_src_10001.listing_id AS listing
              , bookings_source_src_10001.guest_id AS guest
              , bookings_source_src_10001.host_id AS host
              , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph
              , bookings_source_src_10001.listing_id AS create_a_cycle_in_the_join_graph__listing
              , bookings_source_src_10001.guest_id AS create_a_cycle_in_the_join_graph__guest
              , bookings_source_src_10001.host_id AS create_a_cycle_in_the_join_graph__host
            FROM (
              -- User Defined SQL Query
              SELECT * FROM ***************************.fct_bookings
            ) bookings_source_src_10001
          ) subq_4
        ) subq_5
      ) subq_6
      GROUP BY
        subq_6.metric_time
    ) subq_7
  ) subq_10
  ON
    DATE_TRUNC('month', subq_9.metric_time) = subq_10.metric_time
) subq_11
