-- Compute Metrics via Expressions
SELECT
  subq_10.metric_time__day
  , subq_10.listing__user__home_state_latest
  , subq_10.bookings
FROM (
  -- Aggregate Measures
  SELECT
    subq_9.metric_time__day
    , subq_9.listing__user__home_state_latest
    , SUM(subq_9.bookings) AS bookings
  FROM (
    -- Pass Only Elements:
    --   ['bookings', 'listing__user__home_state_latest', 'metric_time__day']
    SELECT
      subq_8.metric_time__day
      , subq_8.listing__user__home_state_latest
      , subq_8.bookings
    FROM (
      -- Join Standard Outputs
      SELECT
        subq_2.metric_time__day AS metric_time__day
        , subq_7.window_start__extract_year AS listing__window_start__extract_year
        , subq_7.window_end__extract_year AS listing__window_end__extract_year
        , subq_2.listing AS listing
        , subq_7.user__home_state_latest AS listing__user__home_state_latest
        , subq_2.bookings AS bookings
      FROM (
        -- Pass Only Elements:
        --   ['bookings', 'metric_time__day', 'listing']
        SELECT
          subq_1.metric_time__day
          , subq_1.listing
          , subq_1.bookings
        FROM (
          -- Metric Time Dimension 'ds'
          SELECT
            subq_0.ds__day
            , subq_0.ds__week
            , subq_0.ds__month
            , subq_0.ds__quarter
            , subq_0.ds__year
            , subq_0.ds__extract_year
            , subq_0.ds__extract_quarter
            , subq_0.ds__extract_month
            , subq_0.ds__extract_week
            , subq_0.ds__extract_day
            , subq_0.ds__extract_dayofweek
            , subq_0.ds__extract_dayofyear
            , subq_0.ds_partitioned__day
            , subq_0.ds_partitioned__week
            , subq_0.ds_partitioned__month
            , subq_0.ds_partitioned__quarter
            , subq_0.ds_partitioned__year
            , subq_0.ds_partitioned__extract_year
            , subq_0.ds_partitioned__extract_quarter
            , subq_0.ds_partitioned__extract_month
            , subq_0.ds_partitioned__extract_week
            , subq_0.ds_partitioned__extract_day
            , subq_0.ds_partitioned__extract_dayofweek
            , subq_0.ds_partitioned__extract_dayofyear
            , subq_0.paid_at__day
            , subq_0.paid_at__week
            , subq_0.paid_at__month
            , subq_0.paid_at__quarter
            , subq_0.paid_at__year
            , subq_0.paid_at__extract_year
            , subq_0.paid_at__extract_quarter
            , subq_0.paid_at__extract_month
            , subq_0.paid_at__extract_week
            , subq_0.paid_at__extract_day
            , subq_0.paid_at__extract_dayofweek
            , subq_0.paid_at__extract_dayofyear
            , subq_0.booking__ds__day
            , subq_0.booking__ds__week
            , subq_0.booking__ds__month
            , subq_0.booking__ds__quarter
            , subq_0.booking__ds__year
            , subq_0.booking__ds__extract_year
            , subq_0.booking__ds__extract_quarter
            , subq_0.booking__ds__extract_month
            , subq_0.booking__ds__extract_week
            , subq_0.booking__ds__extract_day
            , subq_0.booking__ds__extract_dayofweek
            , subq_0.booking__ds__extract_dayofyear
            , subq_0.booking__ds_partitioned__day
            , subq_0.booking__ds_partitioned__week
            , subq_0.booking__ds_partitioned__month
            , subq_0.booking__ds_partitioned__quarter
            , subq_0.booking__ds_partitioned__year
            , subq_0.booking__ds_partitioned__extract_year
            , subq_0.booking__ds_partitioned__extract_quarter
            , subq_0.booking__ds_partitioned__extract_month
            , subq_0.booking__ds_partitioned__extract_week
            , subq_0.booking__ds_partitioned__extract_day
            , subq_0.booking__ds_partitioned__extract_dayofweek
            , subq_0.booking__ds_partitioned__extract_dayofyear
            , subq_0.booking__paid_at__day
            , subq_0.booking__paid_at__week
            , subq_0.booking__paid_at__month
            , subq_0.booking__paid_at__quarter
            , subq_0.booking__paid_at__year
            , subq_0.booking__paid_at__extract_year
            , subq_0.booking__paid_at__extract_quarter
            , subq_0.booking__paid_at__extract_month
            , subq_0.booking__paid_at__extract_week
            , subq_0.booking__paid_at__extract_day
            , subq_0.booking__paid_at__extract_dayofweek
            , subq_0.booking__paid_at__extract_dayofyear
            , subq_0.ds__day AS metric_time__day
            , subq_0.ds__week AS metric_time__week
            , subq_0.ds__month AS metric_time__month
            , subq_0.ds__quarter AS metric_time__quarter
            , subq_0.ds__year AS metric_time__year
            , subq_0.ds__extract_year AS metric_time__extract_year
            , subq_0.ds__extract_quarter AS metric_time__extract_quarter
            , subq_0.ds__extract_month AS metric_time__extract_month
            , subq_0.ds__extract_week AS metric_time__extract_week
            , subq_0.ds__extract_day AS metric_time__extract_day
            , subq_0.ds__extract_dayofweek AS metric_time__extract_dayofweek
            , subq_0.ds__extract_dayofyear AS metric_time__extract_dayofyear
            , subq_0.listing
            , subq_0.guest
            , subq_0.host
            , subq_0.user
            , subq_0.booking__listing
            , subq_0.booking__guest
            , subq_0.booking__host
            , subq_0.booking__user
            , subq_0.is_instant
            , subq_0.booking__is_instant
            , subq_0.bookings
            , subq_0.instant_bookings
            , subq_0.booking_value
            , subq_0.bookers
            , subq_0.average_booking_value
          FROM (
            -- Read Elements From Semantic Model 'bookings_source'
            SELECT
              1 AS bookings
              , CASE WHEN is_instant THEN 1 ELSE 0 END AS instant_bookings
              , bookings_source_src_10015.booking_value
              , bookings_source_src_10015.guest_id AS bookers
              , bookings_source_src_10015.booking_value AS average_booking_value
              , bookings_source_src_10015.booking_value AS booking_payments
              , bookings_source_src_10015.is_instant
              , bookings_source_src_10015.ds AS ds__day
              , DATE_TRUNC('week', bookings_source_src_10015.ds) AS ds__week
              , DATE_TRUNC('month', bookings_source_src_10015.ds) AS ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.ds) AS ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.ds) AS ds__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.ds) AS ds__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.ds) AS ds__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.ds) AS ds__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.ds) AS ds__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.ds) AS ds__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.ds) AS ds__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.ds) AS ds__extract_dayofyear
              , bookings_source_src_10015.ds_partitioned AS ds_partitioned__day
              , DATE_TRUNC('week', bookings_source_src_10015.ds_partitioned) AS ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10015.ds_partitioned) AS ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.ds_partitioned) AS ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.ds_partitioned) AS ds_partitioned__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.ds_partitioned) AS ds_partitioned__extract_dayofyear
              , bookings_source_src_10015.paid_at AS paid_at__day
              , DATE_TRUNC('week', bookings_source_src_10015.paid_at) AS paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10015.paid_at) AS paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.paid_at) AS paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.paid_at) AS paid_at__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.paid_at) AS paid_at__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.paid_at) AS paid_at__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.paid_at) AS paid_at__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.paid_at) AS paid_at__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.paid_at) AS paid_at__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.paid_at) AS paid_at__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.paid_at) AS paid_at__extract_dayofyear
              , bookings_source_src_10015.is_instant AS booking__is_instant
              , bookings_source_src_10015.ds AS booking__ds__day
              , DATE_TRUNC('week', bookings_source_src_10015.ds) AS booking__ds__week
              , DATE_TRUNC('month', bookings_source_src_10015.ds) AS booking__ds__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.ds) AS booking__ds__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.ds) AS booking__ds__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.ds) AS booking__ds__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.ds) AS booking__ds__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.ds) AS booking__ds__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.ds) AS booking__ds__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.ds) AS booking__ds__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.ds) AS booking__ds__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.ds) AS booking__ds__extract_dayofyear
              , bookings_source_src_10015.ds_partitioned AS booking__ds_partitioned__day
              , DATE_TRUNC('week', bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__week
              , DATE_TRUNC('month', bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.ds_partitioned) AS booking__ds_partitioned__extract_dayofyear
              , bookings_source_src_10015.paid_at AS booking__paid_at__day
              , DATE_TRUNC('week', bookings_source_src_10015.paid_at) AS booking__paid_at__week
              , DATE_TRUNC('month', bookings_source_src_10015.paid_at) AS booking__paid_at__month
              , DATE_TRUNC('quarter', bookings_source_src_10015.paid_at) AS booking__paid_at__quarter
              , DATE_TRUNC('year', bookings_source_src_10015.paid_at) AS booking__paid_at__year
              , EXTRACT(YEAR FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_year
              , EXTRACT(QUARTER FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_quarter
              , EXTRACT(MONTH FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_month
              , EXTRACT(WEEK FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_week
              , EXTRACT(DAY FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_day
              , EXTRACT(DAYOFWEEK FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM bookings_source_src_10015.paid_at) AS booking__paid_at__extract_dayofyear
              , bookings_source_src_10015.listing_id AS listing
              , bookings_source_src_10015.guest_id AS guest
              , bookings_source_src_10015.host_id AS host
              , bookings_source_src_10015.guest_id AS user
              , bookings_source_src_10015.listing_id AS booking__listing
              , bookings_source_src_10015.guest_id AS booking__guest
              , bookings_source_src_10015.host_id AS booking__host
              , bookings_source_src_10015.guest_id AS booking__user
            FROM ***************************.fct_bookings bookings_source_src_10015
          ) subq_0
        ) subq_1
      ) subq_2
      LEFT OUTER JOIN (
        -- Pass Only Elements:
        --   ['user__home_state_latest', 'window_start__day', 'window_end__day', 'listing']
        SELECT
          subq_6.window_start__extract_year
          , subq_6.window_end__extract_year
          , subq_6.listing
          , subq_6.user__home_state_latest
        FROM (
          -- Join Standard Outputs
          SELECT
            subq_3.window_start__day AS window_start__day
            , subq_3.window_start__week AS window_start__week
            , subq_3.window_start__month AS window_start__month
            , subq_3.window_start__quarter AS window_start__quarter
            , subq_3.window_start__year AS window_start__year
            , subq_3.window_start__extract_year AS window_start__extract_year
            , subq_3.window_start__extract_quarter AS window_start__extract_quarter
            , subq_3.window_start__extract_month AS window_start__extract_month
            , subq_3.window_start__extract_week AS window_start__extract_week
            , subq_3.window_start__extract_day AS window_start__extract_day
            , subq_3.window_start__extract_dayofweek AS window_start__extract_dayofweek
            , subq_3.window_start__extract_dayofyear AS window_start__extract_dayofyear
            , subq_3.window_end__day AS window_end__day
            , subq_3.window_end__week AS window_end__week
            , subq_3.window_end__month AS window_end__month
            , subq_3.window_end__quarter AS window_end__quarter
            , subq_3.window_end__year AS window_end__year
            , subq_3.window_end__extract_year AS window_end__extract_year
            , subq_3.window_end__extract_quarter AS window_end__extract_quarter
            , subq_3.window_end__extract_month AS window_end__extract_month
            , subq_3.window_end__extract_week AS window_end__extract_week
            , subq_3.window_end__extract_day AS window_end__extract_day
            , subq_3.window_end__extract_dayofweek AS window_end__extract_dayofweek
            , subq_3.window_end__extract_dayofyear AS window_end__extract_dayofyear
            , subq_3.listing__window_start__day AS listing__window_start__day
            , subq_3.listing__window_start__week AS listing__window_start__week
            , subq_3.listing__window_start__month AS listing__window_start__month
            , subq_3.listing__window_start__quarter AS listing__window_start__quarter
            , subq_3.listing__window_start__year AS listing__window_start__year
            , subq_3.listing__window_start__extract_year AS listing__window_start__extract_year
            , subq_3.listing__window_start__extract_quarter AS listing__window_start__extract_quarter
            , subq_3.listing__window_start__extract_month AS listing__window_start__extract_month
            , subq_3.listing__window_start__extract_week AS listing__window_start__extract_week
            , subq_3.listing__window_start__extract_day AS listing__window_start__extract_day
            , subq_3.listing__window_start__extract_dayofweek AS listing__window_start__extract_dayofweek
            , subq_3.listing__window_start__extract_dayofyear AS listing__window_start__extract_dayofyear
            , subq_3.listing__window_end__day AS listing__window_end__day
            , subq_3.listing__window_end__week AS listing__window_end__week
            , subq_3.listing__window_end__month AS listing__window_end__month
            , subq_3.listing__window_end__quarter AS listing__window_end__quarter
            , subq_3.listing__window_end__year AS listing__window_end__year
            , subq_3.listing__window_end__extract_year AS listing__window_end__extract_year
            , subq_3.listing__window_end__extract_quarter AS listing__window_end__extract_quarter
            , subq_3.listing__window_end__extract_month AS listing__window_end__extract_month
            , subq_3.listing__window_end__extract_week AS listing__window_end__extract_week
            , subq_3.listing__window_end__extract_day AS listing__window_end__extract_day
            , subq_3.listing__window_end__extract_dayofweek AS listing__window_end__extract_dayofweek
            , subq_3.listing__window_end__extract_dayofyear AS listing__window_end__extract_dayofyear
            , subq_5.ds__day AS user__ds__day
            , subq_5.ds__week AS user__ds__week
            , subq_5.ds__month AS user__ds__month
            , subq_5.ds__quarter AS user__ds__quarter
            , subq_5.ds__year AS user__ds__year
            , subq_5.ds__extract_year AS user__ds__extract_year
            , subq_5.ds__extract_quarter AS user__ds__extract_quarter
            , subq_5.ds__extract_month AS user__ds__extract_month
            , subq_5.ds__extract_week AS user__ds__extract_week
            , subq_5.ds__extract_day AS user__ds__extract_day
            , subq_5.ds__extract_dayofweek AS user__ds__extract_dayofweek
            , subq_5.ds__extract_dayofyear AS user__ds__extract_dayofyear
            , subq_3.listing AS listing
            , subq_3.user AS user
            , subq_3.listing__user AS listing__user
            , subq_3.country AS country
            , subq_3.is_lux AS is_lux
            , subq_3.capacity AS capacity
            , subq_3.listing__country AS listing__country
            , subq_3.listing__is_lux AS listing__is_lux
            , subq_3.listing__capacity AS listing__capacity
            , subq_5.home_state_latest AS user__home_state_latest
          FROM (
            -- Read Elements From Semantic Model 'listings'
            SELECT
              listings_src_10017.active_from AS window_start__day
              , DATE_TRUNC('week', listings_src_10017.active_from) AS window_start__week
              , DATE_TRUNC('month', listings_src_10017.active_from) AS window_start__month
              , DATE_TRUNC('quarter', listings_src_10017.active_from) AS window_start__quarter
              , DATE_TRUNC('year', listings_src_10017.active_from) AS window_start__year
              , EXTRACT(YEAR FROM listings_src_10017.active_from) AS window_start__extract_year
              , EXTRACT(QUARTER FROM listings_src_10017.active_from) AS window_start__extract_quarter
              , EXTRACT(MONTH FROM listings_src_10017.active_from) AS window_start__extract_month
              , EXTRACT(WEEK FROM listings_src_10017.active_from) AS window_start__extract_week
              , EXTRACT(DAY FROM listings_src_10017.active_from) AS window_start__extract_day
              , EXTRACT(DAYOFWEEK FROM listings_src_10017.active_from) AS window_start__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM listings_src_10017.active_from) AS window_start__extract_dayofyear
              , listings_src_10017.active_to AS window_end__day
              , DATE_TRUNC('week', listings_src_10017.active_to) AS window_end__week
              , DATE_TRUNC('month', listings_src_10017.active_to) AS window_end__month
              , DATE_TRUNC('quarter', listings_src_10017.active_to) AS window_end__quarter
              , DATE_TRUNC('year', listings_src_10017.active_to) AS window_end__year
              , EXTRACT(YEAR FROM listings_src_10017.active_to) AS window_end__extract_year
              , EXTRACT(QUARTER FROM listings_src_10017.active_to) AS window_end__extract_quarter
              , EXTRACT(MONTH FROM listings_src_10017.active_to) AS window_end__extract_month
              , EXTRACT(WEEK FROM listings_src_10017.active_to) AS window_end__extract_week
              , EXTRACT(DAY FROM listings_src_10017.active_to) AS window_end__extract_day
              , EXTRACT(DAYOFWEEK FROM listings_src_10017.active_to) AS window_end__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM listings_src_10017.active_to) AS window_end__extract_dayofyear
              , listings_src_10017.country
              , listings_src_10017.is_lux
              , listings_src_10017.capacity
              , listings_src_10017.active_from AS listing__window_start__day
              , DATE_TRUNC('week', listings_src_10017.active_from) AS listing__window_start__week
              , DATE_TRUNC('month', listings_src_10017.active_from) AS listing__window_start__month
              , DATE_TRUNC('quarter', listings_src_10017.active_from) AS listing__window_start__quarter
              , DATE_TRUNC('year', listings_src_10017.active_from) AS listing__window_start__year
              , EXTRACT(YEAR FROM listings_src_10017.active_from) AS listing__window_start__extract_year
              , EXTRACT(QUARTER FROM listings_src_10017.active_from) AS listing__window_start__extract_quarter
              , EXTRACT(MONTH FROM listings_src_10017.active_from) AS listing__window_start__extract_month
              , EXTRACT(WEEK FROM listings_src_10017.active_from) AS listing__window_start__extract_week
              , EXTRACT(DAY FROM listings_src_10017.active_from) AS listing__window_start__extract_day
              , EXTRACT(DAYOFWEEK FROM listings_src_10017.active_from) AS listing__window_start__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM listings_src_10017.active_from) AS listing__window_start__extract_dayofyear
              , listings_src_10017.active_to AS listing__window_end__day
              , DATE_TRUNC('week', listings_src_10017.active_to) AS listing__window_end__week
              , DATE_TRUNC('month', listings_src_10017.active_to) AS listing__window_end__month
              , DATE_TRUNC('quarter', listings_src_10017.active_to) AS listing__window_end__quarter
              , DATE_TRUNC('year', listings_src_10017.active_to) AS listing__window_end__year
              , EXTRACT(YEAR FROM listings_src_10017.active_to) AS listing__window_end__extract_year
              , EXTRACT(QUARTER FROM listings_src_10017.active_to) AS listing__window_end__extract_quarter
              , EXTRACT(MONTH FROM listings_src_10017.active_to) AS listing__window_end__extract_month
              , EXTRACT(WEEK FROM listings_src_10017.active_to) AS listing__window_end__extract_week
              , EXTRACT(DAY FROM listings_src_10017.active_to) AS listing__window_end__extract_day
              , EXTRACT(DAYOFWEEK FROM listings_src_10017.active_to) AS listing__window_end__extract_dayofweek
              , EXTRACT(DAYOFYEAR FROM listings_src_10017.active_to) AS listing__window_end__extract_dayofyear
              , listings_src_10017.country AS listing__country
              , listings_src_10017.is_lux AS listing__is_lux
              , listings_src_10017.capacity AS listing__capacity
              , listings_src_10017.listing_id AS listing
              , listings_src_10017.user_id AS user
              , listings_src_10017.user_id AS listing__user
            FROM ***************************.dim_listings listings_src_10017
          ) subq_3
          LEFT OUTER JOIN (
            -- Pass Only Elements:
            --   ['home_state_latest',
            --    'user__home_state_latest',
            --    'ds__day',
            --    'ds__week',
            --    'ds__month',
            --    'ds__quarter',
            --    'ds__year',
            --    'ds__day',
            --    'ds__day',
            --    'ds__day',
            --    'ds__day',
            --    'ds__day',
            --    'ds__day',
            --    'ds__day',
            --    'user__ds__day',
            --    'user__ds__week',
            --    'user__ds__month',
            --    'user__ds__quarter',
            --    'user__ds__year',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user__ds__day',
            --    'user']
            SELECT
              subq_4.ds__day
              , subq_4.ds__week
              , subq_4.ds__month
              , subq_4.ds__quarter
              , subq_4.ds__year
              , subq_4.ds__extract_year
              , subq_4.ds__extract_quarter
              , subq_4.ds__extract_month
              , subq_4.ds__extract_week
              , subq_4.ds__extract_day
              , subq_4.ds__extract_dayofweek
              , subq_4.ds__extract_dayofyear
              , subq_4.user__ds__day
              , subq_4.user__ds__week
              , subq_4.user__ds__month
              , subq_4.user__ds__quarter
              , subq_4.user__ds__year
              , subq_4.user__ds__extract_year
              , subq_4.user__ds__extract_quarter
              , subq_4.user__ds__extract_month
              , subq_4.user__ds__extract_week
              , subq_4.user__ds__extract_day
              , subq_4.user__ds__extract_dayofweek
              , subq_4.user__ds__extract_dayofyear
              , subq_4.user
              , subq_4.home_state_latest
              , subq_4.user__home_state_latest
            FROM (
              -- Read Elements From Semantic Model 'users_latest'
              SELECT
                users_latest_src_10021.ds AS ds__day
                , DATE_TRUNC('week', users_latest_src_10021.ds) AS ds__week
                , DATE_TRUNC('month', users_latest_src_10021.ds) AS ds__month
                , DATE_TRUNC('quarter', users_latest_src_10021.ds) AS ds__quarter
                , DATE_TRUNC('year', users_latest_src_10021.ds) AS ds__year
                , EXTRACT(YEAR FROM users_latest_src_10021.ds) AS ds__extract_year
                , EXTRACT(QUARTER FROM users_latest_src_10021.ds) AS ds__extract_quarter
                , EXTRACT(MONTH FROM users_latest_src_10021.ds) AS ds__extract_month
                , EXTRACT(WEEK FROM users_latest_src_10021.ds) AS ds__extract_week
                , EXTRACT(DAY FROM users_latest_src_10021.ds) AS ds__extract_day
                , EXTRACT(DAYOFWEEK FROM users_latest_src_10021.ds) AS ds__extract_dayofweek
                , EXTRACT(DAYOFYEAR FROM users_latest_src_10021.ds) AS ds__extract_dayofyear
                , users_latest_src_10021.home_state_latest
                , users_latest_src_10021.ds AS user__ds__day
                , DATE_TRUNC('week', users_latest_src_10021.ds) AS user__ds__week
                , DATE_TRUNC('month', users_latest_src_10021.ds) AS user__ds__month
                , DATE_TRUNC('quarter', users_latest_src_10021.ds) AS user__ds__quarter
                , DATE_TRUNC('year', users_latest_src_10021.ds) AS user__ds__year
                , EXTRACT(YEAR FROM users_latest_src_10021.ds) AS user__ds__extract_year
                , EXTRACT(QUARTER FROM users_latest_src_10021.ds) AS user__ds__extract_quarter
                , EXTRACT(MONTH FROM users_latest_src_10021.ds) AS user__ds__extract_month
                , EXTRACT(WEEK FROM users_latest_src_10021.ds) AS user__ds__extract_week
                , EXTRACT(DAY FROM users_latest_src_10021.ds) AS user__ds__extract_day
                , EXTRACT(DAYOFWEEK FROM users_latest_src_10021.ds) AS user__ds__extract_dayofweek
                , EXTRACT(DAYOFYEAR FROM users_latest_src_10021.ds) AS user__ds__extract_dayofyear
                , users_latest_src_10021.home_state_latest AS user__home_state_latest
                , users_latest_src_10021.user_id AS user
              FROM ***************************.dim_users_latest users_latest_src_10021
            ) subq_4
          ) subq_5
          ON
            subq_3.user = subq_5.user
        ) subq_6
      ) subq_7
      ON
        (
          subq_2.listing = subq_7.listing
        ) AND (
          (
            subq_2.metric_time__day >= subq_7.window_start__extract_year
          ) AND (
            (
              subq_2.metric_time__day < subq_7.window_end__extract_year
            ) OR (
              subq_7.window_end__extract_year IS NULL
            )
          )
        )
    ) subq_8
  ) subq_9
  GROUP BY
    subq_9.metric_time__day
    , subq_9.listing__user__home_state_latest
) subq_10
