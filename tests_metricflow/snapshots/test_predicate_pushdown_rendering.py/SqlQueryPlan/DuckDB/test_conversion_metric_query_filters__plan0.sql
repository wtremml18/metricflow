-- Compute Metrics via Expressions
SELECT
  subq_18.metric_time__day
  , subq_18.user__home_state_latest
  , CAST(subq_18.buys AS DOUBLE) / CAST(NULLIF(subq_18.visits, 0) AS DOUBLE) AS visit_buy_conversion_rate_7days
FROM (
  -- Combine Aggregated Outputs
  SELECT
    COALESCE(subq_5.metric_time__day, subq_17.metric_time__day) AS metric_time__day
    , COALESCE(subq_5.user__home_state_latest, subq_17.user__home_state_latest) AS user__home_state_latest
    , MAX(subq_5.visits) AS visits
    , MAX(subq_17.buys) AS buys
  FROM (
    -- Aggregate Measures
    SELECT
      subq_4.metric_time__day
      , subq_4.user__home_state_latest
      , SUM(subq_4.visits) AS visits
    FROM (
      -- Constrain Output with WHERE
      -- Pass Only Elements: ['visits', 'user__home_state_latest', 'metric_time__day']
      SELECT
        subq_3.metric_time__day
        , subq_3.user__home_state_latest
        , subq_3.visits
      FROM (
        -- Join Standard Outputs
        -- Pass Only Elements: ['visits', 'user__home_state_latest', 'visit__referrer_id', 'metric_time__day']
        SELECT
          subq_1.metric_time__day AS metric_time__day
          , subq_1.visit__referrer_id AS visit__referrer_id
          , subq_1.visits AS visits
        FROM (
          -- Metric Time Dimension 'ds'
          -- Pass Only Elements: ['visits', 'visit__referrer_id', 'metric_time__day', 'user']
          SELECT
            subq_0.ds__day AS metric_time__day
            , subq_0.user
            , subq_0.visit__referrer_id
            , subq_0.visits
          FROM (
            -- Read Elements From Semantic Model 'visits_source'
            SELECT
              1 AS visits
              , visits_source_src_28000.user_id AS visitors
              , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
              , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
              , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
              , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
              , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
              , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
              , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
              , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
              , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
              , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
              , visits_source_src_28000.referrer_id
              , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
              , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
              , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
              , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
              , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
              , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
              , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
              , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
              , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
              , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
              , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
              , visits_source_src_28000.referrer_id AS visit__referrer_id
              , visits_source_src_28000.user_id AS user
              , visits_source_src_28000.session_id AS session
              , visits_source_src_28000.user_id AS visit__user
              , visits_source_src_28000.session_id AS visit__session
            FROM ***************************.fct_visits visits_source_src_28000
          ) subq_0
        ) subq_1
        LEFT OUTER JOIN (
          -- Read From SemanticModelDataSet('users_latest')
          -- Pass Only Elements: ['home_state_latest', 'user']
          SELECT
            users_latest_src_28000.home_state_latest
            , users_latest_src_28000.user_id AS user
          FROM ***************************.dim_users_latest users_latest_src_28000
        ) subq_2
        ON
          subq_1.user = subq_2.user
      ) subq_3
      WHERE visit__referrer_id = '123456'
    ) subq_4
    GROUP BY
      subq_4.metric_time__day
      , subq_4.user__home_state_latest
  ) subq_5
  FULL OUTER JOIN (
    -- Aggregate Measures
    SELECT
      subq_16.metric_time__day
      , subq_16.user__home_state_latest
      , SUM(subq_16.buys) AS buys
    FROM (
      -- Join Standard Outputs
      -- Pass Only Elements: ['buys', 'user__home_state_latest', 'metric_time__day']
      SELECT
        subq_14.metric_time__day AS metric_time__day
        , subq_14.buys AS buys
      FROM (
        -- Find conversions for user within the range of 7 day
        -- Pass Only Elements: ['buys', 'visit__referrer_id', 'metric_time__day', 'user']
        SELECT
          subq_13.metric_time__day
          , subq_13.user
          , subq_13.visit__referrer_id
          , subq_13.buys
        FROM (
          -- Dedupe the fanout with mf_internal_uuid in the conversion data set
          SELECT DISTINCT
            FIRST_VALUE(subq_9.visits) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visits
            , FIRST_VALUE(subq_9.visit__referrer_id) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS visit__referrer_id
            , FIRST_VALUE(subq_9.user__home_state_latest) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user__home_state_latest
            , FIRST_VALUE(subq_9.ds__day) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS ds__day
            , FIRST_VALUE(subq_9.metric_time__day) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS metric_time__day
            , FIRST_VALUE(subq_9.user) OVER (
              PARTITION BY
                subq_12.user
                , subq_12.ds__day
                , subq_12.mf_internal_uuid
              ORDER BY subq_9.ds__day DESC
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS user
            , subq_12.mf_internal_uuid AS mf_internal_uuid
            , subq_12.buys AS buys
          FROM (
            -- Join Standard Outputs
            -- Pass Only Elements: ['visits', 'visit__referrer_id', 'user__home_state_latest', 'ds__day', 'metric_time__day', 'user']
            SELECT
              subq_7.ds__day AS ds__day
              , subq_7.metric_time__day AS metric_time__day
              , subq_7.user AS user
              , subq_7.visit__referrer_id AS visit__referrer_id
              , subq_7.visits AS visits
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_6.ds__day
                , subq_6.ds__week
                , subq_6.ds__month
                , subq_6.ds__quarter
                , subq_6.ds__year
                , subq_6.ds__extract_year
                , subq_6.ds__extract_quarter
                , subq_6.ds__extract_month
                , subq_6.ds__extract_day
                , subq_6.ds__extract_dow
                , subq_6.ds__extract_doy
                , subq_6.visit__ds__day
                , subq_6.visit__ds__week
                , subq_6.visit__ds__month
                , subq_6.visit__ds__quarter
                , subq_6.visit__ds__year
                , subq_6.visit__ds__extract_year
                , subq_6.visit__ds__extract_quarter
                , subq_6.visit__ds__extract_month
                , subq_6.visit__ds__extract_day
                , subq_6.visit__ds__extract_dow
                , subq_6.visit__ds__extract_doy
                , subq_6.ds__day AS metric_time__day
                , subq_6.ds__week AS metric_time__week
                , subq_6.ds__month AS metric_time__month
                , subq_6.ds__quarter AS metric_time__quarter
                , subq_6.ds__year AS metric_time__year
                , subq_6.ds__extract_year AS metric_time__extract_year
                , subq_6.ds__extract_quarter AS metric_time__extract_quarter
                , subq_6.ds__extract_month AS metric_time__extract_month
                , subq_6.ds__extract_day AS metric_time__extract_day
                , subq_6.ds__extract_dow AS metric_time__extract_dow
                , subq_6.ds__extract_doy AS metric_time__extract_doy
                , subq_6.user
                , subq_6.session
                , subq_6.visit__user
                , subq_6.visit__session
                , subq_6.referrer_id
                , subq_6.visit__referrer_id
                , subq_6.visits
                , subq_6.visitors
              FROM (
                -- Read Elements From Semantic Model 'visits_source'
                SELECT
                  1 AS visits
                  , visits_source_src_28000.user_id AS visitors
                  , DATE_TRUNC('day', visits_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', visits_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', visits_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', visits_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM visits_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM visits_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM visits_source_src_28000.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_28000.ds) AS ds__extract_doy
                  , visits_source_src_28000.referrer_id
                  , DATE_TRUNC('day', visits_source_src_28000.ds) AS visit__ds__day
                  , DATE_TRUNC('week', visits_source_src_28000.ds) AS visit__ds__week
                  , DATE_TRUNC('month', visits_source_src_28000.ds) AS visit__ds__month
                  , DATE_TRUNC('quarter', visits_source_src_28000.ds) AS visit__ds__quarter
                  , DATE_TRUNC('year', visits_source_src_28000.ds) AS visit__ds__year
                  , EXTRACT(year FROM visits_source_src_28000.ds) AS visit__ds__extract_year
                  , EXTRACT(quarter FROM visits_source_src_28000.ds) AS visit__ds__extract_quarter
                  , EXTRACT(month FROM visits_source_src_28000.ds) AS visit__ds__extract_month
                  , EXTRACT(day FROM visits_source_src_28000.ds) AS visit__ds__extract_day
                  , EXTRACT(isodow FROM visits_source_src_28000.ds) AS visit__ds__extract_dow
                  , EXTRACT(doy FROM visits_source_src_28000.ds) AS visit__ds__extract_doy
                  , visits_source_src_28000.referrer_id AS visit__referrer_id
                  , visits_source_src_28000.user_id AS user
                  , visits_source_src_28000.session_id AS session
                  , visits_source_src_28000.user_id AS visit__user
                  , visits_source_src_28000.session_id AS visit__session
                FROM ***************************.fct_visits visits_source_src_28000
              ) subq_6
            ) subq_7
            LEFT OUTER JOIN (
              -- Read From SemanticModelDataSet('users_latest')
              -- Pass Only Elements: ['home_state_latest', 'user']
              SELECT
                users_latest_src_28000.home_state_latest
                , users_latest_src_28000.user_id AS user
              FROM ***************************.dim_users_latest users_latest_src_28000
            ) subq_8
            ON
              subq_7.user = subq_8.user
          ) subq_9
          INNER JOIN (
            -- Add column with generated UUID
            SELECT
              subq_11.ds__day
              , subq_11.ds__week
              , subq_11.ds__month
              , subq_11.ds__quarter
              , subq_11.ds__year
              , subq_11.ds__extract_year
              , subq_11.ds__extract_quarter
              , subq_11.ds__extract_month
              , subq_11.ds__extract_day
              , subq_11.ds__extract_dow
              , subq_11.ds__extract_doy
              , subq_11.buy__ds__day
              , subq_11.buy__ds__week
              , subq_11.buy__ds__month
              , subq_11.buy__ds__quarter
              , subq_11.buy__ds__year
              , subq_11.buy__ds__extract_year
              , subq_11.buy__ds__extract_quarter
              , subq_11.buy__ds__extract_month
              , subq_11.buy__ds__extract_day
              , subq_11.buy__ds__extract_dow
              , subq_11.buy__ds__extract_doy
              , subq_11.metric_time__day
              , subq_11.metric_time__week
              , subq_11.metric_time__month
              , subq_11.metric_time__quarter
              , subq_11.metric_time__year
              , subq_11.metric_time__extract_year
              , subq_11.metric_time__extract_quarter
              , subq_11.metric_time__extract_month
              , subq_11.metric_time__extract_day
              , subq_11.metric_time__extract_dow
              , subq_11.metric_time__extract_doy
              , subq_11.user
              , subq_11.session_id
              , subq_11.buy__user
              , subq_11.buy__session_id
              , subq_11.buys
              , subq_11.buyers
              , GEN_RANDOM_UUID() AS mf_internal_uuid
            FROM (
              -- Metric Time Dimension 'ds'
              SELECT
                subq_10.ds__day
                , subq_10.ds__week
                , subq_10.ds__month
                , subq_10.ds__quarter
                , subq_10.ds__year
                , subq_10.ds__extract_year
                , subq_10.ds__extract_quarter
                , subq_10.ds__extract_month
                , subq_10.ds__extract_day
                , subq_10.ds__extract_dow
                , subq_10.ds__extract_doy
                , subq_10.buy__ds__day
                , subq_10.buy__ds__week
                , subq_10.buy__ds__month
                , subq_10.buy__ds__quarter
                , subq_10.buy__ds__year
                , subq_10.buy__ds__extract_year
                , subq_10.buy__ds__extract_quarter
                , subq_10.buy__ds__extract_month
                , subq_10.buy__ds__extract_day
                , subq_10.buy__ds__extract_dow
                , subq_10.buy__ds__extract_doy
                , subq_10.ds__day AS metric_time__day
                , subq_10.ds__week AS metric_time__week
                , subq_10.ds__month AS metric_time__month
                , subq_10.ds__quarter AS metric_time__quarter
                , subq_10.ds__year AS metric_time__year
                , subq_10.ds__extract_year AS metric_time__extract_year
                , subq_10.ds__extract_quarter AS metric_time__extract_quarter
                , subq_10.ds__extract_month AS metric_time__extract_month
                , subq_10.ds__extract_day AS metric_time__extract_day
                , subq_10.ds__extract_dow AS metric_time__extract_dow
                , subq_10.ds__extract_doy AS metric_time__extract_doy
                , subq_10.user
                , subq_10.session_id
                , subq_10.buy__user
                , subq_10.buy__session_id
                , subq_10.buys
                , subq_10.buyers
              FROM (
                -- Read Elements From Semantic Model 'buys_source'
                SELECT
                  1 AS buys
                  , buys_source_src_28000.user_id AS buyers
                  , DATE_TRUNC('day', buys_source_src_28000.ds) AS ds__day
                  , DATE_TRUNC('week', buys_source_src_28000.ds) AS ds__week
                  , DATE_TRUNC('month', buys_source_src_28000.ds) AS ds__month
                  , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS ds__quarter
                  , DATE_TRUNC('year', buys_source_src_28000.ds) AS ds__year
                  , EXTRACT(year FROM buys_source_src_28000.ds) AS ds__extract_year
                  , EXTRACT(quarter FROM buys_source_src_28000.ds) AS ds__extract_quarter
                  , EXTRACT(month FROM buys_source_src_28000.ds) AS ds__extract_month
                  , EXTRACT(day FROM buys_source_src_28000.ds) AS ds__extract_day
                  , EXTRACT(isodow FROM buys_source_src_28000.ds) AS ds__extract_dow
                  , EXTRACT(doy FROM buys_source_src_28000.ds) AS ds__extract_doy
                  , DATE_TRUNC('day', buys_source_src_28000.ds) AS buy__ds__day
                  , DATE_TRUNC('week', buys_source_src_28000.ds) AS buy__ds__week
                  , DATE_TRUNC('month', buys_source_src_28000.ds) AS buy__ds__month
                  , DATE_TRUNC('quarter', buys_source_src_28000.ds) AS buy__ds__quarter
                  , DATE_TRUNC('year', buys_source_src_28000.ds) AS buy__ds__year
                  , EXTRACT(year FROM buys_source_src_28000.ds) AS buy__ds__extract_year
                  , EXTRACT(quarter FROM buys_source_src_28000.ds) AS buy__ds__extract_quarter
                  , EXTRACT(month FROM buys_source_src_28000.ds) AS buy__ds__extract_month
                  , EXTRACT(day FROM buys_source_src_28000.ds) AS buy__ds__extract_day
                  , EXTRACT(isodow FROM buys_source_src_28000.ds) AS buy__ds__extract_dow
                  , EXTRACT(doy FROM buys_source_src_28000.ds) AS buy__ds__extract_doy
                  , buys_source_src_28000.user_id AS user
                  , buys_source_src_28000.session_id
                  , buys_source_src_28000.user_id AS buy__user
                  , buys_source_src_28000.session_id AS buy__session_id
                FROM ***************************.fct_buys buys_source_src_28000
              ) subq_10
            ) subq_11
          ) subq_12
          ON
            (
              subq_9.user = subq_12.user
            ) AND (
              (
                subq_9.ds__day <= subq_12.ds__day
              ) AND (
                subq_9.ds__day > subq_12.ds__day - INTERVAL 7 day
              )
            )
        ) subq_13
      ) subq_14
      LEFT OUTER JOIN (
        -- Read From SemanticModelDataSet('users_latest')
        -- Pass Only Elements: ['home_state_latest', 'user']
        SELECT
          users_latest_src_28000.home_state_latest
          , users_latest_src_28000.user_id AS user
        FROM ***************************.dim_users_latest users_latest_src_28000
      ) subq_15
      ON
        subq_14.user = subq_15.user
    ) subq_16
    GROUP BY
      subq_16.metric_time__day
      , subq_16.user__home_state_latest
  ) subq_17
  ON
    (
      subq_5.user__home_state_latest = subq_17.user__home_state_latest
    ) AND (
      subq_5.metric_time__day = subq_17.metric_time__day
    )
  GROUP BY
    COALESCE(subq_5.metric_time__day, subq_17.metric_time__day)
    , COALESCE(subq_5.user__home_state_latest, subq_17.user__home_state_latest)
) subq_18
