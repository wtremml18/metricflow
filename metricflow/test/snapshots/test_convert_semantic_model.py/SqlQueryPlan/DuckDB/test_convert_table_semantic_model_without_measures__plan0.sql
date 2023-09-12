-- Read Elements From Semantic Model 'users_latest'
SELECT
  users_latest_src_10008.ds AS ds__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS ds__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS ds__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS ds__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS ds__year
  , EXTRACT(YEAR FROM users_latest_src_10008.ds) AS ds__extract_year
  , EXTRACT(QUARTER FROM users_latest_src_10008.ds) AS ds__extract_quarter
  , EXTRACT(MONTH FROM users_latest_src_10008.ds) AS ds__extract_month
  , EXTRACT(WEEK FROM users_latest_src_10008.ds) AS ds__extract_week
  , EXTRACT(DAY FROM users_latest_src_10008.ds) AS ds__extract_day
  , EXTRACT(DAYOFWEEK FROM users_latest_src_10008.ds) AS ds__extract_dow
  , EXTRACT(DAYOFYEAR FROM users_latest_src_10008.ds) AS ds__extract_doy
  , users_latest_src_10008.home_state_latest
  , users_latest_src_10008.ds AS user__ds__day
  , DATE_TRUNC('week', users_latest_src_10008.ds) AS user__ds__week
  , DATE_TRUNC('month', users_latest_src_10008.ds) AS user__ds__month
  , DATE_TRUNC('quarter', users_latest_src_10008.ds) AS user__ds__quarter
  , DATE_TRUNC('year', users_latest_src_10008.ds) AS user__ds__year
  , EXTRACT(YEAR FROM users_latest_src_10008.ds) AS user__ds__extract_year
  , EXTRACT(QUARTER FROM users_latest_src_10008.ds) AS user__ds__extract_quarter
  , EXTRACT(MONTH FROM users_latest_src_10008.ds) AS user__ds__extract_month
  , EXTRACT(WEEK FROM users_latest_src_10008.ds) AS user__ds__extract_week
  , EXTRACT(DAY FROM users_latest_src_10008.ds) AS user__ds__extract_day
  , EXTRACT(DAYOFWEEK FROM users_latest_src_10008.ds) AS user__ds__extract_dow
  , EXTRACT(DAYOFYEAR FROM users_latest_src_10008.ds) AS user__ds__extract_doy
  , users_latest_src_10008.home_state_latest AS user__home_state_latest
  , users_latest_src_10008.user_id AS user
FROM ***************************.dim_users_latest users_latest_src_10008
