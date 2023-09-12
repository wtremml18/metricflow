-- Read Elements From Semantic Model 'revenue'
SELECT
  revenue_src_10006.revenue AS txn_revenue
  , revenue_src_10006.created_at AS ds__day
  , DATE_TRUNC('week', revenue_src_10006.created_at) AS ds__week
  , DATE_TRUNC('month', revenue_src_10006.created_at) AS ds__month
  , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS ds__quarter
  , DATE_TRUNC('year', revenue_src_10006.created_at) AS ds__year
  , EXTRACT(YEAR FROM revenue_src_10006.created_at) AS ds__extract_year
  , EXTRACT(QUARTER FROM revenue_src_10006.created_at) AS ds__extract_quarter
  , EXTRACT(MONTH FROM revenue_src_10006.created_at) AS ds__extract_month
  , EXTRACT(WEEK FROM revenue_src_10006.created_at) AS ds__extract_week
  , EXTRACT(DAY FROM revenue_src_10006.created_at) AS ds__extract_day
  , EXTRACT(DAYOFWEEK FROM revenue_src_10006.created_at) AS ds__extract_dow
  , EXTRACT(DAYOFYEAR FROM revenue_src_10006.created_at) AS ds__extract_doy
  , revenue_src_10006.created_at AS company__ds__day
  , DATE_TRUNC('week', revenue_src_10006.created_at) AS company__ds__week
  , DATE_TRUNC('month', revenue_src_10006.created_at) AS company__ds__month
  , DATE_TRUNC('quarter', revenue_src_10006.created_at) AS company__ds__quarter
  , DATE_TRUNC('year', revenue_src_10006.created_at) AS company__ds__year
  , EXTRACT(YEAR FROM revenue_src_10006.created_at) AS company__ds__extract_year
  , EXTRACT(QUARTER FROM revenue_src_10006.created_at) AS company__ds__extract_quarter
  , EXTRACT(MONTH FROM revenue_src_10006.created_at) AS company__ds__extract_month
  , EXTRACT(WEEK FROM revenue_src_10006.created_at) AS company__ds__extract_week
  , EXTRACT(DAY FROM revenue_src_10006.created_at) AS company__ds__extract_day
  , EXTRACT(DAYOFWEEK FROM revenue_src_10006.created_at) AS company__ds__extract_dow
  , EXTRACT(DAYOFYEAR FROM revenue_src_10006.created_at) AS company__ds__extract_doy
  , revenue_src_10006.user_id AS user
  , revenue_src_10006.user_id AS company__user
FROM ***************************.fct_revenue revenue_src_10006
