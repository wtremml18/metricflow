-- Join Standard Outputs
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
SELECT
  DATE_TRUNC('day', time_spine_src_10018.ds) AS metric_time__day
  , listings_latest_src_10063.is_lux AS listing__is_lux_latest
  , users_latest_src_10067.home_state_latest AS user__home_state_latest
FROM ***************************.dim_listings_latest listings_latest_src_10063
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_10018
FULL OUTER JOIN
  ***************************.dim_users_latest users_latest_src_10067
ON
  listings_latest_src_10063.user_id = users_latest_src_10067.user_id
GROUP BY
  DATE_TRUNC('day', time_spine_src_10018.ds)
  , listings_latest_src_10063.is_lux
  , users_latest_src_10067.home_state_latest
