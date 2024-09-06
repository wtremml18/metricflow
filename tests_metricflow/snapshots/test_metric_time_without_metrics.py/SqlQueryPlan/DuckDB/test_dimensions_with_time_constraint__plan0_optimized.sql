-- Join Standard Outputs
-- Constrain Time Range to [2020-01-01T00:00:00, 2020-01-03T00:00:00]
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest', 'metric_time__day']
SELECT
  time_spine_src_28006.martian_day AS metric_time__day
  , listings_latest_src_28000.is_lux AS listing__is_lux_latest
  , users_latest_src_28000.home_state_latest AS user__home_state_latest
FROM ***************************.dim_listings_latest listings_latest_src_28000
CROSS JOIN
  ***************************.mf_time_spine time_spine_src_28006
FULL OUTER JOIN
  ***************************.dim_users_latest users_latest_src_28000
ON
  listings_latest_src_28000.user_id = users_latest_src_28000.user_id
WHERE time_spine_src_28006.martian_day BETWEEN '2020-01-01' AND '2020-01-03'
GROUP BY
  time_spine_src_28006.martian_day
  , listings_latest_src_28000.is_lux
  , users_latest_src_28000.home_state_latest
