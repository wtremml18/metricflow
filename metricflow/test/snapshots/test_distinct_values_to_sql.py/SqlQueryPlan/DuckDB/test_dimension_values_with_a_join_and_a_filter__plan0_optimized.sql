-- Constrain Output with WHERE
-- Pass Only Elements: ['user__home_state_latest', 'listing__is_lux_latest']
SELECT
  listing__is_lux_latest
  , user__home_state_latest
FROM (
  -- Join Standard Outputs
  SELECT
    listings_latest_src_10063.is_lux AS listing__is_lux_latest
    , users_latest_src_10067.home_state_latest AS user__home_state_latest
  FROM ***************************.dim_listings_latest listings_latest_src_10063
  FULL OUTER JOIN
    ***************************.dim_users_latest users_latest_src_10067
  ON
    listings_latest_src_10063.user_id = users_latest_src_10067.user_id
) subq_8
WHERE user__home_state_latest = 'us'
GROUP BY
  listing__is_lux_latest
  , user__home_state_latest
