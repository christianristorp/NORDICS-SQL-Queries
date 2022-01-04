select
       country,
       menu_week,
       menu_preference,
       reason,
       level_2,
       Level_3,
       SUM(total_submissions)
from
     materialized_views.wac_pause_survey
where
      country_group = 'Nordics'
and   menu_week >= '2021-W1'
and   reason = 'recipe'

Group by
1, 2, 3, 4, 5, 6
order by
2, 1, 4