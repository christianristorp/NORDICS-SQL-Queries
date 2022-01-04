select
       country,
       menu_week,
       menu_preference,
       reason,
       SUM(total_submissions)
from
     materialized_views.wac_pause_survey
where
      country in ('DK', 'SE', 'NO')
and   menu_week >= '2021-W01'
Group by
1, 2, 3, 4
order by
2, 1, 4