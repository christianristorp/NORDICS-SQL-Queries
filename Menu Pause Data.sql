SELECT country --t1
     , hellofresh_week
     , SUM(CASE WHEN status = 'user_paused' OR status = 'interval_paused' THEN 1 ELSE 0 END) total_paused
     , SUM(CASE
               WHEN status = 'user_paused'
                   OR status = 'active'
                   OR status = 'interval_paused' OR
                    status = 'unknown' THEN 1
               ELSE 0 END) total_active
FROM fact_tables.subscription_statuses
WHERE
      hellofresh_week >= '2021-W02'
  AND country IN ('DK','SE','NO')
GROUP BY country, hellofresh_week
ORDER BY country, hellofresh_week