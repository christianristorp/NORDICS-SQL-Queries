SELECT country
, hellofresh_week
, recipe_index
, SUM(1s_wo_scm) as 1s_wo_scm
, SUM(ratings_wo_scm) as ratings_wo_scm
, SUM(recipe_scm_rating_count) AS recipe_scm_rating_count
, SUM(count_ratings) AS count_ratings
, SUM(count_1) AS count_1
, SUM(sum_ratings) AS sum_ratings
, SUM(recipe_scm_rating_values) AS sum_scm_ratings
FROM materialized_views.local_recipe_reporting_final
WHERE (country = 'DK'or country = 'SE' or country = 'NO') AND hellofresh_week >= '2021-W01'
GROUP BY 1,2,3
ORDER BY 2,3,1