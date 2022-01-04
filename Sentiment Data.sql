SELECT rc.country,
rc.hellofresh_week AS hf_week,
dd.hellofresh_month AS hf_month,
dd.hellofresh_quarter AS hf_quarter,
recipe_number,
sentiment,
attribute,
category,
COUNT(rating_id) AS nb_comments
FROM materialized_views.recipe_comments rc
LEFT JOIN (
SELECT DISTINCT hellofresh_week,
hellofresh_month_label AS hellofresh_month,
hellofresh_quarter_label AS hellofresh_quarter
FROM views_analysts.date_dimension_helper
)                                       dd
ON dd.hellofresh_week = rc.hellofresh_week
WHERE
rc.country in ('SE', 'NO', 'DK')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
Order by 2, 1, 5