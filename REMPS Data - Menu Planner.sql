SELECT
uniquerecipecode as 'recipe_id'
, title
, subtitle
, author
, mainimageurl as 'image'
FROM materialized_views.int_scm_analytics_remps_recipe rr
LEFT JOIN materialized_views.remps_recipe_recipecost rc
on rr.id = rc.recipe_cost__recipe
WHERE rr.country = 'DKSE'