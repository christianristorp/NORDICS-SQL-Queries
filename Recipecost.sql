SELECT
       uniquerecipecode,
       mainrecipecode,
       version,
       cost_2p,
       cost_4p
FROM materialized_views.int_scm_analytics_remps_recipe rr
LEFT JOIN materialized_views.remps_recipe_recipecost rc
ON rr.id = rc.recipe_cost__recipe
LEFT JOIN materialized_views.remps_marketsetup_distributioncentres dc
ON rc.recipe_cost__distribution_centre = dc.id

WHERE rr.country = 'DKSE'
AND dc.bob_code ='SK'
AND status not in ('Inactive', 'Rejected')
ORDER BY 1
