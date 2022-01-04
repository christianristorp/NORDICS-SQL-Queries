SELECT * FROM (SELECT * FROM (WITH MAIN as (WITH remps as (SELECT * FROM (
SELECT yearweek as hf_week
,regioncode as country
,slotnumber as slot
,uniquerecipecode as recipe_code
, title as recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
, cuisine
, handsontime
, lastused as last_week
, nextused as next_week
, lastscore as previous_score
,rm.updated_at as last_update
,ROW_NUMBER() over (partition by slot_recipe,regioncode order by cost_2p) as currency_order
,cost_2p as cost2p
,cost_4p as cost4p
FROM materialized_views.int_scm_analytics_remps_menu rm
LEFT JOIN materialized_views.int_scm_analytics_remps_recipe rr
ON rm.slot_recipe = rr.id
LEFT JOIN materialized_views.remps_recipe_recipecost rc
on rr.id = rc.recipe__recipe_cost

WHERE regioncode in ('SE','DK','NO')
and title is not null) intermedaire
where currency_order =1),


overall_score as (With Tab_1 as (SELECT
rsdq.country
,hellofresh_week
, recipe_index
,SUM(shipped_count) as volume_recipes
,COALESCE(sum(sum_ratings),0) / COALESCE(sum(count_ratings),0)  as recipe_score
,(COALESCE(sum(sum_ratings),0)-COALESCE(sum(recipe_scm_rating_values),0) ) / (COALESCE(sum(count_ratings),0)-COALESCE(sum(recipe_scm_rating_count),0))  as recipe_score_wo_scm
,COALESCE(sum(count_ratings),0) as count_ratings
,COALESCE(sum(count_1),0)/COALESCE(sum(count_ratings),0) as share_1s
,COALESCE(sum(count_2),0)/COALESCE(sum(count_ratings),0) as share_2s
,COALESCE(sum(count_3),0)/COALESCE(sum(count_ratings),0) as share_3s
,COALESCE(sum(count_4),0)/COALESCE(sum(count_ratings),0) as share_4s
,COALESCE(sum(count_1),0) as count_1s
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('SE','DK','NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3),
Tab_2 as
(SELECT country as tab2_country
,hellofresh_week as hf_week
,SUM(shipped_count) as total_volume_recipes
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('SE','DK','NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2
)

SELECT Tab_1.country
,Tab_1.hellofresh_week
,recipe_index
,total_volume_recipes
,volume_recipes/total_volume_recipes as share_volume
,volume_recipes
,recipe_score
,recipe_score_wo_scm
,count_ratings
,share_1s
,share_2s
,share_3s
,share_4s
,count_1s
FROM Tab_1
left join Tab_2
on Tab_1.hellofresh_week = Tab_2.hf_week
and Tab_1.country =Tab_2.tab2_country),




Subscription_type as (
With master as (
With Tab_1 as (SELECT
rsdq.country
,hellofresh_week
, recipe_index
,CASE when subscription_preference_name='0' then 'chefschoice' else subscription_preference_name end as subscription
,SUM(shipped_count) as volume_recipes
,COALESCE(sum(sum_ratings),0) / COALESCE(sum(count_ratings),0)  as score
,COALESCE(sum(count_ratings),0) as count_ratings_preference
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('SE','DK','NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3,4),
Tab_2 as
(SELECT country as tab2_country
,hellofresh_week as hf_week
,CASE when subscription_preference_name='0' then 'chefschoice' else subscription_preference_name end as subscription_
,SUM(shipped_count) as total_volume_recipes
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('SE','DK','NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3)

SELECT Tab_1.country
,Tab_1.hellofresh_week
,recipe_index
,subscription
,count_ratings_preference
,total_volume_recipes
,score
,volume_recipes/total_volume_recipes as share_volume
,volume_recipes
FROM Tab_1
left join Tab_2
on Tab_1.hellofresh_week = Tab_2.hf_week
and Tab_1.country =Tab_2.tab2_country
and Tab_1.subscription=Tab_2.subscription_)


SELECT country,hellofresh_week,recipe_index
,COALESCE(AVG(cb_share),0) as cb_share
,COALESCE(AVG(fm_share),0) as fm_share
,COALESCE(AVG(vg_share),0) as vg_share
,COALESCE(AVG(r_share),0) as r_share
,COALESCE(AVG(cs_share),0) as cs_share
,COALESCE(AVG(cb_score),0) as cb_score
,COALESCE(AVG(fm_score),0) as fm_score
,COALESCE(AVG(vg_score),0) as vg_score
,COALESCE(AVG(r_score),0) as r_score
,COALESCE(AVG(cs_score),0) as cs_score
,COALESCE(AVG(cb_ratings),0) as cb_ratings
,COALESCE(AVG(fm_ratings),0) as fm_ratings
,COALESCE(AVG(vg_ratings),0) as vg_ratings
,COALESCE(AVG(r_ratings),0) as r_ratings
,COALESCE(AVG(cs_ratings),0) as cs_ratings
fROM (SELECT country,hellofresh_week,recipe_index
,case when subscription like 'chefschoice' then share_volume end as cb_share
,case when subscription ='family' then share_volume end as fm_share
,case when subscription ='veggie' then share_volume end as vg_share
,case when subscription ='quick' then share_volume end as r_share
,case when subscription ='lowcalorie' then share_volume end as cs_share

,case when subscription ='chefschoice' then score end as cb_score
,case when subscription ='family' then score end as fm_score
,case when subscription ='veggie' then score end as vg_score
,case when subscription ='quick' then score end as r_score
,case when subscription ='lowcalorie' then score end as cs_score

,case when subscription ='chefschoice' then count_ratings_preference end as cb_ratings
,case when subscription ='family' then count_ratings_preference end as fm_ratings
,case when subscription ='veggie' then count_ratings_preference end as vg_ratings
,case when subscription ='quick' then count_ratings_preference end as r_ratings
,case when subscription ='lowcalorie' then count_ratings_preference end as cs_ratings
From master) t
GROUP BY country,hellofresh_week,recipe_index )



SELECT remps.country
,hf_week
,remps.slot
,remps.recipe_code
,split_part(recipe_code,'-',1) as split_code
,cast(split_part(recipe_code,'-',2) as BIGINT)  as split_code_version
,concat(split_part(recipe_code,'-',1) ,'-',split_part(recipe_code,'-',2)) as recipe_code_without_country
, recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
,overall_score.recipe_score
,overall_score.recipe_score_wo_scm
,cost2p
,overall_score.share_1s
,overall_score.share_volume
,Subscription_type.cb_score
,Subscription_type.cb_ratings
,Subscription_type.cb_share
,Subscription_type.fm_score
,Subscription_type.fm_ratings
,Subscription_type.fm_share
,Subscription_type.vg_score
,Subscription_type.vg_ratings
,Subscription_type.vg_share
,Subscription_type.r_score
,Subscription_type.r_ratings
,Subscription_type.r_share
,Subscription_type.cs_score
,Subscription_type.cs_ratings
,Subscription_type.cs_share
,overall_score.count_ratings
,overall_score.count_1s
, cuisine
, handsontime
, last_week
, next_week
, previous_score as old_version_previous_score
,overall_score.share_2s
,overall_score.share_3s
,overall_score.share_4s
, remps.last_update
,cost4p
,overall_score.volume_recipes
,overall_score.total_volume_recipes
FROM remps
LEFT JOIN overall_score
on overall_score.hellofresh_week=remps.hf_week
and overall_score.country=remps.country
and overall_score.recipe_index=remps.slot
LEFT JOIN Subscription_type
on Subscription_type.hellofresh_week= overall_score.hellofresh_week
and    Subscription_type.country= overall_score.country
and    Subscription_type.recipe_index= overall_score.recipe_index)

SELECT main_1.country
,hf_week
,slot as recipe_index
,recipe_code
, recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
,recipe_score
,recipe_score_wo_scm
,cost2p
,share_1s
,share_volume
,cb_score
,cb_ratings
,cb_share
,fm_score
,fm_ratings
,fm_share
,vg_score
,vg_ratings
,vg_share
,r_score
,r_ratings
,r_share
,cs_score
,cs_ratings
,cs_share
,count_ratings
,count_1s
, cuisine
, handsontime
, previous_table.last_week
, next_week
, previous_table.previous_score
,share_2s
,share_3s
,share_4s
, last_update
,cost4p
,volume_recipes
,total_volume_recipes

FROM MAIN as main_1
LEFT JOIN (SELECT  country
,hf_week as last_week
,split_code
,split_code_version
,recipe_code_without_country
,concat(split_code,'-',CAST(split_code_version+1 AS STRING)) as new_recipe_code
,recipe_score as previous_score FROM MAIN) as previous_table
ON main_1.country = previous_table.country
AND main_1.recipe_code_without_country = previous_table.new_recipe_code) tab_1

UNION
SELECT * FROM (WITH MAIN as (WITH remps as (SELECT * FROM (
SELECT yearweek as hf_week
,regioncode as country
,slotnumber as slot
,uniquerecipecode as recipe_code
, title as recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
, cuisine
, handsontime
, lastused as last_week
, nextused as next_week
, lastscore as previous_score
,rm.updated_at as last_update
,ROW_NUMBER() over (partition by slot_recipe,regioncode order by cost_2p) as currency_order
,cost_2p as cost2p
,cost_4p as cost4p
FROM materialized_views.int_scm_analytics_remps_menu rm
LEFT JOIN materialized_views.int_scm_analytics_remps_recipe rr
ON rm.slot_recipe = rr.id
LEFT JOIN materialized_views.remps_recipe_recipecost rc
on rr.id = rc.recipe__recipe_cost

WHERE regioncode in ('NO')
and title is not null) intermedaire
where currency_order =1),


overall_score as (With Tab_1 as (SELECT
rsdq.country
,hellofresh_week
, recipe_index
,SUM(shipped_count) as volume_recipes
,COALESCE(sum(sum_ratings),0) / COALESCE(sum(count_ratings),0)  as recipe_score
,(COALESCE(sum(sum_ratings),0)-COALESCE(sum(recipe_scm_rating_values),0) ) / (COALESCE(sum(count_ratings),0)-COALESCE(sum(recipe_scm_rating_count),0))  as recipe_score_wo_scm
,COALESCE(sum(count_ratings),0) as count_ratings
,COALESCE(sum(count_1),0)/COALESCE(sum(count_ratings),0) as share_1s
,COALESCE(sum(count_2),0)/COALESCE(sum(count_ratings),0) as share_2s
,COALESCE(sum(count_3),0)/COALESCE(sum(count_ratings),0) as share_3s
,COALESCE(sum(count_4),0)/COALESCE(sum(count_ratings),0) as share_4s
,COALESCE(sum(count_1),0) as count_1s

FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3),
Tab_2 as
(SELECT country as tab2_country
,hellofresh_week as hf_week
,SUM(shipped_count) as total_volume_recipes

FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2
)

SELECT Tab_1.country
,Tab_1.hellofresh_week
,recipe_index
,total_volume_recipes
,volume_recipes/total_volume_recipes as share_volume
,volume_recipes
,recipe_score
,recipe_score_wo_scm
,count_ratings
,share_1s
,share_2s
,share_3s
,share_4s
,count_1s
FROM Tab_1
left join Tab_2
on Tab_1.hellofresh_week = Tab_2.hf_week
and Tab_1.country =Tab_2.tab2_country),




Subscription_type as (
With master as (
With Tab_1 as (SELECT
rsdq.country
,hellofresh_week
, recipe_index
,CASE when subscription_preference_name='0' then 'chefschoice' else subscription_preference_name end as subscription
,SUM(shipped_count) as volume_recipes
,COALESCE(sum(sum_ratings),0) / COALESCE(sum(count_ratings),0)  as score
,COALESCE(sum(count_ratings),0) as count_ratings_preference
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3,4),
Tab_2 as
(SELECT country as tab2_country
,hellofresh_week as hf_week
,CASE when subscription_preference_name='0' then 'chefschoice' else subscription_preference_name end as subscription_
,SUM(shipped_count) as total_volume_recipes
FROM materialized_views.local_recipe_reporting_final rsdq
where rsdq.country in ('NO')
and rsdq.hellofresh_week>='2020-W00'
group by 1,2,3)

SELECT Tab_1.country
,Tab_1.hellofresh_week
,recipe_index
,subscription
,count_ratings_preference
,total_volume_recipes
,score
,volume_recipes/total_volume_recipes as share_volume
,volume_recipes
FROM Tab_1
left join Tab_2
on Tab_1.hellofresh_week = Tab_2.hf_week
and Tab_1.country =Tab_2.tab2_country
and Tab_1.subscription=Tab_2.subscription_)


SELECT country,hellofresh_week,recipe_index
,COALESCE(AVG(cb_share),0) as cb_share
,COALESCE(AVG(fm_share),0) as fm_share
,COALESCE(AVG(vg_share),0) as vg_share
,COALESCE(AVG(r_share),0) as r_share
,COALESCE(AVG(cs_share),0) as cs_share
,COALESCE(AVG(cb_score),0) as cb_score
,COALESCE(AVG(fm_score),0) as fm_score
,COALESCE(AVG(vg_score),0) as vg_score
,COALESCE(AVG(r_score),0) as r_score
,COALESCE(AVG(cs_score),0) as cs_score
,COALESCE(AVG(cb_ratings),0) as cb_ratings
,COALESCE(AVG(fm_ratings),0) as fm_ratings
,COALESCE(AVG(vg_ratings),0) as vg_ratings
,COALESCE(AVG(r_ratings),0) as r_ratings
,COALESCE(AVG(cs_ratings),0) as cs_ratings
fROM (SELECT country,hellofresh_week,recipe_index
,case when subscription like 'chefschoice' then share_volume end as cb_share
,case when subscription ='family' then share_volume end as fm_share
,case when subscription ='veggie' then share_volume end as vg_share
,case when subscription ='quick' then share_volume end as r_share
,case when subscription ='lowcalorie' then share_volume end as cs_share

,case when subscription ='chefschoice' then score end as cb_score
,case when subscription ='family' then score end as fm_score
,case when subscription ='veggie' then score end as vg_score
,case when subscription ='quick' then score end as r_score
,case when subscription ='lowcalorie' then score end as cs_score

,case when subscription ='chefschoice' then count_ratings_preference end as cb_ratings
,case when subscription ='family' then count_ratings_preference end as fm_ratings
,case when subscription ='veggie' then count_ratings_preference end as vg_ratings
,case when subscription ='quick' then count_ratings_preference end as r_ratings
,case when subscription ='lowcalorie' then count_ratings_preference end as cs_ratings

From master) t
GROUP BY country,hellofresh_week,recipe_index )



SELECT overall_score.country
,overall_score.hellofresh_week
,overall_score.recipe_index
,remps.recipe_code
,split_part(recipe_code,'-',1) as split_code
,cast(split_part(recipe_code,'-',2) as BIGINT)  as split_code_version
,concat(split_part(recipe_code,'-',1) ,'-',split_part(recipe_code,'-',2)) as recipe_code_without_country
, recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
,overall_score.recipe_score
,overall_score.recipe_score_wo_scm
,cost2p
,overall_score.share_1s
,overall_score.share_volume
,Subscription_type.cb_score
,Subscription_type.cb_ratings
,Subscription_type.cb_share
,Subscription_type.fm_score
,Subscription_type.fm_ratings
,Subscription_type.fm_share
,Subscription_type.vg_score
,Subscription_type.vg_ratings
,Subscription_type.vg_share
,Subscription_type.r_score
,Subscription_type.r_ratings
,Subscription_type.r_share
,Subscription_type.cs_score
,Subscription_type.cs_ratings
,Subscription_type.cs_share
,overall_score.count_ratings
,overall_score.count_1s
, cuisine
, handsontime
, last_week
, next_week
, previous_score as old_version_previous_score
,overall_score.share_2s
,overall_score.share_3s
,overall_score.share_4s
, remps.last_update
,cost4p
,overall_score.volume_recipes
,overall_score.total_volume_recipes
FROM overall_score
LEFT JOIN remps
on overall_score.hellofresh_week=remps.hf_week
and overall_score.country=remps.country
and overall_score.recipe_index=remps.slot
LEFT JOIN Subscription_type
on Subscription_type.hellofresh_week= overall_score.hellofresh_week
and    Subscription_type.country= overall_score.country
and    Subscription_type.recipe_index= overall_score.recipe_index)

SELECT main_1.country
,hellofresh_week as hf_week
,recipe_index
,recipe_code
, recipe_name
, primaryprotein
, primarystarch
, primaryvegetable
,recipe_score
,recipe_score_wo_scm
,cost2p
,share_1s
,share_volume
,cb_score
,cb_ratings
,cb_share
,fm_score
,fm_ratings
,fm_share
,vg_score
,vg_ratings
,vg_share
,r_score
,r_ratings
,r_share
,cs_score
,cs_ratings
,cs_share
,count_ratings
,count_1s
, cuisine
, handsontime
, previous_table.last_week
, next_week
, previous_table.previous_score
,share_2s
,share_3s
,share_4s
, last_update
,cost4p
,volume_recipes
,total_volume_recipes

FROM MAIN as main_1
LEFT JOIN (SELECT  country
,hellofresh_week as last_week
,split_code
,split_code_version
,recipe_code_without_country
,concat(split_code,'-',CAST(split_code_version+1 AS STRING)) as new_recipe_code
,recipe_score as previous_score FROM MAIN) as previous_table
ON main_1.country = previous_table.country
AND main_1.recipe_code_without_country = previous_table.new_recipe_code) as tab_2) main

ORDER BY hf_week DESC
,recipe_index
,country