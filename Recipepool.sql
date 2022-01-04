with all_scores as(
select 'DKSE' as country, 'all' as region, mainrecipecode
    ,sum(score*rating_count)/sum(rating_count) as scorewoscm
    ,sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    from(
select *
    ,dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
from materialized_views.gamp_recipe_scores
where (region='DK' or region='SE') and score>0 and rating_count>50
) t where o=1
group by 1,2,3
    )

, scores_dk as(
select 'DKSE' as country, 'dk' as region, mainrecipecode
    ,sum(score*rating_count)/sum(rating_count) as scorewoscm
    ,sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    from(
select *
    ,dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
from materialized_views.gamp_recipe_scores
where rating_count>50 and region='DK' and country='DK'
) t where o = 1
group by 1,2,3)


, scores_se as(
select 'DKSE' as country, 'se' as region, mainrecipecode
    ,sum(score*rating_count)/sum(rating_count) as scorewoscm
    ,sum(score_wscm*rating_count_wscm)/sum(rating_count_wscm) as scorescm
    from(
select *
    ,dense_rank() over (partition by mainrecipecode, region, case when right(uniquerecipecode,2) in ('FR','CH','DK') then right(uniquerecipecode,2) else 'X' end order by hellofresh_week desc) as o
from materialized_views.gamp_recipe_scores
where score>0 and rating_count>50 and region='SE' and country='SE'
) t where o = 1
group by 1,2,3)

, score_prefs_all as(
select * from (
select *
,dense_rank() over (partition by code, region order by hellofresh_week desc) as o
from
( select region as country,
      'all' as region,
      hellofresh_week,
      split_part(uniquerecipecode,'-',1) as code
        ,sum(score_classic*rating_count_classic)/sum(rating_count_classic) as scorewoscm_classic
        ,sum(rating_count_classic) as rating_count_classic
       ,sum(score_wscm_classic*rating_count_wscm_classic)/sum(rating_count_wscm_classic) as scorescm_classic
       ,sum(score_family*rating_count_family)/sum(rating_count_family) as scorewoscm_family
       ,sum(rating_count_family) as rating_count_family
       ,sum(score_wscm_family*rating_count_wscm_family)/sum(rating_count_wscm_family) as scorescm_family
       ,sum(score_veggie*rating_count_veggie)/sum(rating_count_veggie) as scorewoscm_veggie
       ,sum(rating_count_veggie) as rating_count_veggie
       ,sum(score_wscm_veggie*rating_count_wscm_veggie)/sum(rating_count_wscm_veggie) as scorescm_veggie
       ,sum(score_quick*rating_count_quick)/sum(rating_count_quick) as scorewoscm_quick
       ,sum(rating_count_quick) as rating_count_quick
       ,sum(score_wscm_quick*rating_count_wscm_quick)/sum(rating_count_wscm_quick) as scorescm_quick

from views_analysts.gamp_dkse_pref_scores
where region='DKSE'
group by 1,2,3,4
having (rating_count_family>50 or rating_count_veggie>50 or rating_count_quick>50 or rating_count_classic>50)
)t
)x
where o=1
)
-------
------ UPDATE THE PLANNING INTERVAL !!!!!!!
-------
,seasonality as(
    select sku,max(seasonality_score) as seasonality_score from uploads.gp_sku_seasonality
    where country='DKSE' and week>='W26' and week<='W29'
    group by 1
)

, last_recipe as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipes
where remps_instance='DKSE'
)t where o=1
)

, last_cost as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_recipecost
where remps_instance='DKSE'
)t where o=1
)

, last_nutrition AS (
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        FROM remps.recipe_nutritionalinfopp
        WHERE remps_instance='DKSE'
    ) AS t
    WHERE o = 1)

, last_tag as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_tags
where remps_instance='DKSE'
)t where o=1
)

, last_tag_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_tags_recipes
where remps_instance='DKSE'
)t where o=1
)


, last_product as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipe_producttypes
where remps_instance='DKSE'
)t where o=1
)

, last_preference as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_recipepreferences
where remps_instance='DKSE'
)t where o=1
)

, last_preference_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_recipepreferences_recipes
where remps_instance='DKSE'
)t where o=1
)

, last_hqtag as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.recipetags_hqtags
where remps_instance='DKSE'
)t where o=1
)

, last_hqtag_map as(
select *
from (
select *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
from remps.map_hqtags_recipes
where remps_instance='DKSE'
)t where o=1
)

,last_ingredient_group as(
    SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_ingredientgroup
    WHERE remps_instance='DKSE'
    ) AS t
WHERE o = 1)

, last_recipe_sku as(
SELECT *
    FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.recipe_recipeskus
    WHERE remps_instance='DKSE'
    ) AS t
WHERE o = 1)

,last_sku as(
SELECT *
FROM (
        SELECT *,
        dense_rank() over(partition by remps_instance order by fk_imported_at desc)o
        from remps.sku_sku
    WHERE remps_instance='DKSE'
    ) AS t
WHERE o = 1)


,picklists as(
    select
    uniquerecipecode
    , group_concat(code," | ") as skucode
    , group_concat(display_name, " | ") as skuname
    , max(coalesce(seasonality_score,0)) as seasonalityrisk
    , sum(coalesce(boxitem,0)) as boxitem
    , count(distinct code) as skucount
    from (
        select
        r.unique_recipe_code as uniquerecipecode
        , sku.code
        , regexp_replace(sku.display_name, '\t|\n', '') as display_name
        , seasonality_score
        , boxitem
        from last_recipe r
        join last_ingredient_group ig
        on r.id = ig.ingredient_group__recipe
        join last_recipe_sku rs
        on ig.id = rs.recipe_sku__ingredient_group
        join last_sku sku
        on sku.id = rs.recipe_sku__sku
        left join seasonality s on s.sku=sku.code
        left join uploads.gamp_dkse_boxitems b on b.code= sku.code
        group by 1,2,3,4,5) t
    group by 1
)

,hqtag as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.original_name,','),'') as name from last_recipe rr
left join last_hqtag_map m on rr.id= m.recipe_recipes_id
left join last_hqtag rt on rt.id=m.recipetags_hqtags_id
group by 1
)

, preference as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe rr
left join last_preference_map m on rr.id= m.recipe_recipes_id
left join last_preference rp on rp.id=m.recipetags_recipepreferences_id
group by 1
)

,producttype as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rp.name,','),'') as name from last_recipe rr
left join last_product rp on rp.id=rr.recipe__product_type
group by 1
)
, tag as(
select rr.unique_recipe_code as uniquerecipecode, coalesce(group_concat(distinct rt.name,','),'')as name from last_recipe rr
left join last_tag_map m on rr.id= m.recipe_recipes_id
left join last_tag rt on rt.id=m.recipetags_tags_id
group by 1
)

,weeks as(
select distinct hellofresh_week, hellofresh_running_week
from dimensions.date_dimension
)

, all_recipes_woscore as(
select * from(
select r.id as rempsid
       ,r.country
       ,r.uniquerecipecode
       ,r.mainrecipecode as code
       ,r.version
       ,r.status
       ,r.title
        ,concat(r.title,coalesce (r.subtitle,''),coalesce (r.primaryprotein,''),coalesce(r.primarystarch,''),coalesce(r.cuisine,''), coalesce(r.dishtype,''), coalesce(r.primaryvegetable,''),coalesce(r.primaryfruit,'')) as subtitle
       ,r.lastused
       ,case when w1.hellofresh_running_week is NOT NULL then w1.hellofresh_running_week else -1 end as lastusedrunning
       ,r.nextused
       ,case when w2.hellofresh_running_week is NOT NULL then w2.hellofresh_running_week else -1 end as nextusedrunning
       ,r.absolutelastused
       ,case when w3.hellofresh_running_week is NOT NULL then w3.hellofresh_running_week else -1 end as absolutelastusedrunning
       ,case when w1.hellofresh_running_week is not NULL and w2.hellofresh_running_week is not NULL THEN w2.hellofresh_running_week-w1.hellofresh_running_week
             else 0 end as lastnextuseddiff
       ,case when r.lastused is NULL and r.nextused is NULL THEN 1 else 0 end as isnewrecipe
       ,case when r.nextused is not NULL and r.lastused is NULL  then 1 else 0 end as isnewscheduled
       ,r.isdefault as isdefault
       ,r.primaryprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',1),r.primaryprotein)) as mainprotein
       ,TRIM(coalesce(split_part(r.primaryprotein,'-',2),r.primaryprotein)) as proteincut
       ,coalesce(r.secondaryprotein,'none') as secondaryprotein
       ,concat(coalesce (r.primaryprotein,''),coalesce(r.secondaryprotein,'none')) as proteins
       ,r.primarystarch
       ,coalesce(TRIM(coalesce(split_part(r.primarystarch,'-',1),r.primarystarch)),'none') as mainstarch
       ,coalesce(r.secondarystarch,'none') as secondarystarch
       ,concat(coalesce (r.primarystarch,''),coalesce(r.secondarystarch,'none')) as starches
       ,coalesce(r.primaryvegetable,'none') as primaryvegetable
       ,coalesce(TRIM(coalesce(split_part(r.primaryvegetable,'-',1),r.primaryvegetable)),'none') as mainvegetable
       ,concat(coalesce (r.primaryvegetable,'none'),coalesce(r.secondaryvegetable,'none'),coalesce(r.tertiaryvegetable,'none')) as vegetables
       ,coalesce(r.secondaryvegetable,'none') as secondaryvegetable
       ,coalesce(r.tertiaryvegetable,'none') as tertiaryvegetable
       ,coalesce(r.primarydryspice,'none') as primarydryspice
       ,coalesce(r.primarycheese,'none') as primarycheese
       ,coalesce(r.primaryfruit,'none') as primaryfruit
       ,coalesce(r.primarydairy,'none') as primarydairy
       ,coalesce(r.primaryfreshherb,'none') as primaryfreshherb
       ,coalesce(r.primarysauce,'none') as primarysauce
       ,case when n.salt is null then 0 else n.salt end as salt
       ,case when n.kilo_calories=0 then 999 else n.kilo_calories end as calories
       ,r.cuisine
       ,r.dishtype
       ,case when r.handsontime ="" or r.handsontime is NULL then cast(99 as float)
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is NULL then 99
             when length (r.handsontime) >3 and cast( left(r.handsontime,2) as float) is not NULL then cast( left(r.handsontime,2) as float)
             when length (r.handsontime) <2 then cast(99 as float)
             when r.handsontime='0' then cast(99 as float)
             else cast(r.handsontime as float) end as handsontime
       ,case when r.totaltime ="" or r.totaltime is NULL then cast(99 as float)
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is NULL then 99
             when length (r.totaltime) >3 and cast( left(r.totaltime,2) as float) is not NULL then cast( left(r.totaltime,2) as float)
             when length (r.totaltime) <2 then cast(99 as float)
             when r.totaltime='0' then cast(99 as float)
             else cast(r.totaltime as float) end as totaltime
       ,difficultylevel as difficulty
       ,ht.name as hqtag
       ,rt.name as tag
       ,pf.name as preference
       ,concat (ht.name,rt.name,pf.name) as preftag
       ,pt.name as producttype
       ,r.author
       ,round(rc.cost_1p,2) as cost1p
       ,round(rc.cost_2p,2) as cost2p
       ,round(rc.cost_3p,2) as cost3p
       ,round(rc.cost_4p,2) as cost4p
       -- ,case when s.scorescm is not NULL then s.scorescm
        --    when avg(s.scorescm) over (partition by r.primaryprotein, r.country ) is not NULL THEN avg(s.scorescm) over (partition by r.primaryprotein, r.country)
        --    when avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
        --       THEN avg(s.scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
        --    else 3.4
    --    end as scorescm
        ,coalesce( s_dk.scorescm,0) as scorescmdk
        ,coalesce( s_se.scorescm,0) as scorescmse
             --   ,case when s.scorewoscm is not NULL then s.scorewoscm
           --  when avg(s.scorewoscm) over (partition by temp.primaryprotein, temp.country ) is not NULL
            --     THEN avg(s.scorewoscm) over (partition by temp.primaryprotein, temp.country)
           --  when avg(s.scorewoscm) over (partition by split_part(temp.primaryprotein,'-',1), temp.country ) is not NULL
            --     THEN avg(s.scorewoscm) over (partition by split_part(temp.primaryprotein,'-',1), temp.country )
            -- else 3.4
           -- end as scorewoscm
        ,coalesce( s_dk.scorewoscm,0) as scorewoscmdk
        ,coalesce( s_se.scorewoscm,0) as scorewoscmse
        ,coalesce(spa.scorewoscm_classic,0) as scorewoscm_classic
        ,coalesce(spa.scorescm_classic,0) as scorescm_classic
        ,coalesce(spa.scorewoscm_family,0) as scorewoscm_family
        ,coalesce(spa.scorescm_family,0) as scorescm_family
        ,coalesce(spa.scorewoscm_veggie,0)  as scorewoscm_veggie
        ,coalesce(spa.scorescm_veggie,0)  as scorescm_veggie
       ,coalesce(spa.scorewoscm_quick,0) as scorewoscm_quick
        ,coalesce(spa.scorescm_quick,0) as scorescm_quick

  --  ,case when s.scorewoscm is  NULL then 1 else 0 end as isscorereplace
       ,p.skucode
       ,p.skuname
       ,p.skucount
       ,p.seasonalityrisk
      ,TO_TIMESTAMP(cast(r2.fk_imported_at as string),'yyyyMMdd') as updated_at --its not unix timestamp
     ,dense_rank() over (partition by r.mainrecipecode, r.country, case when right(r.uniquerecipecode,2) in ('FR','CH','DK') then right(r.uniquerecipecode,2) else 'X' end order by cast(r.version as int) desc) as o
     ,p.boxitem
from materialized_views.int_scm_analytics_remps_recipe as r
left join last_recipe r2 on r2.unique_recipe_code=r.uniquerecipecode
left join last_cost rc on rc.id=r2.recipe__recipe_cost
left join last_nutrition n on n.id=r.nutritionalinfo2p
--left join scores s on s.mainrecipecode=r.mainrecipecode and s.country=r.country
left join scores_dk s_dk on s_dk.mainrecipecode=r.mainrecipecode and s_dk.country=r.country
left join scores_se s_se on s_se.mainrecipecode=r.mainrecipecode and s_se.country=r.country
left join score_prefs_all as spa on spa.code=r.mainrecipecode and spa.country=r.country
left join picklists p on p.uniquerecipecode=r.uniquerecipecode
left join preference as pf on pf.uniquerecipecode=r.uniquerecipecode
left join hqtag as ht on ht.uniquerecipecode=r.uniquerecipecode
left join tag as rt on rt.uniquerecipecode=r.uniquerecipecode
left join producttype as pt on pt.uniquerecipecode=r.uniquerecipecode
left join weeks as w1 on w1.hellofresh_week=r.lastused
left join weeks as w2 on w2.hellofresh_week=r.nextused
left join weeks as w3 on w3.hellofresh_week=r.absolutelastused
where
     --lower(r.status)  not in ('inactive','rejected') and
     length (primaryprotein)>0
    and primaryprotein <>'N/A'
    and  r.country='DKSE'
    and right(r.uniquerecipecode,2)<>'DK'
    and rc.cost_2p > 0

) temp where o =1)

, score_prediction as (
    select r.*,
            case when scorescm is not NULL then scorescm
             when avg(scorescm) over (partition by r.primaryprotein, r.country ) is not NULL
                 THEN avg(scorescm) over (partition by r.primaryprotein, r.country)
             when avg(scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                 THEN avg(scorescm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
            else 3.4
            end as scorescm
       ,case when scorewoscm is not NULL then scorewoscm
             when avg(scorewoscm) over (partition by r.primaryprotein, r.country ) is not NULL
                 THEN avg(scorewoscm) over (partition by r.primaryprotein, r.country)
             when avg(scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country ) is not NULL
                 THEN avg(scorewoscm) over (partition by split_part(r.primaryprotein,'-',1), r.country )
             else 3.4
            end as scorewoscm
        ,case when s.scorewoscm is  NULL then 1 else 0 end as isscorereplace
from all_recipes_woscore r
left join all_scores s on s.mainrecipecode = r.code and s.country = r.country
)

,all_recipes as(
select
    rempsid,
    country,
    uniquerecipecode,
    code,
    version,
    status,
    title,
    subtitle,
    lastused,
    lastusedrunning,
    nextused,
    nextusedrunning,
    absolutelastused,
    absolutelastusedrunning,
    lastnextuseddiff,
    isnewrecipe,
    isnewscheduled,
    isdefault,
    primaryprotein,
    mainprotein,
    proteincut,
    secondaryprotein,
    proteins,
    primarystarch,
    mainstarch,
    secondarystarch,
    starches,
    primaryvegetable,
    mainvegetable,
    vegetables,
    secondaryvegetable,
    tertiaryvegetable,
    primarydryspice,
    primarycheese,
    primaryfruit,
    primarydairy,
    primaryfreshherb,
    primarysauce,
    salt,
    calories,
    cuisine,
    dishtype,
    handsontime,
    totaltime,
    difficulty,
    hqtag,
    tag,
    preference,
    preftag,
    producttype,
    author,
    cost1p,
    cost2p,
    cost3p,
    cost4p,
    skucode,
    skuname,
    skucount,
    seasonalityrisk,
    scorescm,
    scorewoscm,
    scorewoscmdk,
    scorewoscmse,
    scorescmdk,
    scorescmse,
    isscorereplace,
    scorewoscm_classic,
    scorescm_classic,
    scorewoscm_family,
    scorescm_family,
    scorewoscm_quick,
    scorescm_quick,
    scorewoscm_veggie,
    scorescm_veggie,
    updated_at,
    o,
    boxitem
from score_prediction
where lower(status) not in ('inactive','rejected')
)


select * from all_recipes