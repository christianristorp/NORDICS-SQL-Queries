 WITH base AS (
    SELECT
        country
        , hellofresh_delivery_week
        , customer_id
        , MAX(IF(product_type = 'meal box', 1, 0)) AS has_mealbox
        , MAX(IF(product_type = 'add-ons', 1, 0)) AS has_addons
        , MAX(IF(product_type = 'extra meals', 1, 0)) AS has_extrameals
        , MAX(IF(product_type = 'surcharges', 1, 0)) AS has_surcharges
    FROM
        materialized_views.surcharge_items_granular_adjusted
    WHERE
        Country in ('DK', 'SE', 'NO')
    AND hellofresh_delivery_week >= '2021-W01'
    GROUP BY
        1,2,3
    )

SELECT
    country
    , hellofresh_delivery_week
    , SUM(has_addons)/SUM(has_mealbox) AS addon_uptake
    , SUM(has_extrameals)/SUM(has_mealbox) AS extrameals_uptake
    , SUM(has_surcharges)/SUM(has_mealbox) AS surcharges_uptake
FROM
    base
GROUP BY
    1,2
Order by
    2,1
