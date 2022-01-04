SELECT
        week,
       day,
       country,
       dc,
       region,
       product_family,
       servings,
       size,
       meal_number,
       meals_to_deliver,
       revenue_slot,
       fk_imported_at
FROM
    materialized_views.remps_forecast_volumeforecastraw
WHERE
    country = 'SE'
and week = '2021-W51'

ORDER BY
1, 2, 4, 7, 8, 9

