-- Run after notebooks/02_build_marts.py. Replace the catalog/schema as needed.

-- Store × product family demand with promotion mix and business context.
SELECT date, store_nbr, city, state, family, net_units, positive_units,
       promotion_share, transactions, dcoilwtico, is_holiday
FROM main.retail_intelligence.gold_daily_family_store;

-- Promotion-associated difference in average positive units per item-day.
-- This is descriptive and is not a causal estimate.
SELECT store_nbr, family, item_days, avg_units_per_item_day,
       nonpromo_avg, promo_avg, associated_change_pct
FROM main.retail_intelligence.gold_promotion_summary
ORDER BY associated_change_pct DESC;

-- Forecast evaluation and future family-level predictions.
SELECT * FROM main.retail_intelligence.gold_forecast_validation;
SELECT * FROM main.retail_intelligence.gold_forecasts;

