-- Updated Monthly Sales Aggregation
-- Change: Added new discount_impact column
-- Author: syed.zyad@lumendata.com
-- Date: 2026-06-16

CREATE OR REPLACE TABLE dev_gold.reporting.agg_monthly_sales AS
SELECT 
    year,
    month,
    month_name,
    ROUND(SUM(revenue), 2) as total_revenue,
    ROUND(SUM(profit), 2) as total_profit,
    COUNT(DISTINCT order_id) as total_orders,
    ROUND(AVG(profit_margin), 4) as avg_profit_margin,
    ROUND(AVG(discount), 4) as avg_discount,
    ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) as revenue_per_order,
    -- NEW: discount impact analysis
    ROUND(SUM(discount * revenue), 2) as total_discount_impact,
    ROUND(AVG(CASE WHEN discount > 0.2 THEN profit_margin ELSE NULL END), 4) as high_discount_margin
FROM dev_silver.sales.fact_orders
GROUP BY year, month, month_name
ORDER BY year, month;