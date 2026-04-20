-- Top 20 zonas de pickup por revenue total
SELECT
  pickup_zone,
  pickup_borough,
  total_trips,
  total_revenue,
  avg_fare,
  avg_tip_pct
FROM IDENTIFIER(:catalog || '.gold.revenue_by_zone')
WHERE pickup_borough = COALESCE(:borough, pickup_borough)
ORDER BY total_revenue DESC
LIMIT 20;
