-- KPIs globales para counters del dashboard
-- Parámetro :catalog debe estar configurado en el dashboard (ej: casm_nytaxi)
SELECT
  COUNT(*)                                AS total_trips,
  ROUND(SUM(total_amount) / 1e6, 2)       AS total_revenue_millions,
  ROUND(AVG(total_amount), 2)             AS avg_fare,
  ROUND(AVG(trip_distance), 2)            AS avg_distance_mi,
  ROUND(AVG(trip_duration_min), 2)        AS avg_duration_min,
  ROUND(SUM(tip_amount) / 1e6, 2)         AS total_tips_millions,
  ROUND(AVG(tip_rate) * 100, 1)           AS avg_tip_pct
FROM IDENTIFIER(:catalog || '.silver.trips_enriched')
WHERE pickup_date BETWEEN :start_date AND :end_date;
