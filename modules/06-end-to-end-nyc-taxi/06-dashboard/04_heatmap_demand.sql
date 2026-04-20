-- Heatmap: demanda hora × día de semana
SELECT
  pickup_dayofweek,
  CASE pickup_dayofweek
    WHEN 1 THEN 'Dom' WHEN 2 THEN 'Lun' WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Mie' WHEN 5 THEN 'Jue' WHEN 6 THEN 'Vie'
    WHEN 7 THEN 'Sab'
  END AS day_name,
  pickup_hour,
  SUM(trips) AS trips
FROM IDENTIFIER(:catalog || '.gold.hourly_demand')
WHERE pickup_borough = COALESCE(:borough, pickup_borough)
GROUP BY pickup_dayofweek, pickup_hour
ORDER BY pickup_dayofweek, pickup_hour;
