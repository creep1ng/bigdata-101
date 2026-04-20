-- Serie temporal diaria: trips y revenue
SELECT
  pickup_date,
  total_trips,
  total_revenue,
  avg_fare
FROM IDENTIFIER(:catalog || '.gold.daily_metrics')
WHERE pickup_date BETWEEN :start_date AND :end_date
ORDER BY pickup_date;
