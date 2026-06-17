  select
      symbol,
      anomaly_type,
      date_trunc('day', detected_at) as day,
      count(*) as anomaly_count,
      avg(z_score) as avg_z_score,
      max(abs(z_score)) as max_z_score
  from {{ source('public', 'anomalies') }}
  group by symbol, anomaly_type, date_trunc('day', detected_at)