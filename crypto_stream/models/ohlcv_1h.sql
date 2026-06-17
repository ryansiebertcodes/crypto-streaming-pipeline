  with base as (
      select
          symbol,
          date_trunc('hour', window_start) as hour_start,
          open,
          high,
          low,
          close,
          volume,
          row_number() over (partition by symbol, date_trunc('hour', window_start) order by window_start asc) as rn_asc,
          row_number() over (partition by symbol, date_trunc('hour', window_start) order by window_start desc) as rn_desc
      from {{ source('public', 'ohlcv_1m') }}
  )
  select
      symbol,
      hour_start,
      max(case when rn_asc = 1 then open end) as open,
      max(high) as high,
      min(low) as low,
      max(case when rn_desc = 1 then close end) as close,
      sum(volume) as volume
  from base
  group by symbol, hour_start