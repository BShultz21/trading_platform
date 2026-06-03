-- public.equity_hourly_metrics source

CREATE MATERIALIZED VIEW public.equity_hourly_metrics
AS WITH base_data AS (
         SELECT historical_equity.symbol,
            historical_equity."timestamp",
            historical_equity.close,
            historical_equity.volume,
            lag(historical_equity.close) OVER (PARTITION BY historical_equity.symbol ORDER BY historical_equity."timestamp") AS previous_close
           FROM historical_equity
        ), returns AS (
         SELECT base_data.symbol,
            base_data."timestamp",
            base_data.close,
            base_data.volume,
            base_data.previous_close,
            (base_data.close - base_data.previous_close) / NULLIF(base_data.previous_close, 0::double precision) * 100::double precision AS hourly_return
           FROM base_data
        )
 SELECT symbol,
    "timestamp",
    close,
    volume,
    previous_close,
    hourly_return,
    stddev(hourly_return) OVER (PARTITION BY symbol ORDER BY "timestamp" ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_volatility,
    volume::numeric / NULLIF(avg(volume) OVER (PARTITION BY symbol ORDER BY "timestamp" ROWS BETWEEN 23 PRECEDING AND CURRENT ROW), 0::numeric) AS relative_volume
   FROM returns
WITH DATA;