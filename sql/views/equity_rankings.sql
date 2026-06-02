-- public.equity_rankings source

CREATE MATERIALIZED VIEW public.equity_rankings
AS SELECT "timestamp",
    symbol,
    hourly_return,
    rolling_volatility,
    relative_volume,
    rank() OVER (PARTITION BY "timestamp" ORDER BY hourly_return DESC) AS gainer_rank,
    rank() OVER (PARTITION BY "timestamp" ORDER BY hourly_return) AS loser_rank,
    rank() OVER (PARTITION BY "timestamp" ORDER BY rolling_volatility DESC) AS volatility_rank,
    rank() OVER (PARTITION BY "timestamp" ORDER BY relative_volume DESC) AS volume_rank
   FROM equity_hourly_metrics
WITH DATA;