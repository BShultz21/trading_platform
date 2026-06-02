-- public.options_expiration_summary source

CREATE MATERIALIZED VIEW public.options_expiration_summary
AS SELECT symbol,
    "timestamp",
    expiration_date,
    count(*) AS contracts,
    sum(volume) AS total_volume,
    sum(open_interest) AS total_open_interest
   FROM options
  GROUP BY symbol, "timestamp", expiration_date
WITH DATA;