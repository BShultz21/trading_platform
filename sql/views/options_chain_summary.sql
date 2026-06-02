-- public.options_chain_summary source

CREATE MATERIALIZED VIEW public.options_chain_summary
AS SELECT symbol,
    "timestamp",
    sum(
        CASE
            WHEN option_type = 'CALL'::text THEN volume
            ELSE 0::bigint
        END) AS total_call_volume,
    sum(
        CASE
            WHEN option_type = 'PUT'::text THEN volume
            ELSE 0::bigint
        END) AS total_put_volume,
    round(sum(
        CASE
            WHEN option_type = 'PUT'::text THEN volume
            ELSE 0::bigint
        END) / NULLIF(sum(
        CASE
            WHEN option_type = 'CALL'::text THEN volume
            ELSE 0::bigint
        END), 0::numeric), 4) AS put_call_ratio,
    sum(open_interest) AS total_open_interest
   FROM options
  GROUP BY symbol, "timestamp"
WITH DATA;