CREATE TABLE public.equity (
	"index" int8 NULL,
	"timestamp" timestamp NULL,
	symbol text NULL,
	"open" float8 NULL,
	high float8 NULL,
	low float8 NULL,
	"close" float8 NULL,
	volume int8 NULL,
	bid float8 NULL,
	ask float8 NULL,
	mark float8 NULL,
	CONSTRAINT equity_unique UNIQUE (symbol, "timestamp")
);
CREATE INDEX ix_equity_index ON public.equity USING btree (index);

CREATE TABLE public.test_table (
	"index" int8 NULL,
	"timestamp" timestamp NULL,
	symbol text NULL,
	"open" float8 NULL,
	high float8 NULL,
	low float8 NULL,
	"close" float8 NULL,
	volume int8 NULL,
	bid float8 NULL,
	ask float8 NULL,
	mark float8 NULL,
	CONSTRAINT test_unique UNIQUE (symbol, "timestamp")
);
CREATE INDEX ix_test_table_index ON public.equity USING btree (index);

CREATE TABLE public.historical_equity (
	"index" int8 NULL,
	"timestamp" timestamp NULL,
	symbol text NULL,
	"open" float8 NULL,
	high float8 NULL,
	low float8 NULL,
	"close" float8 NULL,
	volume int8 NULL,
	CONSTRAINT historical_equity_unique UNIQUE ("timestamp", symbol)
);
CREATE INDEX ix_historical_equity_index ON public.historical_equity USING btree (index);

CREATE TABLE public.options (
	"index" int8 NULL,
	"timestamp" timestamp NULL,
	symbol text NULL,
	expiration_date timestamp NULL,
	option_type text NULL,
	strike_price float8 NULL,
	bid float8 NULL,
	ask float8 NULL,
	bid_size int8 NULL,
	ask_size int8 NULL,
	mark_price float8 NULL,
	last_price float8 NULL,
	volume int8 NULL,
	volatility float8 NULL,
	open_interest int8 NULL,
	delta float8 NULL,
	gamma float8 NULL,
	theta float8 NULL,
	vega float8 NULL,
	rho float8 NULL,
	CONSTRAINT options_unique UNIQUE ("timestamp", symbol, expiration_date, option_type, strike_price)
);
CREATE INDEX ix_options_index ON public.options USING btree (index);

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