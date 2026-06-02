-- public."options" definition

-- Drop table

-- DROP TABLE public."options";

CREATE TABLE public."options" (
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