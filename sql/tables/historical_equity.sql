-- public.historical_equity definition

-- Drop table

-- DROP TABLE public.historical_equity;

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