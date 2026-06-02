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