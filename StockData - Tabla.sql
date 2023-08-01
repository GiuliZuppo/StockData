-- giulianazuppo_coderhouse.stockdata definition

-- Drop table

-- DROP TABLE giulianazuppo_coderhouse.stockdata;

--DROP TABLE giulianazuppo_coderhouse.stockdata;
CREATE TABLE IF NOT EXISTS giulianazuppo_coderhouse.stockdata
(
	fecha TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"open" NUMERIC(10,2)   ENCODE az64
	,high NUMERIC(10,2)   ENCODE az64
	,low NUMERIC(10,2)   ENCODE az64
	,"close" NUMERIC(10,2)   ENCODE az64
	,volume BIGINT   ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE giulianazuppo_coderhouse.stockdata owner to giulianazuppo_coderhouse;