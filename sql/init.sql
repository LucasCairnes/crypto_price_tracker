CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS enriched_trades (
    symbol        TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    vwap          NUMERIC(38,8)    NOT NULL,
    volume        NUMERIC(38,8)    NOT NULL,
    trade_count   BIGINT           NOT NULL,
    open          NUMERIC(18,8)    NOT NULL,
    high          NUMERIC(18,8)    NOT NULL,
    low           NUMERIC(18,8)    NOT NULL,
    close         NUMERIC(18,8)    NOT NULL,
    PRIMARY KEY (symbol, window_start)
);

SELECT create_hypertable('enriched_trades', 'window_start', if_not_exists => TRUE);
