CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS enriched_trades (
    symbol        TEXT             NOT NULL,
    window_start  TIMESTAMPTZ      NOT NULL,
    window_end    TIMESTAMPTZ      NOT NULL,
    vwap          DOUBLE PRECISION NOT NULL,
    volume        DOUBLE PRECISION NOT NULL,
    trade_count   BIGINT           NOT NULL,
    open          DOUBLE PRECISION NOT NULL,
    high          DOUBLE PRECISION NOT NULL,
    low           DOUBLE PRECISION NOT NULL,
    close         DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (symbol, window_start)
);

SELECT create_hypertable('enriched_trades', 'window_start', if_not_exists => TRUE);
