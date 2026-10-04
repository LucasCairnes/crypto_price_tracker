from pyflink.table import EnvironmentSettings, TableEnvironment

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

t_env.execute_sql("""
    CREATE TABLE raw_trades (
        event_type            STRING,
        event_time            BIGINT,
        symbol                STRING,
        agg_trade_id          BIGINT,
        price                 STRING,
        quantity              STRING,
        first_trade_id        BIGINT,
        last_trade_id         BIGINT,
        trade_time            BIGINT,
        buyer_is_market_maker BOOLEAN,
        rowtime AS TO_TIMESTAMP_LTZ(trade_time, 3),
        WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'raw_trades',
        'properties.bootstrap.servers' = 'redpanda:9092',
        'properties.group.id' = 'pyflink-vwap',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'protobuf',
        'protobuf.message-class-name' = 'market.Trade',
        'protobuf.ignore-parse-errors' = 'true'
    )
""")

t_env.execute_sql("""
    CREATE TABLE enriched_trades (
        symbol        STRING,
        window_start  STRING,
        window_end    STRING,
        vwap          STRING,
        volume        STRING,
        trade_count   BIGINT,
        `open`        STRING,
        high          STRING,
        low           STRING,
        `close`       STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'enriched_trades',
        'properties.bootstrap.servers' = 'redpanda:9092',
        'format' = 'json',
        'sink.partitioner' = 'fixed'
    )
""")

t_env.execute_sql("""
    CREATE TEMPORARY VIEW trades AS
    SELECT
        symbol,
        CAST(price AS DECIMAL(18, 8))    AS price,
        CAST(quantity AS DECIMAL(18, 8)) AS quantity,
        rowtime
    FROM raw_trades
""")

t_env.execute_sql("""
    INSERT INTO enriched_trades
    SELECT
        symbol,
        CAST(window_start AS STRING),
        CAST(window_end AS STRING),
        CAST(CAST(SUM(price * quantity) / SUM(quantity) AS DECIMAL(38, 8)) AS STRING) AS vwap,
        CAST(SUM(quantity) AS STRING)      AS volume,
        COUNT(*)                           AS trade_count,
        CAST(FIRST_VALUE(price) AS STRING) AS `open`,
        CAST(MAX(price) AS STRING)         AS high,
        CAST(MIN(price) AS STRING)         AS low,
        CAST(LAST_VALUE(price) AS STRING)  AS `close`
    FROM TABLE(
        TUMBLE(TABLE trades, DESCRIPTOR(rowtime), INTERVAL '10' SECOND)
    )
    GROUP BY symbol, window_start, window_end
""")
