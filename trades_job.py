from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    source_ddl = """
        CREATE TABLE raw_trades (
            event_type            STRING,
            event_time            BIGINT,
            symbol                STRING,
            agg_trade_id          BIGINT,
            price                 DOUBLE,
            quantity              DOUBLE,
            first_trade_id        BIGINT,
            last_trade_id         BIGINT,
            trade_time            BIGINT,
            buyer_is_market_maker BOOLEAN,
            rowtime AS TO_TIMESTAMP_LTZ(trade_time, 3),
            WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND
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
    """
    t_env.execute_sql(source_ddl)

    sink_ddl = """
        CREATE TABLE enriched_trades (
            symbol        STRING,
            window_start  STRING,
            window_end    STRING,
            vwap          DOUBLE,
            volume        DOUBLE,
            trade_count   BIGINT,
            `open`        DOUBLE,
            high          DOUBLE,
            low           DOUBLE,
            `close`       DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'enriched_trades',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'format' = 'json',
            'sink.partitioner' = 'fixed'
        )
    """
    t_env.execute_sql(sink_ddl)

    vwap_query = """
        INSERT INTO enriched_trades
        SELECT
            symbol,
            CAST(window_start AS STRING)          AS window_start,
            CAST(window_end   AS STRING)          AS window_end,
            SUM(price * quantity) / SUM(quantity) AS vwap,
            SUM(quantity)                         AS volume,
            COUNT(*)                              AS trade_count,
            FIRST_VALUE(price)                    AS `open`,
            MAX(price)                            AS high,
            MIN(price)                            AS low,
            LAST_VALUE(price)                     AS `close`
        FROM TABLE(
            TUMBLE(TABLE raw_trades, DESCRIPTOR(rowtime), INTERVAL '10' SECOND)
        )
        GROUP BY symbol, window_start, window_end
    """

    print("Submitting PyFlink VWAP job...")
    t_env.execute_sql(vwap_query)


if __name__ == '__main__':
    main()
