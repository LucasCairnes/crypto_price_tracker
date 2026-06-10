from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # 1. Set up the Flink Table Environment in streaming mode
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # 2. Define the Source (Where the data comes from and its schema)
    # Column names must match Binance JSON keys exactly for deserialization to work.
    # A view below renames them to descriptive labels.
    source_ddl = """
        CREATE TABLE raw_trades (
            e  STRING,
            `E` BIGINT,
            s  STRING,
            a  BIGINT,
            p  STRING,
            q  STRING,
            f  BIGINT,
            l  BIGINT,
            `T` BIGINT,
            m  BOOLEAN,
            `M` BOOLEAN,
            event_time AS TO_TIMESTAMP_LTZ(`T`, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw_trades',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'pyflink-processor',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """
    t_env.execute_sql(source_ddl)

    # 2b. View with descriptive column names on top of the raw source
    trades_view = """
        CREATE VIEW trades AS
        SELECT
            e                        AS event_type,
            event_time               AS event_time,
            s                        AS symbol,
            a                        AS agg_trade_id,
            CAST(p AS DOUBLE)        AS price,
            CAST(q AS DOUBLE)        AS quantity,
            f                        AS first_trade_id,
            l                        AS last_trade_id,
            m                        AS buyer_is_market_maker
        FROM raw_trades
    """
    t_env.execute_sql(trades_view)

    # 3. Define the Sink (Where the processed data goes)
    # The 'print' connector simply outputs the results to your terminal/logs.
    sink_ddl = """
        CREATE TABLE aggregated_output (
            symbol        STRING,
            total_volume  DOUBLE
        ) WITH (
            'connector' = 'print'
        )
    """
    t_env.execute_sql(sink_ddl)

    # 4. Write the Processing Logic
    # We use standard SQL to process the stream as it arrives.
    processing_query = """
        INSERT INTO aggregated_output
        SELECT
            symbol,
            SUM(quantity) AS total_volume
        FROM trades
        GROUP BY symbol
    """

    # 5. Execute the Job
    # This submits the pipeline to the Flink cluster (or runs it locally).
    print("Starting PyFlink job...")
    t_env.execute_sql(processing_query)

if __name__ == '__main__':
    main()
