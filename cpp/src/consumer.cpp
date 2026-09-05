#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <unistd.h>

using json = nlohmann::json;

std::atomic<bool> keep_running{true};

void handle_signal(int) {
    keep_running = false;
}

struct EnrichedTrade {
    std::string symbol;
    std::string window_start;
    std::string window_end;
    double vwap;
    double volume;
    int64_t trade_count;
    double open;
    double high;
    double low;
    double close;
};

static std::string env_or(const char *name, const std::string &fallback) {
    const char *value = std::getenv(name);
    return value ? std::string(value) : fallback;
}

static std::string build_conninfo() {
    return "host=" + env_or("PG_HOST", "timescaledb") +
           " port=" + env_or("PG_PORT", "5432") +
           " dbname=" + env_or("PG_DB", "market") +
           " user=" + env_or("PG_USER", "market") +
           " password=" + env_or("PG_PASSWORD", "market");
}

// Resident set size of this process in bytes, read from /proc/self/statm.
static double read_rss_bytes() {
    std::ifstream statm("/proc/self/statm");
    long pages_total = 0, pages_resident = 0;
    if (statm >> pages_total >> pages_resident) {
        return static_cast<double>(pages_resident) * sysconf(_SC_PAGESIZE);
    }
    return 0.0;
}

struct Metrics {
    prometheus::Counter &messages_consumed;
    prometheus::Counter &rows_committed;
    prometheus::Counter &batches_flushed;
    prometheus::Counter &flush_errors;
    prometheus::Counter &parse_errors;
    prometheus::Histogram &flush_duration;
    prometheus::Gauge &resident_memory;
    prometheus::Gauge &batch_fill;
};

class TimescaleSink {
    private:
        pqxx::connection conn;
        Metrics &metrics;

    public:
        TimescaleSink(const std::string &conninfo, Metrics &m)
            : conn(conninfo), metrics(m) {}

        void flush(const std::vector<EnrichedTrade> &batch) {
            if (batch.empty()) return;
            auto start = std::chrono::steady_clock::now();
            try {
                pqxx::work txn(conn);
                std::string sql =
                    "INSERT INTO enriched_trades "
                    "(symbol, window_start, window_end, vwap, volume, trade_count, "
                    "open, high, low, close) VALUES ";
                pqxx::params params;
                constexpr size_t cols = 10;
                for (size_t i = 0; i < batch.size(); ++i) {
                    size_t base = i * cols;
                    if (i) sql += ",";
                    sql += "(";
                    for (size_t c = 0; c < cols; ++c) {
                        if (c) sql += ",";
                        sql += "$" + std::to_string(base + c + 1);
                    }
                    sql += ")";
                    params.append(batch[i].symbol);
                    params.append(batch[i].window_start);
                    params.append(batch[i].window_end);
                    params.append(batch[i].vwap);
                    params.append(batch[i].volume);
                    params.append(batch[i].trade_count);
                    params.append(batch[i].open);
                    params.append(batch[i].high);
                    params.append(batch[i].low);
                    params.append(batch[i].close);
                }
                sql +=
                    " ON CONFLICT (symbol, window_start) DO UPDATE SET "
                    "window_end = EXCLUDED.window_end, "
                    "vwap = EXCLUDED.vwap, "
                    "volume = EXCLUDED.volume, "
                    "trade_count = EXCLUDED.trade_count, "
                    "open = EXCLUDED.open, "
                    "high = EXCLUDED.high, "
                    "low = EXCLUDED.low, "
                    "close = EXCLUDED.close";
                txn.exec_params(sql, params);
                txn.commit();
            } catch (...) {
                metrics.flush_errors.Increment();
                throw;
            }
            std::chrono::duration<double> elapsed =
                std::chrono::steady_clock::now() - start;
            metrics.flush_duration.Observe(elapsed.count());
            metrics.batches_flushed.Increment();
            metrics.rows_committed.Increment(static_cast<double>(batch.size()));
            std::cout << "committed " << batch.size() << " rows\n";
        }
};

static bool parse_message(const std::string &payload, EnrichedTrade &out) {
    auto j = json::parse(payload);
    out.symbol = j.at("symbol").get<std::string>();
    out.window_start = j.at("window_start").get<std::string>();
    out.window_end = j.at("window_end").get<std::string>();
    out.vwap = j.at("vwap").get<double>();
    out.volume = j.at("volume").get<double>();
    out.trade_count = j.at("trade_count").get<int64_t>();
    out.open = j.at("open").get<double>();
    out.high = j.at("high").get<double>();
    out.low = j.at("low").get<double>();
    out.close = j.at("close").get<double>();
    return true;
}

int main() {
    try {
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);

        std::string brokers = env_or("BROKERS", "localhost:19092");
        std::string topic = env_or("TOPIC", "enriched_trades");
        std::string group = env_or("GROUP_ID", "timescale-sink");
        size_t batch_size = std::stoul(env_or("BATCH_SIZE", "500"));
        std::string metrics_bind = "0.0.0.0:" + env_or("METRICS_PORT", "9102");

        prometheus::Exposer exposer{metrics_bind};
        auto registry = std::make_shared<prometheus::Registry>();

        auto &consumed_family = prometheus::BuildCounter()
            .Name("consumer_messages_consumed_total")
            .Help("Messages consumed from the enriched_trades topic")
            .Register(*registry);
        auto &rows_family = prometheus::BuildCounter()
            .Name("consumer_rows_committed_total")
            .Help("Rows committed to TimescaleDB")
            .Register(*registry);
        auto &batches_family = prometheus::BuildCounter()
            .Name("consumer_batches_flushed_total")
            .Help("Batches flushed to TimescaleDB")
            .Register(*registry);
        auto &flush_err_family = prometheus::BuildCounter()
            .Name("consumer_flush_errors_total")
            .Help("Failed flushes to TimescaleDB")
            .Register(*registry);
        auto &parse_err_family = prometheus::BuildCounter()
            .Name("consumer_parse_errors_total")
            .Help("Messages that failed to parse")
            .Register(*registry);
        auto &flush_hist_family = prometheus::BuildHistogram()
            .Name("consumer_flush_duration_seconds")
            .Help("Wall-clock duration of a TimescaleDB flush")
            .Register(*registry);
        auto &mem_family = prometheus::BuildGauge()
            .Name("consumer_resident_memory_bytes")
            .Help("Resident set size of the consumer process")
            .Register(*registry);
        auto &fill_family = prometheus::BuildGauge()
            .Name("consumer_batch_fill")
            .Help("Number of rows currently buffered in the pending batch")
            .Register(*registry);

        Metrics metrics{
            consumed_family.Add({}),
            rows_family.Add({}),
            batches_family.Add({}),
            flush_err_family.Add({}),
            parse_err_family.Add({}),
            flush_hist_family.Add({}, prometheus::Histogram::BucketBoundaries{
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5}),
            mem_family.Add({}),
            fill_family.Add({}),
        };
        exposer.RegisterCollectable(registry);

        TimescaleSink sink(build_conninfo(), metrics);

        std::string errstr;
        std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        conf->set("bootstrap.servers", brokers, errstr);
        conf->set("group.id", group, errstr);
        conf->set("auto.offset.reset", "earliest", errstr);
        conf->set("enable.auto.commit", "false", errstr);

        std::unique_ptr<RdKafka::KafkaConsumer> consumer(
            RdKafka::KafkaConsumer::create(conf.get(), errstr));
        if (!consumer) {
            std::cerr << "Failed to create consumer: " << errstr << "\n";
            return 1;
        }

        RdKafka::ErrorCode sub_err = consumer->subscribe({topic});
        if (sub_err) {
            std::cerr << "Failed to subscribe: " << RdKafka::err2str(sub_err) << "\n";
            return 1;
        }

        std::cout << "Consumer running. Metrics on " << metrics_bind
                  << ". Send SIGINT/SIGTERM (docker stop) to exit.\n";

        std::vector<EnrichedTrade> batch;
        batch.reserve(batch_size);

        while (keep_running) {
            std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR: {
                    metrics.messages_consumed.Increment();
                    try {
                        EnrichedTrade trade;
                        std::string payload(
                            static_cast<const char *>(msg->payload()), msg->len());
                        if (parse_message(payload, trade)) {
                            batch.push_back(std::move(trade));
                        }
                    } catch (const std::exception &e) {
                        metrics.parse_errors.Increment();
                        std::cerr << "skipping bad message: " << e.what() << "\n";
                    }
                    if (batch.size() >= batch_size) {
                        sink.flush(batch);
                        consumer->commitSync();
                        batch.clear();
                    }
                    break;
                }
                case RdKafka::ERR__TIMED_OUT:
                case RdKafka::ERR__PARTITION_EOF: {
                    if (!batch.empty()) {
                        sink.flush(batch);
                        consumer->commitSync();
                        batch.clear();
                    }
                    break;
                }
                default:
                    std::cerr << "consume error: " << msg->errstr() << "\n";
                    break;
            }
            metrics.batch_fill.Set(static_cast<double>(batch.size()));
            metrics.resident_memory.Set(read_rss_bytes());
        }

        if (!batch.empty()) {
            sink.flush(batch);
            consumer->commitSync();
            batch.clear();
        }

        consumer->close();

    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
