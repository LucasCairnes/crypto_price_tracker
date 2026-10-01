#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

using json = nlohmann::json;

const std::string BROKERS = "redpanda:9092";
const std::string TOPIC = "enriched_trades";
const std::string GROUP_ID = "timescale-sink";
const std::string CONNINFO = "host=timescaledb port=5432 dbname=market user=market password=market";
const std::string METRICS_BIND = "0.0.0.0:9102";
const size_t BATCH_SIZE = 500;
const auto MAX_BATCH_AGE = std::chrono::milliseconds(250);
const int POLL_TIMEOUT_MS = 100;

struct Candle {
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

std::atomic<bool> running{true};

void stop(int) { running = false; }

double resident_bytes() {
    std::ifstream statm("/proc/self/statm");
    long total = 0, resident = 0;
    statm >> total >> resident;
    return double(resident) * sysconf(_SC_PAGESIZE);
}

prometheus::Counter &counter(prometheus::Registry &reg, const std::string &name, const std::string &help) {
    return prometheus::BuildCounter().Name(name).Help(help).Register(reg).Add({});
}

prometheus::Gauge &gauge(prometheus::Registry &reg, const std::string &name, const std::string &help) {
    return prometheus::BuildGauge().Name(name).Help(help).Register(reg).Add({});
}

Candle parse_candle(const std::string &payload) {
    auto j = json::parse(payload);
    Candle c;
    c.symbol = j.at("symbol").get<std::string>();
    c.window_start = j.at("window_start").get<std::string>();
    c.window_end = j.at("window_end").get<std::string>();
    c.vwap = j.at("vwap").get<double>();
    c.volume = j.at("volume").get<double>();
    c.trade_count = j.at("trade_count").get<int64_t>();
    c.open = j.at("open").get<double>();
    c.high = j.at("high").get<double>();
    c.low = j.at("low").get<double>();
    c.close = j.at("close").get<double>();
    return c;
}

void upsert(pqxx::connection &db, const std::vector<Candle> &batch) {
    pqxx::work txn(db);
    std::string sql =
        "INSERT INTO enriched_trades (symbol, window_start, window_end, vwap, "
        "volume, trade_count, open, high, low, close) VALUES ";

    for (size_t i = 0; i < batch.size(); ++i) {
        const Candle &c = batch[i];
        if (i) sql += ",";
        sql += "(" + txn.quote(c.symbol) +
               "," + txn.quote(c.window_start) +
               "," + txn.quote(c.window_end) +
               "," + txn.quote(c.vwap) +
               "," + txn.quote(c.volume) +
               "," + txn.quote(c.trade_count) +
               "," + txn.quote(c.open) +
               "," + txn.quote(c.high) +
               "," + txn.quote(c.low) +
               "," + txn.quote(c.close) + ")";
    }

    sql += " ON CONFLICT (symbol, window_start) DO UPDATE SET "
           "window_end = EXCLUDED.window_end, vwap = EXCLUDED.vwap, "
           "volume = EXCLUDED.volume, trade_count = EXCLUDED.trade_count, "
           "open = EXCLUDED.open, high = EXCLUDED.high, "
           "low = EXCLUDED.low, close = EXCLUDED.close";

    txn.exec(sql);
    txn.commit();
}

int main() {
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    prometheus::Exposer exposer{METRICS_BIND};
    auto registry = std::make_shared<prometheus::Registry>();
    auto &consumed = counter(*registry, "consumer_messages_consumed_total", "Messages read from Redpanda");
    auto &rows_committed = counter(*registry, "consumer_rows_committed_total", "Rows written to TimescaleDB");
    auto &batches_flushed = counter(*registry, "consumer_batches_flushed_total", "Batches written to TimescaleDB");
    auto &flush_errors = counter(*registry, "consumer_flush_errors_total", "Failed writes to TimescaleDB");
    auto &parse_errors = counter(*registry, "consumer_parse_errors_total", "Messages that failed to parse");
    auto &memory = gauge(*registry, "consumer_resident_memory_bytes", "Resident set size");
    auto &batch_fill = gauge(*registry, "consumer_batch_fill", "Rows waiting in the current batch");
    auto &flush_seconds = prometheus::BuildHistogram()
        .Name("consumer_flush_duration_seconds")
        .Help("Time taken by a batched upsert")
        .Register(*registry)
        .Add({}, prometheus::Histogram::BucketBoundaries{
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5});
    exposer.RegisterCollectable(registry);

    // TimescaleDB is usually still starting up when this container comes up.
    std::unique_ptr<pqxx::connection> db;
    while (running && !db) {
        try {
            db = std::make_unique<pqxx::connection>(CONNINFO);
        } catch (const std::exception &e) {
            std::cerr << "waiting for timescaledb: " << e.what() << "\n";
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    std::string err;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", BROKERS, err);
    conf->set("group.id", GROUP_ID, err);
    conf->set("auto.offset.reset", "earliest", err);
    conf->set("enable.auto.commit", "false", err);

    std::unique_ptr<RdKafka::KafkaConsumer> consumer(RdKafka::KafkaConsumer::create(conf.get(), err));
    if (!consumer) {
        std::cerr << "could not create consumer: " << err << "\n";
        return 1;
    }
    if (consumer->subscribe({TOPIC})) {
        std::cerr << "could not subscribe to " << TOPIC << "\n";
        return 1;
    }

    std::vector<Candle> batch;
    batch.reserve(BATCH_SIZE);
    std::chrono::steady_clock::time_point batch_started;

    auto flush = [&]() {
        if (batch.empty()) return;
        auto start = std::chrono::steady_clock::now();
        try {
            upsert(*db, batch);
        } catch (const std::exception &e) {
            flush_errors.Increment();
            std::cerr << "flush failed: " << e.what() << "\n";
            return;
        }
        std::chrono::duration<double> elapsed = std::chrono::steady_clock::now() - start;
        flush_seconds.Observe(elapsed.count());
        batches_flushed.Increment();
        rows_committed.Increment(batch.size());
        std::cout << "committed " << batch.size() << " rows\n";
        consumer->commitSync();
        batch.clear();
    };

    std::cout << "consumer running, metrics on " << METRICS_BIND << "\n";

    while (running) {
        std::unique_ptr<RdKafka::Message> msg(consumer->consume(POLL_TIMEOUT_MS));

        if (msg->err() == RdKafka::ERR_NO_ERROR) {
            consumed.Increment();
            try {
                Candle c = parse_candle(
                    std::string(static_cast<const char *>(msg->payload()), msg->len()));
                if (batch.empty()) batch_started = std::chrono::steady_clock::now();
                batch.push_back(std::move(c));
            } catch (const std::exception &e) {
                parse_errors.Increment();
                std::cerr << "bad message: " << e.what() << "\n";
            }
        } else if (msg->err() != RdKafka::ERR__TIMED_OUT) {
            std::cerr << "consume error: " << msg->errstr() << "\n";
        }

        // Flush on size, or once the oldest buffered row has waited long enough.
        if (!batch.empty() &&
            (batch.size() >= BATCH_SIZE ||
             std::chrono::steady_clock::now() - batch_started >= MAX_BATCH_AGE)) {
            flush();
        }

        batch_fill.Set(batch.size());
        memory.Set(resident_bytes());
    }

    flush();
    consumer->close();
    std::cout << "consumer stopped\n";
    return 0;
}
