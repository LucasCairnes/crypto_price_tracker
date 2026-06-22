#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <pqxx/pqxx>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <exception>

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

class TimescaleSink {
    private:
        pqxx::connection conn;

    public:
        TimescaleSink(const std::string &conninfo) : conn(conninfo) {}

        void flush(const std::vector<EnrichedTrade> &batch) {
            if (batch.empty()) return;
            pqxx::work txn(conn);
            std::string sql =
                "INSERT INTO enriched_trades "
                "(symbol, window_start, window_end, vwap, volume, trade_count) VALUES ";
            pqxx::params params;
            for (size_t i = 0; i < batch.size(); ++i) {
                size_t base = i * 6;
                if (i) sql += ",";
                sql += "($" + std::to_string(base + 1) +
                       ",$" + std::to_string(base + 2) +
                       ",$" + std::to_string(base + 3) +
                       ",$" + std::to_string(base + 4) +
                       ",$" + std::to_string(base + 5) +
                       ",$" + std::to_string(base + 6) + ")";
                params.append(batch[i].symbol);
                params.append(batch[i].window_start);
                params.append(batch[i].window_end);
                params.append(batch[i].vwap);
                params.append(batch[i].volume);
                params.append(batch[i].trade_count);
            }
            sql +=
                " ON CONFLICT (symbol, window_start) DO UPDATE SET "
                "window_end = EXCLUDED.window_end, "
                "vwap = EXCLUDED.vwap, "
                "volume = EXCLUDED.volume, "
                "trade_count = EXCLUDED.trade_count";
            txn.exec_params(sql, params);
            txn.commit();
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

        TimescaleSink sink(build_conninfo());

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

        std::cout << "Consumer running. Send SIGINT/SIGTERM (docker stop) to exit.\n";

        std::vector<EnrichedTrade> batch;
        batch.reserve(batch_size);

        while (keep_running) {
            std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR: {
                    try {
                        EnrichedTrade trade;
                        std::string payload(
                            static_cast<const char *>(msg->payload()), msg->len());
                        if (parse_message(payload, trade)) {
                            batch.push_back(std::move(trade));
                        }
                    } catch (const std::exception &e) {
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
