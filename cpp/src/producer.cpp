#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>
#include "trade.pb.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

using json = nlohmann::json;

const std::string BROKERS = "redpanda:9092";
const std::string TOPIC = "raw_trades";
const std::string BINANCE_URL = "wss://stream.binance.com:9443/ws";
const std::string STREAM = "btcusdt@aggTrade";
const std::string METRICS_BIND = "0.0.0.0:9101";

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

int main() {
    std::signal(SIGINT, stop);
    std::signal(SIGTERM, stop);

    prometheus::Exposer exposer{METRICS_BIND};
    auto registry = std::make_shared<prometheus::Registry>();
    auto &published = counter(*registry, "producer_messages_published_total", "Trades published to Redpanda");
    auto &parse_errors = counter(*registry, "producer_parse_errors_total", "Messages that failed to parse");
    auto &reconnects = counter(*registry, "producer_websocket_reconnects_total", "WebSocket reconnects");
    auto &connected = gauge(*registry, "producer_websocket_connected", "1 while the WebSocket is up");
    auto &memory = gauge(*registry, "producer_resident_memory_bytes", "Resident set size");
    exposer.RegisterCollectable(registry);

    std::string err;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", BROKERS, err);
    conf->set("enable.idempotence", "true", err);
    std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(conf.get(), err));
    if (!producer) {
        std::cerr << "could not create producer: " << err << "\n";
        return 1;
    }

    json subscribe = {
        {"method", "SUBSCRIBE"},
        {"params", {STREAM}},
        {"id", 1}
    };

    ix::initNetSystem();
    ix::WebSocket websocket;
    websocket.setUrl(BINANCE_URL);

    bool connected_once = false;
    websocket.setOnMessageCallback([&](const ix::WebSocketMessagePtr &msg) {
        if (msg->type == ix::WebSocketMessageType::Open) {
            std::cout << "connected to " << BINANCE_URL << "\n";
            connected.Set(1);
            if (connected_once) reconnects.Increment();
            connected_once = true;
            websocket.send(subscribe.dump());
            return;
        }
        if (msg->type == ix::WebSocketMessageType::Close) {
            std::cout << "websocket closed\n";
            connected.Set(0);
            return;
        }
        if (msg->type == ix::WebSocketMessageType::Error) {
            std::cerr << "websocket error: " << msg->errorInfo.reason << "\n";
            return;
        }
        if (msg->type != ix::WebSocketMessageType::Message) return;

        try {
            auto j = json::parse(msg->str);
            if (j.value("e", "") != "aggTrade") return;

            market::Trade trade;
            trade.set_event_type(j["e"].get<std::string>());
            trade.set_event_time(j["E"].get<int64_t>());
            trade.set_symbol(j["s"].get<std::string>());
            trade.set_agg_trade_id(j["a"].get<int64_t>());
            trade.set_price(j["p"].get<std::string>());
            trade.set_quantity(j["q"].get<std::string>());
            trade.set_first_trade_id(j["f"].get<int64_t>());
            trade.set_last_trade_id(j["l"].get<int64_t>());
            trade.set_trade_time(j["T"].get<int64_t>());
            trade.set_buyer_is_market_maker(j["m"].get<bool>());

            std::string payload = trade.SerializeAsString();
            producer->produce(TOPIC, RdKafka::Topic::PARTITION_UA,
                              RdKafka::Producer::RK_MSG_COPY,
                              payload.data(), payload.size(),
                              nullptr, 0, 0, nullptr);
            producer->poll(0);
            published.Increment();
        } catch (const std::exception &e) {
            parse_errors.Increment();
            std::cerr << "bad message: " << e.what() << "\n";
        }
    });

    websocket.start();
    std::cout << "producer running, metrics on " << METRICS_BIND << "\n";

    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        memory.Set(resident_bytes());
    }

    websocket.stop();
    producer->flush(10000);
    std::cout << "producer stopped\n";
    return 0;
}
