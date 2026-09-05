#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>
#include "trade.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <fstream>
#include <optional>
#include <exception>
#include <algorithm>
#include <functional>
#include <csignal>
#include <cstdlib>
#include <unistd.h>

using json = nlohmann::json;

// Resident set size of this process in bytes, read from /proc/self/statm.
static double read_rss_bytes() {
    std::ifstream statm("/proc/self/statm");
    long pages_total = 0, pages_resident = 0;
    if (statm >> pages_total >> pages_resident) {
        return static_cast<double>(pages_resident) * sysconf(_SC_PAGESIZE);
    }
    return 0.0;
}

std::atomic<bool> keep_running{true};

void handle_signal(int) {
    keep_running = false;
}

class RedpandaProducer {
    private:
        std::unique_ptr<RdKafka::Producer> producer;
        std::string topic;

    public:
        RedpandaProducer(const std::string &brokers, const std::string &topic_name) {
            std::string errstr;
            std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
            conf->set("bootstrap.servers", brokers, errstr);
            std::unique_ptr<RdKafka::Producer> producer_object(RdKafka::Producer::create(conf.get(), errstr));
            producer = std::move(producer_object);
            topic = topic_name;
        };

        void send_data(const std::string &data);
        void stop(const int &timeout_ms);
};

void RedpandaProducer::send_data(const std::string &data) {
    producer->produce(
        topic, 
        RdKafka::Topic::PARTITION_UA, 
        RdKafka::Producer::RK_MSG_COPY, 
        const_cast<char*>(data.c_str()), data.size(),
        nullptr, 0,                     
        0,                      
        nullptr                         
    );
    producer->poll(0);
}

void RedpandaProducer::stop(const int &timeout_ms) {
    producer->flush(timeout_ms);
}

class WebsocketClient {
    private:
        std::string current_url;
        std::string sub_string;
        ix::WebSocket webSocket;
    
        void set_callbacks();

    public:
        std::function<void(const std::string&)> send_json;
        std::function<void()> on_open;
        std::function<void()> on_close;

        WebsocketClient(const std::string &url) {
            ix::initNetSystem();
            webSocket.setUrl(url);
            current_url = url;
        };

        void start(const std::string &subscribe_string);
        void stop();
};

void WebsocketClient::set_callbacks() {
    webSocket.setOnMessageCallback([this](const ix::WebSocketMessagePtr& msg) {
        if (msg->type == ix::WebSocketMessageType::Open) {
            std::cout << "[Client] Connection established to " << current_url << ".\n";
            if (on_open) on_open();
            webSocket.send(sub_string);
        }
        else if (msg->type == ix::WebSocketMessageType::Message) {
            try {
                std::cout << "[Client] Raw JSON received:\n" << msg->str << "\n";
                send_json(msg->str);

            } catch (const std::exception& e) {
                std::cerr << "Exception: " << e.what() << "\n";
            }
        }
        else if (msg->type == ix::WebSocketMessageType::Error) {
            std::cout << "[Client] Error: " << msg->errorInfo.reason << "\n";
        }
        else if (msg->type == ix::WebSocketMessageType::Close) {
            std::cout << "[Client] Connection closed.\n";
            if (on_close) on_close();
        }
    });
}

void WebsocketClient::start(const std::string &subscribe_string) {
    sub_string = subscribe_string;
    this->set_callbacks();
    webSocket.start();
}

void WebsocketClient::stop() {
    webSocket.stop();
}

int main() {
    try {
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);

        const char* env_metrics_port = std::getenv("METRICS_PORT");
        std::string metrics_bind =
            "0.0.0.0:" + std::string(env_metrics_port ? env_metrics_port : "9101");
        prometheus::Exposer exposer{metrics_bind};
        auto registry = std::make_shared<prometheus::Registry>();

        auto &published = prometheus::BuildCounter()
            .Name("producer_messages_published_total")
            .Help("Trade messages published to Redpanda")
            .Register(*registry).Add({});
        auto &parse_errors = prometheus::BuildCounter()
            .Name("producer_parse_errors_total")
            .Help("WebSocket messages that failed to parse")
            .Register(*registry).Add({});
        auto &reconnects = prometheus::BuildCounter()
            .Name("producer_websocket_reconnects_total")
            .Help("WebSocket reconnects since startup")
            .Register(*registry).Add({});
        auto &connected = prometheus::BuildGauge()
            .Name("producer_websocket_connected")
            .Help("1 when the WebSocket is connected, 0 otherwise")
            .Register(*registry).Add({});
        auto &resident_memory = prometheus::BuildGauge()
            .Name("producer_resident_memory_bytes")
            .Help("Resident set size of the producer process")
            .Register(*registry).Add({});
        exposer.RegisterCollectable(registry);

        std::string binance_url = "wss://stream.binance.com:9443/ws";
        WebsocketClient client(binance_url);

        const char* env_brokers = std::getenv("BROKERS");
        std::string brokers = env_brokers ? env_brokers : "localhost:19092";
        std::string topic = "raw_trades";
        RedpandaProducer producer(brokers, topic);

        bool seen_open = false;
        client.on_open = [&]() {
            connected.Set(1);
            if (seen_open) reconnects.Increment();
            seen_open = true;
        };
        client.on_close = [&]() { connected.Set(0); };

        client.send_json = [&](const std::string& msg) {
            try {
                auto j = json::parse(msg);
                if (!j.contains("e") || j["e"] != "aggTrade") return;
                market::Trade t;
                t.set_event_type(j["e"].get<std::string>());
                t.set_event_time(j["E"].get<int64_t>());
                t.set_symbol(j["s"].get<std::string>());
                t.set_agg_trade_id(j["a"].get<int64_t>());
                t.set_price(std::stod(j["p"].get<std::string>()));
                t.set_quantity(std::stod(j["q"].get<std::string>()));
                t.set_first_trade_id(j["f"].get<int64_t>());
                t.set_last_trade_id(j["l"].get<int64_t>());
                t.set_trade_time(j["T"].get<int64_t>());
                t.set_buyer_is_market_maker(j["m"].get<bool>());
                std::string payload;
                t.SerializeToString(&payload);
                producer.send_data(payload);
                published.Increment();
            } catch (const std::exception& e) {
                parse_errors.Increment();
                std::cerr << "Parse error: " << e.what() << "\n";
            }
        };

        json btc_subscribe = {
            {"method", "SUBSCRIBE"},
            {"params", {"btcusdt@aggTrade"}},
            {"id", 1}
        };

        std::string subscribe_string = btc_subscribe.dump();
        client.start(subscribe_string);

        std::cout << "Producer running. Metrics on " << metrics_bind
                  << ". Send SIGINT/SIGTERM (docker stop) to exit.\n";
        while (keep_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            resident_memory.Set(read_rss_bytes());
        }

        producer.stop(10000);
        client.stop();

    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}