#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
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

using json = nlohmann::json;

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

        std::string binance_url = "wss://stream.binance.com:9443/ws";
        WebsocketClient client(binance_url);

        const char* env_brokers = std::getenv("BROKERS");
        std::string brokers = env_brokers ? env_brokers : "localhost:19092";
        std::string topic = "raw_trades";
        RedpandaProducer producer(brokers, topic);

        client.send_json = [&producer](const std::string& msg) {
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
            } catch (const std::exception& e) {
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

        std::cout << "Producer running. Send SIGINT/SIGTERM (docker stop) to exit.\n";
        while (keep_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        producer.stop(10000);
        client.stop();

    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}