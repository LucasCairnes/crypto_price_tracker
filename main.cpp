#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <string>
#include <chrono>
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <fstream>
#include <optional>
#include <pqxx/pqxx>
#include <exception>
#include <algorithm>

using json = nlohmann::json;

std::atomic<bool> keep_running{true};
int time_interval = 1000;

struct Trade {
    std::string coin;
    long double price;
    long double volume;
    int64_t timestamp;
};

struct OHLCV {
    int64_t timestamp;
    long double open;
    long double high;
    long double low;
    long double close;
    long double volume;
};

std::optional<Trade> format_trade(json &received_json) {
    if (!received_json.contains("e") || received_json["e"] != "aggTrade") {
        return std::nullopt;
    }

    std::string coin = received_json["s"];
    long double price = std::stold(received_json["p"].get<std::string>());
    long double volume = std::stold(received_json["q"].get<std::string>());
    int64_t timestamp = received_json["E"].get<int64_t>();                

    Trade received_trade = {.coin = coin, .price = price, .volume = volume, .timestamp = timestamp};
    
    return received_trade;
}

OHLCV calculate_ohlcv(OHLCV &current_ohlcv, const Trade &current_trade) {
    current_ohlcv.high = std::max(current_ohlcv.high, current_trade.price);
    current_ohlcv.low = std::min(current_ohlcv.low, current_trade.price);
    current_ohlcv.volume += current_trade.volume;
    current_ohlcv.close = current_trade.price;

    return current_ohlcv;
}

void to_timescale(OHLCV &current_ohlcv, pqxx::connection &C) {
    try {
        pqxx::work W(C);
        W.exec_params(
            "INSERT INTO ohlcv_data (time, open, high, low, close, volume) VALUES (TO_TIMESTAMP($1::numeric), $2, $3, $4, $5, $6)",
            current_ohlcv.timestamp / 1000.0, current_ohlcv.open, current_ohlcv.high, current_ohlcv.low, current_ohlcv.close, current_ohlcv.volume);
            W.commit();
        }
    catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return;
    }
    return;
}

int main() {
    std::string trades_filename = "saved_trades.csv";
    std::string ohlcvs_filename = "saved_ohlcvs.csv";

    std::vector<Trade> trades;
    std::vector<OHLCV> ohlcvs;
    std::mutex data_mutex;
    
    try {
        std::string connection_string = "dbname=postgres user=postgres password=password host=127.0.0.1 port=5432";
        pqxx::connection C(connection_string);

        if (C.is_open()) {
            std::cout << "Successfully connected to database: " << C.dbname() << std::endl;
        } else {
            std::cerr << "Failed to connect to database." << std::endl;
            return 1;
        }

        ix::initNetSystem();
        ix::WebSocket webSocket;

        std::string binance_url = "wss://stream.binance.com:9443/ws";
        webSocket.setUrl(binance_url);

        json btc_subscribe;
        btc_subscribe["method"] = "SUBSCRIBE";
        btc_subscribe["params"] = json::array({"btcusdt@aggTrade"});
        btc_subscribe["id"] = 1;
        std::string subscribe_string = btc_subscribe.dump();

        OHLCV current_ohlcv;

        webSocket.setOnMessageCallback([&binance_url, &subscribe_string, &webSocket, &trades, &ohlcvs, &data_mutex, &current_ohlcv, &C](const ix::WebSocketMessagePtr& msg) {
            if (msg->type == ix::WebSocketMessageType::Open) {
                std::cout << "[Client] Connection established to " << binance_url << ".\n";
                webSocket.send(subscribe_string);
            }
            else if (msg->type == ix::WebSocketMessageType::Message) {
                try {
                    json received_json = json::parse(msg->str);
                    std::optional<Trade> formatted_trade = format_trade(received_json);

                    if (formatted_trade.has_value()) {
                        std::lock_guard<std::mutex> lock(data_mutex);
                        
                        trades.emplace_back(formatted_trade.value());

                        if (ohlcvs.empty()) {
                            current_ohlcv = {.timestamp = formatted_trade->timestamp, .open = formatted_trade->price, .high = formatted_trade->price,
                                             .low = formatted_trade->price, .close = formatted_trade->price, .volume = formatted_trade->volume};
                            ohlcvs.emplace_back(current_ohlcv);
                        }
                        else if ((formatted_trade->timestamp - current_ohlcv.timestamp) >= time_interval) {
                            to_timescale(ohlcvs.back(), C);
                            
                            int64_t next_time = current_ohlcv.timestamp + time_interval;
                            
                            current_ohlcv = {.timestamp = next_time, .open = formatted_trade->price, .high = formatted_trade->price,
                                             .low = formatted_trade->price, .close = formatted_trade->price, .volume = formatted_trade->volume};
                            
                            ohlcvs.emplace_back(current_ohlcv);
                        }
                        else {
                            current_ohlcv = calculate_ohlcv(current_ohlcv, formatted_trade.value());
                            ohlcvs.back() = current_ohlcv;
                        }
                    }
                } catch (const json::parse_error& e) {
                    std::cerr << "JSON Parse Error: " << e.what() << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Exception: " << e.what() << std::endl;
                }
            }
            else if (msg->type == ix::WebSocketMessageType::Error) {
                std::cout << "[Client] Error: " << msg->errorInfo.reason << "\n";
            }
            else if (msg->type == ix::WebSocketMessageType::Close) {
                std::cout << "[Client] Connection closed.\n";
            }
        });

        webSocket.start();

        std::string input;
        std::cout << "Type 'quit' to exit: \n";
        while (std::getline(std::cin, input)) {
            if (input == "quit") break;
        }   

        webSocket.stop();
        ix::uninitNetSystem();
        std::cout << "[Client] Connection closed to " << binance_url <<"\n";

    } catch (const std::exception &e) {
        std::cerr << "Fatal Error: " << e.what() << std::endl;
        return 1;
    }
    

    std::lock_guard<std::mutex> lock(data_mutex);

    std::cout << "Writing data to " << trades_filename <<"...\n";    
    std::ofstream trades_out(trades_filename);
    trades_out << "coin,timestamp,price,volume\n";
    for (const auto &trade : trades) {
        trades_out << trade.coin << "," << trade.timestamp << "," << trade.price << "," << trade.volume << "\n";
    }
    trades_out.close();
    std::cout << "Writing complete.\n";    

    std::cout << "Writing data to " << ohlcvs_filename <<"...\n";    
    std::ofstream ohlcvs_out(ohlcvs_filename);
    ohlcvs_out << "timestamp,open,high,low,close,volume\n";
    for (const auto &ohlcv : ohlcvs) {
        ohlcvs_out << ohlcv.timestamp << "," << ohlcv.open << "," << ohlcv.high << "," 
                   << ohlcv.low << "," << ohlcv.close << "," << ohlcv.volume << "\n";
    }
    ohlcvs_out.close();
    std::cout << "Writing complete.\n";   

    return 0;
}