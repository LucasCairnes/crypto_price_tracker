cmake_minimum_required(VERSION 3.15)
project(live_market_data)

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED True)

find_package(fmt CONFIG REQUIRED)
find_package(ixwebsocket CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(RdKafka CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

add_executable(producer producer.cpp)
target_compile_features(producer PRIVATE cxx_std_20)

# Generate trade.pb.{h,cc} from trade.proto into the build dir and attach to target
protobuf_generate(TARGET producer PROTOS trade.proto LANGUAGE cpp
                  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(producer PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(producer PRIVATE
    fmt::fmt
    ixwebsocket::ixwebsocket
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    protobuf::libprotobuf
)

add_executable(consumer consumer.cpp)
target_compile_features(consumer PRIVATE cxx_std_20)

target_link_libraries(consumer PRIVATE
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    libpqxx::pqxx
)
