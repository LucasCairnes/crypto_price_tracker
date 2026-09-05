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
find_package(prometheus-cpp CONFIG REQUIRED)

# The schema is shared with the Flink job, so it lives outside this tree.
# Generate into the binary dir so producer.cpp keeps including "trade.pb.h".
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../protobuf)
set(TRADE_PB_SRC ${CMAKE_CURRENT_BINARY_DIR}/trade.pb.cc)
set(TRADE_PB_HDR ${CMAKE_CURRENT_BINARY_DIR}/trade.pb.h)

add_custom_command(
    OUTPUT ${TRADE_PB_SRC} ${TRADE_PB_HDR}
    COMMAND protobuf::protoc
    ARGS --cpp_out ${CMAKE_CURRENT_BINARY_DIR} -I ${PROTO_DIR} ${PROTO_DIR}/trade.proto
    DEPENDS ${PROTO_DIR}/trade.proto protobuf::protoc
    COMMENT "Generating C++ sources from protobuf/trade.proto"
    VERBATIM
)

add_executable(producer src/producer.cpp ${TRADE_PB_SRC})
target_compile_features(producer PRIVATE cxx_std_20)
target_include_directories(producer PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(producer PRIVATE
    fmt::fmt
    ixwebsocket::ixwebsocket
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    protobuf::libprotobuf
    prometheus-cpp::pull
    prometheus-cpp::core
)

add_executable(consumer src/consumer.cpp)
target_compile_features(consumer PRIVATE cxx_std_20)

target_link_libraries(consumer PRIVATE
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    libpqxx::pqxx
    prometheus-cpp::pull
    prometheus-cpp::core
)
