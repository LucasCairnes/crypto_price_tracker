cmake_minimum_required(VERSION 3.15)
project(live_market_data)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(ixwebsocket CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(RdKafka CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)

set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../protobuf)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/trade.pb.cc ${CMAKE_CURRENT_BINARY_DIR}/trade.pb.h
    COMMAND protobuf::protoc
    ARGS --cpp_out ${CMAKE_CURRENT_BINARY_DIR} -I ${PROTO_DIR} ${PROTO_DIR}/trade.proto
    DEPENDS ${PROTO_DIR}/trade.proto protobuf::protoc
    VERBATIM
)

add_executable(producer src/producer.cpp ${CMAKE_CURRENT_BINARY_DIR}/trade.pb.cc)
target_include_directories(producer PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(producer PRIVATE
    ixwebsocket::ixwebsocket
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    protobuf::libprotobuf
    prometheus-cpp::pull
    prometheus-cpp::core
)

add_executable(consumer src/consumer.cpp)
target_link_libraries(consumer PRIVATE
    nlohmann_json::nlohmann_json
    RdKafka::rdkafka++
    libpqxx::pqxx
    prometheus-cpp::pull
    prometheus-cpp::core
)
