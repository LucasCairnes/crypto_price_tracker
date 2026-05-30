cmake_minimum_required(VERSION 3.15)
project(live_market_data)

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(my_app main.cpp)
target_compile_features(my_app PRIVATE cxx_std_20)

find_package(fmt CONFIG REQUIRED)
find_package(ixwebsocket CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

target_link_libraries(my_app PRIVATE 
    fmt::fmt
    ixwebsocket::ixwebsocket
    nlohmann_json::nlohmann_json
    libpqxx::pqxx
)