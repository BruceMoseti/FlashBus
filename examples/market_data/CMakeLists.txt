add_library(flashbus_market_data INTERFACE)
target_include_directories(flashbus_market_data INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(flashbus_market_data INTERFACE flashbus)

add_executable(market-data-demo market_data_demo.cpp)
target_link_libraries(market-data-demo PRIVATE flashbus_market_data)
