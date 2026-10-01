find_package(GTest REQUIRED)

function(flashbus_test name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE flashbus flashbus_alloc_counter GTest::gtest_main ${ARGN})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${name} COMMAND ${name})
  # The stress and sanitizer runs are slow; give them room without making the
  # default timeout generous enough to hide a hang.
  set_tests_properties(${name} PROPERTIES TIMEOUT 900)
endfunction()

flashbus_test(ring_buffer_test)
flashbus_test(protocol_test)
flashbus_test(memory_pool_test)
flashbus_test(metrics_test)
flashbus_test(ordering_test)
flashbus_test(backpressure_test)
flashbus_test(order_book_test flashbus_market_data)
flashbus_test(stress_test)
