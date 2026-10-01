find_package(GTest REQUIRED)

function(flashbus_test name)
  cmake_parse_arguments(FB "SERIAL" "" "LIBS" ${ARGN})
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} PRIVATE flashbus flashbus_alloc_counter GTest::gtest_main
                                        ${FB_LIBS})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${name} COMMAND ${name})
  # The stress and sanitizer runs are slow; give them room without making the
  # default timeout generous enough to hide a hang.
  set_tests_properties(${name} PROPERTIES TIMEOUT 900)
  if(FB_SERIAL)
    # Tests that drive the broker need the machine to themselves. Every loop in
    # FlashBus spins before it parks, so two of these running side by side
    # oversubscribe the CPUs and the subscribers fall behind -- which looks
    # exactly like a dropped-event bug and is not one.
    set_tests_properties(${name} PROPERTIES RUN_SERIAL TRUE)
  endif()
endfunction()

flashbus_test(ring_buffer_test)
flashbus_test(protocol_test)
flashbus_test(memory_pool_test)
flashbus_test(metrics_test)
flashbus_test(order_book_test LIBS flashbus_market_data)
flashbus_test(ordering_test SERIAL)
flashbus_test(backpressure_test SERIAL)
flashbus_test(stress_test SERIAL)
